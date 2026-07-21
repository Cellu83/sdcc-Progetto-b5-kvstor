package raft

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Cellu83/sdcc-Progetto-b5-kvstor/internal/raftlog"
)

// State identifica il ruolo corrente di un nodo nel protocollo Raft.
type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// RequestVoteArgs / RequestVoteResult ed AppendEntriesArgs / AppendEntriesResult
// sono le versioni "in puro Go" (senza dipendere da gRPC/protobuf) degli
// argomenti e delle risposte delle due RPC di Raft. Tenerle separate dai
// tipi generati da raft.proto permette di testare la logica di consenso
// senza dover avviare un server/client gRPC reale.
type RequestVoteArgs struct {
	Term         uint64
	CandidateID  string
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteResult struct {
	Term        uint64
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []raftlog.Entry
	LeaderCommit uint64
}

type AppendEntriesResult struct {
	Term    uint64
	Success bool
}

// Config raccoglie i parametri con cui costruire un Node, tutti provenienti
// dalla configurazione del nodo (nessun valore hard-coded).
type Config struct {
	ID                 string
	Peers              map[string]string // nodeID -> indirizzo gRPC, include se stesso
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration
	RPCTimeout         time.Duration
}

// Node è il cuore dell'algoritmo di consenso: la macchina a stati
// Follower/Candidate/Leader, il timer di elezione, e le regole di
// sicurezza di Raft per concedere voti e riconoscere un leader legittimo.
//
// Non contiene nessuna dipendenza diretta da gRPC: parla con gli altri nodi
// tramite l'interfaccia astratta *Client, e viene a sua volta esposto in
// rete da un Server (in server.go) che è un semplice adattatore gRPC verso
// i metodi HandleRequestVote/HandleAppendEntries.
type Node struct {
	mu sync.Mutex

	id      string
	peers   map[string]string
	storage *raftlog.Storage
	client  *Client

	electionTimeoutMin time.Duration
	electionTimeoutMax time.Duration
	heartbeatInterval  time.Duration
	rpcTimeout         time.Duration

	state         State
	currentLeader string // "" se non conosciamo un leader in questo momento

	resetElectionCh chan struct{}
	stopCh          chan struct{}
}

// NewNode costruisce un Node pronto per essere avviato con Run(). Parte
// sempre come Follower, come previsto da Raft all'avvio di un nodo.
func NewNode(cfg Config, storage *raftlog.Storage, client *Client) *Node {
	return &Node{
		id:                 cfg.ID,
		peers:              cfg.Peers,
		storage:            storage,
		client:             client,
		electionTimeoutMin: cfg.ElectionTimeoutMin,
		electionTimeoutMax: cfg.ElectionTimeoutMax,
		heartbeatInterval:  cfg.HeartbeatInterval,
		rpcTimeout:         cfg.RPCTimeout,
		state:              Follower,
		resetElectionCh:    make(chan struct{}, 1),
		stopCh:             make(chan struct{}),
	}
}

// Run avvia in background il ciclo di vita del nodo (il timer di elezione).
// Non blocca il chiamante.
func (n *Node) Run() {
	go n.electionLoop()
}

// Stop ferma tutti i cicli in background del nodo.
func (n *Node) Stop() {
	close(n.stopCh)
}

// State restituisce il ruolo corrente del nodo, in modo sicuro rispetto
// alla concorrenza.
func (n *Node) State() State {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state
}

// CurrentLeader restituisce l'ID del nodo che riteniamo essere l'attuale
// leader, oppure "" se non lo sappiamo.
func (n *Node) CurrentLeader() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentLeader
}

// resetElectionTimerLocked segnala al ciclo di elezione di ricominciare ad
// attendere un nuovo timeout casuale, perché il nodo ha appena "sentito"
// un altro nodo attivo e legittimo (un leader valido, o ha concesso un
// voto). Il chiamante deve già tenere n.mu. L'invio è non bloccante: se il
// canale è già pieno (un reset era già in coda), non importa, ne basta uno.
func (n *Node) resetElectionTimerLocked() {
	select {
	case n.resetElectionCh <- struct{}{}:
	default:
	}
}

// becomeFollowerLocked riporta il nodo a Follower. Se term è più recente di
// quello persistito, aggiorna anche il term persistito e azzera il voto
// (nuovo term, nessun voto ancora dato). Se term è uguale a quello
// corrente, il nodo scende semplicemente da Candidate/Leader a Follower
// senza toccare il voto già espresso in questo stesso term. Il chiamante
// deve già tenere n.mu.
func (n *Node) becomeFollowerLocked(term uint64) {
	if term > n.storage.CurrentTerm() {
		_ = n.storage.SetTermAndVote(term, "")
	}
	n.state = Follower
	n.currentLeader = ""
}

// becomeLeaderLocked promuove il nodo a Leader e avvia il ciclo di
// heartbeat. Il chiamante deve già tenere n.mu. È protetta da una
// promozione doppia (es. per una race tra più risposte di voto che
// arrivano quasi insieme): se il nodo è già Leader, non fa nulla.
func (n *Node) becomeLeaderLocked() {
	if n.state == Leader {
		return
	}
	n.state = Leader
	n.currentLeader = n.id
	log.Printf("[%s] eletto Leader per il term %d", n.id, n.storage.CurrentTerm())
	go n.runLeaderHeartbeats()
}

// HandleRequestVote implementa le regole di sicurezza di Raft per decidere
// se concedere il voto a un candidato. È chiamata dal Server gRPC quando
// arriva una RequestVote da un altro nodo.
func (n *Node) HandleRequestVote(args RequestVoteArgs) RequestVoteResult {
	n.mu.Lock()
	defer n.mu.Unlock()

	currentTerm := n.storage.CurrentTerm()

	// Regola 1: un candidato con un term più vecchio del nostro non merita
	// nemmeno di essere considerato.
	if args.Term < currentTerm {
		return RequestVoteResult{Term: currentTerm, VoteGranted: false}
	}

	// Se il candidato ha un term più recente, ci aggiorniamo subito e
	// torniamo Follower, dimenticando l'eventuale voto del term precedente.
	if args.Term > currentTerm {
		n.becomeFollowerLocked(args.Term)
		currentTerm = args.Term
	}

	// Regola 2: possiamo votare solo se non abbiamo già votato per un
	// candidato diverso in questo term. Se abbiamo già votato per lo
	// stesso candidato (es. messaggio duplicato), va bene rivotarlo.
	votedFor := n.storage.VotedFor()
	if votedFor != "" && votedFor != args.CandidateID {
		return RequestVoteResult{Term: currentTerm, VoteGranted: false}
	}

	// Regola 3 (l'"election restriction" di Raft): il log del candidato
	// deve essere almeno aggiornato quanto il nostro, altrimenti eleggere
	// quel candidato rischierebbe di far perdere entry già confermate.
	myLastTerm := n.storage.LastLogTerm()
	myLastIndex := n.storage.LastLogIndex()
	candidateLogIsUpToDate := args.LastLogTerm > myLastTerm ||
		(args.LastLogTerm == myLastTerm && args.LastLogIndex >= myLastIndex)

	if !candidateLogIsUpToDate {
		return RequestVoteResult{Term: currentTerm, VoteGranted: false}
	}

	// Tutte le condizioni sono soddisfatte: concediamo il voto e lo
	// persistiamo subito (Fase 2), poi rimandiamo il nostro timeout di
	// elezione, perché sappiamo che nel cluster c'è un candidato attivo.
	if err := n.storage.SetTermAndVote(currentTerm, args.CandidateID); err != nil {
		log.Printf("[%s] errore persistendo il voto: %v", n.id, err)
		return RequestVoteResult{Term: currentTerm, VoteGranted: false}
	}
	n.resetElectionTimerLocked()

	log.Printf("[%s] voto concesso a %s per il term %d", n.id, args.CandidateID, currentTerm)
	return RequestVoteResult{Term: currentTerm, VoteGranted: true}
}

// HandleAppendEntries riconosce l'autorità di un leader legittimo. In
// questa fase non applica ancora nessuna entry né controlla la coerenza
// del log tramite prevLogIndex/prevLogTerm: quella logica (la vera replica)
// arriva nella Fase 5. Qui ci limitiamo a decidere se il mittente è un
// leader che dobbiamo rispettare, e nel caso a tornare Follower.
func (n *Node) HandleAppendEntries(args AppendEntriesArgs) AppendEntriesResult {
	n.mu.Lock()
	defer n.mu.Unlock()

	currentTerm := n.storage.CurrentTerm()

	// Regola 1: un mittente con un term più vecchio del nostro non è un
	// leader legittimo per noi: lo rifiutiamo.
	if args.Term < currentTerm {
		return AppendEntriesResult{Term: currentTerm, Success: false}
	}

	// Un AppendEntries con term >= al nostro implica un leader legittimo
	// per (almeno) questo term: ci allineiamo e torniamo Follower, anche
	// se eravamo Candidate (o, in teoria, Leader di un term più vecchio).
	if n.currentLeader != args.LeaderID {
		log.Printf("[%s] riconosciuto leader %s per il term %d", n.id, args.LeaderID, args.Term)
	}
	n.becomeFollowerLocked(args.Term)
	n.currentLeader = args.LeaderID
	n.resetElectionTimerLocked()

	return AppendEntriesResult{Term: n.storage.CurrentTerm(), Success: true}
}

// electionLoop è il ciclo in background che gestisce il timeout di
// elezione: se passa un tempo casuale (tra electionTimeoutMin e
// electionTimeoutMax) senza che arrivi un reset (voto concesso o leader
// valido riconosciuto), il nodo avvia una nuova elezione.
func (n *Node) electionLoop() {
	for {
		timeout := randomDuration(n.electionTimeoutMin, n.electionTimeoutMax)
		select {
		case <-time.After(timeout):
			if n.State() == Leader {
				// Il leader non deve mai avviare un'elezione su se stesso:
				// la sua "presenza" è garantita dagli heartbeat, non da
				// questo timer.
				continue
			}
			n.startElection()
		case <-n.resetElectionCh:
			// Qualcosa ha rimandato la scadenza: ricominciamo il ciclo con
			// un nuovo timeout casuale.
		case <-n.stopCh:
			return
		}
	}
}

// startElection trasforma il nodo in Candidate, incrementa e persiste il
// term, vota per se stesso, e chiede il voto a tutti i peer in parallelo.
func (n *Node) startElection() {
	n.mu.Lock()
	n.state = Candidate
	newTerm := n.storage.CurrentTerm() + 1
	if err := n.storage.SetTermAndVote(newTerm, n.id); err != nil {
		log.Printf("[%s] errore persistendo il nuovo term: %v", n.id, err)
		n.mu.Unlock()
		return
	}
	lastIndex := n.storage.LastLogIndex()
	lastTerm := n.storage.LastLogTerm()
	peers := make(map[string]string, len(n.peers))
	for id, addr := range n.peers {
		peers[id] = addr
	}
	n.mu.Unlock()

	log.Printf("[%s] avvio elezione per il term %d", n.id, newTerm)

	votes := int32(1) // il nostro auto-voto
	majority := int32(len(n.peers)/2 + 1)

	for id, addr := range peers {
		if id == n.id {
			continue
		}
		go func(id, addr string) {
			ctx, cancel := context.WithTimeout(context.Background(), n.rpcTimeout)
			defer cancel()

			result, err := n.client.RequestVote(ctx, addr, RequestVoteArgs{
				Term:         newTerm,
				CandidateID:  n.id,
				LastLogIndex: lastIndex,
				LastLogTerm:  lastTerm,
			})
			if err != nil {
				// Il peer non ha risposto in tempo o è irraggiungibile:
				// lo ignoriamo, non blocchiamo l'elezione per un nodo giù.
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if result.Term > n.storage.CurrentTerm() {
				n.becomeFollowerLocked(result.Term)
				return
			}
			// Se nel frattempo non siamo più Candidate per QUESTO term
			// (es. abbiamo già vinto, o siamo tornati Follower), la
			// risposta è superata dagli eventi: la ignoriamo.
			if n.state != Candidate || n.storage.CurrentTerm() != newTerm {
				return
			}
			if result.VoteGranted {
				newVotes := atomic.AddInt32(&votes, 1)
				if newVotes >= majority {
					n.becomeLeaderLocked()
				}
			}
		}(id, addr)
	}
}

// runLeaderHeartbeats manda periodicamente un AppendEntries vuoto (un
// heartbeat) a tutti i peer, per mantenere l'autorità del leader e
// impedire che gli altri nodi facciano scadere il proprio timeout di
// elezione. Si ferma da solo appena il nodo non è più Leader.
func (n *Node) runLeaderHeartbeats() {
	ticker := time.NewTicker(n.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			n.mu.Lock()
			if n.state != Leader {
				n.mu.Unlock()
				return
			}
			term := n.storage.CurrentTerm()
			peers := make(map[string]string, len(n.peers))
			for id, addr := range n.peers {
				peers[id] = addr
			}
			n.mu.Unlock()

			n.broadcastHeartbeat(term, peers)
		case <-n.stopCh:
			return
		}
	}
}

// broadcastHeartbeat manda un AppendEntries vuoto a ogni peer in parallelo.
// Se un peer risponde con un term più alto del nostro, vuol dire che nel
// cluster è già in corso (o è già avvenuta) un'elezione più recente: ci
// dimettiamo subito da Leader.
func (n *Node) broadcastHeartbeat(term uint64, peers map[string]string) {
	for id, addr := range peers {
		if id == n.id {
			continue
		}
		go func(id, addr string) {
			ctx, cancel := context.WithTimeout(context.Background(), n.rpcTimeout)
			defer cancel()

			result, err := n.client.AppendEntries(ctx, addr, AppendEntriesArgs{
				Term:     term,
				LeaderID: n.id,
			})
			if err != nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()
			if result.Term > n.storage.CurrentTerm() {
				log.Printf("[%s] mi dimetto da Leader: il peer %s è già al term %d", n.id, id, result.Term)
				n.becomeFollowerLocked(result.Term)
			}
		}(id, addr)
	}
}

// randomDuration restituisce una durata casuale nell'intervallo [min, max).
// La casualità è essenziale in Raft: se tutti i nodi avessero lo stesso
// timeout di elezione, scadrebbero tutti nello stesso istante e si
// candiderebbero tutti insieme, causando elezioni con voti sempre divisi.
func randomDuration(min, max time.Duration) time.Duration {
	if max <= min {
		return min
	}
	return min + time.Duration(rand.Int63n(int64(max-min)))
}
