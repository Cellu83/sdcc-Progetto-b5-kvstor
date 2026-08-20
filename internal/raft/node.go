package raft

import (
	"context"
	"errors"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Cellu83/sdcc-Progetto-b5-kvstor/internal/kvstore"
	"github.com/Cellu83/sdcc-Progetto-b5-kvstor/internal/raftlog"
)

// ErrNotLeader viene ritornato da Propose (e quindi dalle RPC Put/Delete)
// quando il nodo contattato non è (o non è più) il Leader del cluster.
var ErrNotLeader = errors.New("questo nodo non è il Leader")

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

// *****************************************************************************
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

// commitPollInterval è la frequenza con cui Propose ricontrolla se la sua
// entry è stata committata. Un polling breve invece di una notifica basata
// su canali/condition variable è la scelta più semplice per rispettare
// anche la cancellazione/scadenza del context della richiesta gRPC in
// arrivo, che sync.Cond non sa gestire nativamente.
const commitPollInterval = 5 * time.Millisecond

//*****************************************************************************

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
	store   *kvstore.Store
	client  *Client

	electionTimeoutMin time.Duration
	electionTimeoutMax time.Duration
	heartbeatInterval  time.Duration
	rpcTimeout         time.Duration

	state         State
	currentLeader string // "" se non conosciamo un leader in questo momento

	// Stato volatile di replica (Raft paper, Figure 2): commitIndex e
	// lastApplied esistono su tutti i nodi; nextIndex e matchIndex hanno
	// senso solo quando il nodo è Leader (reinizializzati a ogni elezione).
	commitIndex uint64
	lastApplied uint64
	nextIndex   map[string]uint64
	matchIndex  map[string]uint64

	resetElectionCh chan struct{}
	stopCh          chan struct{}
	stopOnce        sync.Once
}

// NewNode costruisce un Node pronto per essere avviato con Run(). Parte
// sempre come Follower, come previsto da Raft all'avvio di un nodo.
func NewNode(cfg Config, storage *raftlog.Storage, store *kvstore.Store, client *Client) *Node {
	return &Node{
		id:                 cfg.ID,
		peers:              cfg.Peers,
		storage:            storage,
		store:              store,
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

// Stop ferma tutti i cicli in background del nodo. È sicuro chiamarlo più
// volte (es. un test che uccide un nodo esplicitamente e poi lo rifà nel
// proprio cleanup): solo la prima chiamata ha effetto.
func (n *Node) Stop() {
	n.stopOnce.Do(func() {
		close(n.stopCh)
	})
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

// ID restituisce l'identificativo di questo nodo. È immutabile per tutta
// la vita del Node, quindi non serve alcun lock per leggerlo.
func (n *Node) ID() string {
	return n.id
}

// ClusterStatus restituisce la vista corrente del nodo sul cluster: i peer
// conosciuti e chi ritiene essere l'attuale Leader ("" se non lo sa
// ancora). È il dato alla base del pattern Service Discovery: un client
// esterno (il Client proxy service, Fase 6) lo usa per scoprire
// dinamicamente dove indirizzare le richieste.
func (n *Node) ClusterStatus() (leaderID string, peers map[string]string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	peersCopy := make(map[string]string, len(n.peers))
	for id, addr := range n.peers {
		peersCopy[id] = addr
	}
	return n.currentLeader, peersCopy
}

// CommitIndex restituisce l'indice dell'ultima entry committata (nota
// anche solo localmente: su un follower può essere temporaneamente
// indietro rispetto al leader).
func (n *Node) CommitIndex() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.commitIndex
}

// SnapshotState restituisce una copia dello stato applicativo corrente
// (tutte le coppie chiave-valore) insieme all'indice e al term dell'ultima
// entry di log applicata (lastApplied) — i metadati che indicano fino a
// dove quello stato è aggiornato. È il dato che lo Snapshot & backup
// service scarica per costruire un checkpoint esterno: chiamarla non
// modifica in alcun modo il log persistito del nodo.
func (n *Node) SnapshotState() (data map[string]string, lastIncludedIndex uint64, lastIncludedTerm uint64) {
	n.mu.Lock()
	lastIncludedIndex = n.lastApplied
	lastIncludedTerm = n.termAtIndexLocked(n.lastApplied)
	n.mu.Unlock()

	return n.store.Snapshot(), lastIncludedIndex, lastIncludedTerm
}

// Get legge il valore locale della state machine chiave-valore. È una
// lettura "grezza": non verifica se il nodo è Leader. Quel controllo (che
// serve per garantire la consistenza forte richiesta dalla spec) è
// responsabilità del chiamante esterno (KVServer, l'adattatore gRPC in
// kvserver.go). Tenerlo separato qui permette anche di ispezionare
// facilmente lo stato di un follower nei test, per verificare che la
// replica sia avvenuta davvero.
func (n *Node) Get(key string) (string, bool) {
	return n.store.Get(key)
}

// Propose accoda cmd come nuova entry di log (solo se il nodo è
// correntemente Leader), avvia subito la replica verso i follower — senza
// aspettare il prossimo heartbeat periodico, per non introdurre latenza
// inutile — e attende che l'entry venga committata dalla maggioranza prima
// di ritornare. Ritorna ErrNotLeader se il nodo non è (o smette di essere,
// durante l'attesa) il Leader.
func (n *Node) Propose(ctx context.Context, cmd kvstore.Command) error {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return ErrNotLeader
	}
	term := n.storage.CurrentTerm()
	index := n.storage.LastLogIndex() + 1
	entry := raftlog.Entry{Term: term, Index: index, Command: cmd}
	if err := n.storage.Append(entry); err != nil {
		n.mu.Unlock()
		return err
	}
	n.mu.Unlock()

	n.broadcastAppendEntries()

	return n.waitForCommit(ctx, index)
}

// waitForCommit attende, con un polling breve (commitPollInterval), che
// l'entry a index risulti committata. Ritorna ErrNotLeader se nel
// frattempo il nodo perde la leadership (es. un leader più recente si è
// imposto altrove), o l'errore del context se questo scade/viene
// cancellato prima.
func (n *Node) waitForCommit(ctx context.Context, index uint64) error {
	ticker := time.NewTicker(commitPollInterval)
	defer ticker.Stop()

	for {
		n.mu.Lock()
		committed := n.commitIndex >= index
		stillLeader := n.state == Leader
		n.mu.Unlock()

		if committed {
			return nil
		}
		if !stillLeader {
			return ErrNotLeader
		}

		select {
		case <-ticker.C:
		case <-ctx.Done():
			return ctx.Err()
		case <-n.stopCh:
			return ErrNotLeader
		}
	}
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

// becomeLeaderLocked promuove il nodo a Leader, (re)inizializza lo stato
// volatile di replica nextIndex/matchIndex e avvia il ciclo di
// replica/heartbeat. Il chiamante deve già tenere n.mu. È protetta da una
// promozione doppia (es. per una race tra più risposte di voto che
// arrivano quasi insieme): se il nodo è già Leader, non fa nulla.
func (n *Node) becomeLeaderLocked() {
	if n.state == Leader {
		return
	}
	n.state = Leader
	n.currentLeader = n.id

	// nextIndex parte ottimisticamente da "il follower ha il mio stesso
	// log": se si sbaglia, il meccanismo di conflitto in replicateToPeer
	// lo farà scendere finché non trova un punto di accordo. matchIndex
	// parte da 0: non sappiamo ancora nulla di confermato su nessun peer.
	lastIndex := n.storage.LastLogIndex()
	n.nextIndex = make(map[string]uint64, len(n.peers))
	n.matchIndex = make(map[string]uint64, len(n.peers))
	for id := range n.peers {
		n.nextIndex[id] = lastIndex + 1
		n.matchIndex[id] = 0
	}

	log.Printf("[%s] eletto Leader per il term %d", n.id, n.storage.CurrentTerm())
	go n.runLeaderReplicationLoop()
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

// HandleAppendEntries riconosce l'autorità di un leader legittimo,
// verifica la coerenza del proprio log rispetto a quello del leader
// (prevLogIndex/prevLogTerm), integra le nuove entry (risolvendo eventuali
// conflitti) e avanza il proprio commitIndex applicando le entry
// committate alla state machine.
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
	currentTerm = n.storage.CurrentTerm()

	// Controllo di coerenza del log (Raft paper, Figure 2, punto 2): il
	// nostro log deve avere un'entry a PrevLogIndex con lo stesso
	// PrevLogTerm del leader. Se non ce l'abbiamo, il nostro log è
	// "indietro" o divergente in quel punto: rifiutiamo, e il leader
	// riproverà più indietro (vedi replicateToPeer).
	if args.PrevLogIndex > 0 {
		myTermAtPrev := n.termAtIndexLocked(args.PrevLogIndex)
		if myTermAtPrev == 0 || myTermAtPrev != args.PrevLogTerm {
			return AppendEntriesResult{Term: currentTerm, Success: false}
		}
	}

	if len(args.Entries) > 0 {
		if err := n.integrateEntriesLocked(args.Entries); err != nil {
			log.Printf("[%s] errore integrando le entry ricevute: %v", n.id, err)
			return AppendEntriesResult{Term: currentTerm, Success: false}
		}
	}

	// Avanziamo il nostro commitIndex fino al minimo tra quanto ci
	// consente il leader (leaderCommit) e quanto log abbiamo davvero
	// ricevuto finora (lastNewIndex) — non possiamo committare entry che
	// non abbiamo ancora.
	if args.LeaderCommit > n.commitIndex {
		lastNewIndex := args.PrevLogIndex + uint64(len(args.Entries))
		newCommit := args.LeaderCommit
		if lastNewIndex < newCommit {
			newCommit = lastNewIndex
		}
		if newCommit > n.commitIndex {
			n.commitIndex = newCommit
			n.applyCommittedLocked()
		}
	}

	return AppendEntriesResult{Term: currentTerm, Success: true}
}

// termAtIndexLocked restituisce il Term della entry all'Index dato
// (1-based), o 0 se index è 0 o supera la fine del log. Il chiamante deve
// già tenere n.mu.
func (n *Node) termAtIndexLocked(index uint64) uint64 {
	if index == 0 {
		return 0
	}
	entries := n.storage.Log()
	if index > uint64(len(entries)) {
		return 0
	}
	return entries[index-1].Term
}

// entriesFromLocked restituisce tutte le entry del log a partire da
// startIndex (1-based) fino alla fine. Il chiamante deve già tenere n.mu.
func (n *Node) entriesFromLocked(startIndex uint64) []raftlog.Entry {
	entries := n.storage.Log()
	if startIndex == 0 {
		startIndex = 1
	}
	if startIndex > uint64(len(entries)) {
		return nil
	}
	return entries[startIndex-1:]
}

// integrateEntriesLocked incorpora newEntries nel nostro log. Per ogni
// entry: se al suo Index abbiamo già un'entry con lo stesso Term, è un
// duplicato (es. retry di un AppendEntries) e la saltiamo; se abbiamo
// un'entry diversa allo stesso Index, è un conflitto — Raft impone di
// troncare il nostro log da quel punto in poi e accettare la versione del
// leader; altrimenti la accodiamo semplicemente. Il chiamante deve già
// tenere n.mu.
func (n *Node) integrateEntriesLocked(newEntries []raftlog.Entry) error {
	for _, e := range newEntries {
		existing := n.storage.Log()
		if e.Index <= uint64(len(existing)) {
			if existing[e.Index-1].Term == e.Term {
				continue // già presente, nessun conflitto
			}
			if err := n.storage.Truncate(e.Index); err != nil {
				return err
			}
		}
		if err := n.storage.Append(e); err != nil {
			return err
		}
	}
	return nil
}

// applyCommittedLocked applica alla state machine tutte le entry tra
// lastApplied (escluso) e commitIndex (incluso), in ordine — esattamente
// come previsto da Raft: solo le entry già committate dalla maggioranza
// possono raggiungere lo Store. Il chiamante deve già tenere n.mu.
func (n *Node) applyCommittedLocked() {
	entries := n.storage.Log()
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		entry := entries[n.lastApplied-1]
		if err := n.store.Apply(entry.Command); err != nil {
			log.Printf("[%s] errore applicando l'entry %d alla state machine: %v", n.id, entry.Index, err)
		}
	}
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

// runLeaderReplicationLoop manda periodicamente un AppendEntries a tutti i
// peer — con le entry mancanti se il peer è indietro, vuoto (un puro
// heartbeat) se è già aggiornato. Mantiene l'autorità del leader e impedisce
// agli altri nodi di far scadere il proprio timeout di elezione. Si ferma
// da solo appena il nodo non è più Leader.
func (n *Node) runLeaderReplicationLoop() {
	ticker := time.NewTicker(n.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if n.State() != Leader {
				return
			}
			n.broadcastAppendEntries()
		case <-n.stopCh:
			return
		}
	}
}

// broadcastAppendEntries manda, in parallelo, un AppendEntries a ogni peer
// (chiamata sia dal ciclo di heartbeat periodico sia subito dopo una
// Propose, per non aspettare il prossimo tick prima di replicare una
// scrittura appena accettata).
func (n *Node) broadcastAppendEntries() {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return
	}
	term := n.storage.CurrentTerm()
	leaderCommit := n.commitIndex
	peers := make(map[string]string, len(n.peers))
	for id, addr := range n.peers {
		peers[id] = addr
	}
	n.mu.Unlock()

	for id, addr := range peers {
		if id == n.id {
			continue
		}
		go n.replicateToPeer(id, addr, term, leaderCommit)
	}
}

// replicateToPeer manda a un singolo peer le entry che gli mancano (a
// partire dal nextIndex che gli abbiamo assegnato), secondo il protocollo
// AppendEntries di Raft. Se il peer rifiuta per un conflitto di log,
// decrementiamo il suo nextIndex e riproveremo al giro successivo; se
// accetta, avanziamo matchIndex/nextIndex e ricalcoliamo se possiamo
// avanzare il commitIndex del cluster.
func (n *Node) replicateToPeer(id, addr string, term uint64, leaderCommit uint64) {
	n.mu.Lock()
	if n.state != Leader || n.storage.CurrentTerm() != term {
		n.mu.Unlock()
		return
	}
	nextIdx := n.nextIndex[id]
	if nextIdx == 0 {
		nextIdx = 1
	}
	prevLogIndex := nextIdx - 1
	prevLogTerm := n.termAtIndexLocked(prevLogIndex)
	entries := n.entriesFromLocked(nextIdx)
	n.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), n.rpcTimeout)
	defer cancel()

	result, err := n.client.AppendEntries(ctx, addr, AppendEntriesArgs{
		Term:         term,
		LeaderID:     n.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	})
	if err != nil {
		// Peer irraggiungibile o RPC scaduta: ci riproveremo al prossimo
		// giro di replica, non è un errore fatale.
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if result.Term > n.storage.CurrentTerm() {
		log.Printf("[%s] mi dimetto da Leader: il peer %s è già al term %d", n.id, id, result.Term)
		n.becomeFollowerLocked(result.Term)
		return
	}
	// Se nel frattempo non siamo più Leader per QUESTO term, la risposta è
	// superata dagli eventi.
	if n.state != Leader || n.storage.CurrentTerm() != term {
		return
	}

	if result.Success {
		newMatch := prevLogIndex + uint64(len(entries))
		if newMatch > n.matchIndex[id] {
			n.matchIndex[id] = newMatch
		}
		n.nextIndex[id] = newMatch + 1
		n.maybeAdvanceCommitIndexLocked()
	} else {
		// Conflitto di log: il peer non aveva un'entry coerente a
		// prevLogIndex. Arretriamo di una posizione e riproveremo: prima o
		// poi troveremo il punto in cui i due log tornano ad accordarsi.
		if n.nextIndex[id] > 1 {
			n.nextIndex[id]--
		}
	}
}

// maybeAdvanceCommitIndexLocked cerca il più grande N tale per cui: N è
// maggiore del commitIndex attuale, la maggioranza dei nodi (noi compresi)
// ha replicato almeno fino a N, e l'entry a N appartiene al nostro term
// corrente. Quest'ultima condizione è la regola di sicurezza più sottile di
// Raft (paper, §5.4.2): un leader non può concludere che una entry di un
// term precedente sia committata solo contando le repliche — deve aspettare
// che si commiti (indirettamente) insieme a una entry del proprio term
// corrente. Il chiamante deve già tenere n.mu.
func (n *Node) maybeAdvanceCommitIndexLocked() {
	entries := n.storage.Log()
	lastIndex := uint64(len(entries))
	currentTerm := n.storage.CurrentTerm()
	majority := len(n.peers)/2 + 1

	for N := lastIndex; N > n.commitIndex; N-- {
		if entries[N-1].Term != currentTerm {
			continue
		}
		count := 1 // il leader ha sempre la propria entry
		for id := range n.peers {
			if id == n.id {
				continue
			}
			if n.matchIndex[id] >= N {
				count++
			}
		}
		if count >= majority {
			n.commitIndex = N
			n.applyCommittedLocked()
			break
		}
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
