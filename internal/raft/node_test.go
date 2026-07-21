package raft

import (
	"testing"
	"time"

	"github.com/Cellu83/sdcc-Progetto-b5-kvstor/internal/raftlog"
)

// newTestNode crea un Node isolato, con storage persistente su una cartella
// temporanea, senza peer reali: sufficiente per testare HandleRequestVote e
// HandleAppendEntries, che non fanno mai chiamate di rete (sono le altre
// funzioni, come startElection, a farne).
func newTestNode(t *testing.T, id string) *Node {
	t.Helper()
	storage, err := raftlog.Open(t.TempDir())
	if err != nil {
		t.Fatalf("raftlog.Open fallita: %v", err)
	}
	return NewNode(Config{
		ID:                 id,
		Peers:              map[string]string{id: "localhost:0"},
		ElectionTimeoutMin: 100 * time.Millisecond,
		ElectionTimeoutMax: 200 * time.Millisecond,
		HeartbeatInterval:  20 * time.Millisecond,
		RPCTimeout:         20 * time.Millisecond,
	}, storage, NewClient())
}

func TestHandleRequestVote_GrantsVoteForNewerTermWithUpToDateLog(t *testing.T) {
	n := newTestNode(t, "node1")

	result := n.HandleRequestVote(RequestVoteArgs{
		Term:         1,
		CandidateID:  "node2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if !result.VoteGranted {
		t.Fatalf("il voto dovrebbe essere concesso a un candidato con term più recente e log non indietro")
	}
	if result.Term != 1 {
		t.Fatalf("term atteso 1, ottenuto %d", result.Term)
	}
	if got := n.storage.VotedFor(); got != "node2" {
		t.Fatalf("il voto concesso deve essere persistito, ottenuto VotedFor=%q", got)
	}
}

func TestHandleRequestVote_RejectsOlderTerm(t *testing.T) {
	n := newTestNode(t, "node1")
	// Portiamo il nodo al term 5.
	_ = n.storage.SetTermAndVote(5, "")

	result := n.HandleRequestVote(RequestVoteArgs{Term: 3, CandidateID: "node2"})

	if result.VoteGranted {
		t.Fatalf("un candidato con term più vecchio del nostro non deve mai ricevere il voto")
	}
	if result.Term != 5 {
		t.Fatalf("la risposta deve riportare il nostro term corrente (5), ottenuto %d", result.Term)
	}
}

func TestHandleRequestVote_RejectsSecondVoteForDifferentCandidateSameTerm(t *testing.T) {
	n := newTestNode(t, "node1")

	first := n.HandleRequestVote(RequestVoteArgs{Term: 1, CandidateID: "node2"})
	if !first.VoteGranted {
		t.Fatalf("il primo voto nel term 1 dovrebbe essere concesso")
	}

	second := n.HandleRequestVote(RequestVoteArgs{Term: 1, CandidateID: "node3"})
	if second.VoteGranted {
		t.Fatalf("non si può votare due candidati diversi nello stesso term")
	}
}

func TestHandleRequestVote_GrantsSameCandidateTwiceSameTerm(t *testing.T) {
	n := newTestNode(t, "node1")

	first := n.HandleRequestVote(RequestVoteArgs{Term: 1, CandidateID: "node2"})
	second := n.HandleRequestVote(RequestVoteArgs{Term: 1, CandidateID: "node2"})

	if !first.VoteGranted || !second.VoteGranted {
		t.Fatalf("rivotare lo stesso candidato nello stesso term deve essere concesso entrambe le volte (es. messaggio duplicato)")
	}
}

func TestHandleRequestVote_RejectsWhenCandidateLogIsBehind(t *testing.T) {
	n := newTestNode(t, "node1")
	// Il nostro log ha già una entry al term 3, index 1.
	_ = n.storage.SetTermAndVote(3, "")
	_ = n.storage.Append(raftlog.Entry{Term: 3, Index: 1})

	// Il candidato ha un term di elezione più alto (4), ma il suo ultimo
	// log risale solo al term 2: è "indietro" rispetto a noi.
	result := n.HandleRequestVote(RequestVoteArgs{
		Term:         4,
		CandidateID:  "node2",
		LastLogIndex: 1,
		LastLogTerm:  2,
	})

	if result.VoteGranted {
		t.Fatalf("un candidato con log meno aggiornato del nostro non deve ricevere il voto, anche con term più alto")
	}
}

func TestHandleAppendEntries_RejectsOlderTerm(t *testing.T) {
	n := newTestNode(t, "node1")
	_ = n.storage.SetTermAndVote(5, "")

	result := n.HandleAppendEntries(AppendEntriesArgs{Term: 3, LeaderID: "node2"})

	if result.Success {
		t.Fatalf("un AppendEntries con term più vecchio del nostro deve essere rifiutato")
	}
	if result.Term != 5 {
		t.Fatalf("la risposta deve riportare il nostro term corrente (5), ottenuto %d", result.Term)
	}
}

func TestHandleAppendEntries_AcceptsAndStepsDownFromCandidate(t *testing.T) {
	n := newTestNode(t, "node1")

	// Simuliamo che il nodo si sia autopromosso Candidate per il term 2
	// (come farebbe startElection).
	n.mu.Lock()
	n.state = Candidate
	_ = n.storage.SetTermAndVote(2, "node1")
	n.mu.Unlock()

	result := n.HandleAppendEntries(AppendEntriesArgs{Term: 2, LeaderID: "node2"})

	if !result.Success {
		t.Fatalf("un AppendEntries con term >= al nostro da un leader legittimo deve essere accettato")
	}
	if got := n.State(); got != Follower {
		t.Fatalf("il nodo deve tornare Follower dopo aver riconosciuto un leader valido, stato attuale=%s", got)
	}
	if got := n.CurrentLeader(); got != "node2" {
		t.Fatalf("il leader riconosciuto deve essere 'node2', ottenuto %q", got)
	}
}

func TestHandleAppendEntries_PreservesVoteWhenTermUnchanged(t *testing.T) {
	n := newTestNode(t, "node1")
	_ = n.storage.SetTermAndVote(2, "node1") // abbiamo votato per noi stessi nel term 2

	// Un AppendEntries con lo STESSO term (non superiore) non deve
	// azzerare il voto già espresso in questo term.
	n.HandleAppendEntries(AppendEntriesArgs{Term: 2, LeaderID: "node3"})

	if got := n.storage.VotedFor(); got != "node1" {
		t.Fatalf("il voto nel term corrente non deve essere azzerato da un AppendEntries a term invariato, ottenuto %q", got)
	}
}

func TestStateString(t *testing.T) {
	cases := map[State]string{
		Follower:  "Follower",
		Candidate: "Candidate",
		Leader:    "Leader",
	}
	for state, want := range cases {
		if got := state.String(); got != want {
			t.Fatalf("State(%d).String() = %q, atteso %q", state, got, want)
		}
	}
}
