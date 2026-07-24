package raftlog

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

const stateFileName = "raft_state.json"

// State è la porzione di stato di Raft che deve sopravvivere a un riavvio
// del nodo: il mandato corrente, l'eventuale voto già espresso in quel
// mandato, e l'intero log delle entry.
type State struct {
	CurrentTerm uint64  `json:"current_term"`
	VotedFor    string  `json:"voted_for"`
	Log         []Entry `json:"log"`
}

// Storage gestisce la lettura/scrittura su disco di State per un singolo
// nodo. Ogni mutazione viene persistita immediatamente e in modo atomico:
// se il processo crasha a metà scrittura, il file su disco resta valido
// (o la versione vecchia, o quella nuova completa — mai uno stato a metà).
type Storage struct {
	mu    sync.Mutex
	path  string
	state State
}

// Open apre (o crea, se non esiste ancora) lo storage persistente nella
// cartella dataDir. Se il nodo non ha mai scritto nulla su disco, restituisce
// uno Storage con stato azzerato (CurrentTerm=0, VotedFor="", Log vuoto) —
// esattamente lo stato di un nodo che entra per la prima volta nel cluster.
func Open(dataDir string) (*Storage, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("creazione data dir %q: %w", dataDir, err)
	}

	s := &Storage{path: filepath.Join(dataDir, stateFileName)}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Storage) load() error {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		s.state = State{}
		return nil
	}
	if err != nil {
		return fmt.Errorf("lettura stato persistito %q: %w", s.path, err)
	}

	if err := json.Unmarshal(data, &s.state); err != nil {
		return fmt.Errorf("parsing stato persistito %q: %w", s.path, err)
	}
	return nil
}

// persist scrive lo stato corrente su disco in modo atomico: prima su un
// file temporaneo, poi con una rename sopra il file definitivo. Il chiamante
// deve già tenere s.mu.
func (s *Storage) persist() error {
	data, err := json.MarshalIndent(s.state, "", "  ")
	if err != nil {
		return fmt.Errorf("serializzazione stato: %w", err)
	}

	tmpPath := s.path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return fmt.Errorf("scrittura file temporaneo %q: %w", tmpPath, err)
	}
	if err := os.Rename(tmpPath, s.path); err != nil {
		return fmt.Errorf("rename %q -> %q: %w", tmpPath, s.path, err)
	}
	return nil
}

// CurrentTerm restituisce il mandato corrente persistito.
func (s *Storage) CurrentTerm() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state.CurrentTerm
}

// VotedFor restituisce l'ID del candidato per cui questo nodo ha votato
// nel mandato corrente ("" se non ha ancora votato in questo mandato).
func (s *Storage) VotedFor() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state.VotedFor
}

// SetTermAndVote aggiorna atomicamente mandato corrente e voto espresso, e
// persiste subito su disco. In Raft, term e voto vanno sempre aggiornati e
// salvati insieme: è così che un nodo, dopo un riavvio, ricorda di aver già
// votato in un dato mandato ed evita di votare due volte per candidati
// diversi nello stesso mandato (violando la sicurezza dell'elezione).
func (s *Storage) SetTermAndVote(term uint64, votedFor string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state.CurrentTerm = term
	s.state.VotedFor = votedFor
	return s.persist()
}

// Log restituisce una copia del log persistito.
func (s *Storage) Log() []Entry {
	s.mu.Lock()
	defer s.mu.Unlock()
	logCopy := make([]Entry, len(s.state.Log))
	copy(logCopy, s.state.Log)
	return logCopy
}

// Append accoda una o più entry al log e persiste subito su disco.
func (s *Storage) Append(entries ...Entry) error {
	if len(entries) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state.Log = append(s.state.Log, entries...)
	return s.persist()
}

// Truncate rimuove dal log tutte le entry a partire da fromIndex (incluso)
// fino alla fine, e persiste subito il log troncato. Serve quando un
// follower riceve dal leader un'entry in conflitto con una già presente
// allo stesso indice (term diverso): quella entry e tutte quelle dopo di
// lei non erano mai state confermate dalla maggioranza, quindi vanno
// scartate per lasciare spazio alla versione del leader corrente.
func (s *Storage) Truncate(fromIndex uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if fromIndex == 0 || fromIndex > uint64(len(s.state.Log)) {
		return nil
	}
	s.state.Log = s.state.Log[:fromIndex-1]
	return s.persist()
}

// LastLogIndex restituisce l'Index dell'ultima entry del log, 0 se il log
// è vuoto.
func (s *Storage) LastLogIndex() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.state.Log) == 0 {
		return 0
	}
	return s.state.Log[len(s.state.Log)-1].Index
}

// LastLogTerm restituisce il Term dell'ultima entry del log, 0 se il log
// è vuoto.
func (s *Storage) LastLogTerm() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.state.Log) == 0 {
		return 0
	}
	return s.state.Log[len(s.state.Log)-1].Term
}
