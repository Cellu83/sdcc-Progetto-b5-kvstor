// Package kvstore implementa la state machine chiave-valore che Raft
// applicherà una volta che le entry di log sono committate.
package kvstore

import (
	"fmt"
	"sync"
)

// OpType identifica il tipo di operazione richiesta su una chiave.
type OpType int

const (
	OpPut OpType = iota
	OpDelete
)

func (op OpType) String() string {
	switch op {
	case OpPut:
		return "PUT"
	case OpDelete:
		return "DELETE"
	default:
		return "UNKNOWN"
	}
}

// Command è l'operazione applicativa che viaggia dentro una entry di log
// Raft e che, una volta committata dalla maggioranza, viene eseguita
// sulla state machine tramite Apply.
type Command struct {
	Op    OpType
	Key   string
	Value string // ignorato per OpDelete
}

// Store è una state machine chiave-valore in memoria. È deliberatamente
// disaccoppiata da qualsiasi logica di rete o di consenso: sa solo
// applicare comandi già decisi e rispondere a letture. L'accesso
// concorrente è protetto da un RWMutex perché più goroutine (handler gRPC
// del proxy, applicazione del log Raft) accederanno allo store in parallelo.
type Store struct {
	mu   sync.RWMutex
	data map[string]string
}

// New crea uno Store vuoto.
func New() *Store {
	return &Store{
		data: make(map[string]string),
	}
}

// Get restituisce il valore associato a key e true se la chiave è presente.
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	return v, ok
}

// Apply esegue un Command sulla state machine. È l'unico punto di mutazione
// dello stato: Raft lo chiamerà, in ordine, per ogni entry di log committata.
func (s *Store) Apply(cmd Command) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch cmd.Op {
	case OpPut:
		s.data[cmd.Key] = cmd.Value
		return nil
	case OpDelete:
		delete(s.data, cmd.Key)
		return nil
	default:
		return fmt.Errorf("comando sconosciuto: %v", cmd.Op)
	}
}

// Len restituisce il numero di chiavi correntemente presenti. Utile nei
// test e, più avanti, per verificare che due nodi siano conversi allo
// stesso stato dopo la replica.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}
