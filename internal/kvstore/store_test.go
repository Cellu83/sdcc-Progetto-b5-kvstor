package kvstore

import (
	"sync"
	"testing"
)

func TestGetMissingKey(t *testing.T) {
	s := New()

	_, ok := s.Get("assente")
	if ok {
		t.Fatalf("Get su chiave mai scritta dovrebbe restituire ok=false")
	}
}

func TestApplyPutThenGet(t *testing.T) {
	s := New()

	if err := s.Apply(Command{Op: OpPut, Key: "nome", Value: "federico"}); err != nil {
		t.Fatalf("Apply(PUT) inatteso errore: %v", err)
	}

	v, ok := s.Get("nome")
	if !ok {
		t.Fatalf("Get dopo PUT dovrebbe trovare la chiave")
	}
	if v != "federico" {
		t.Fatalf("valore atteso 'federico', ottenuto %q", v)
	}
}

func TestApplyPutOverwritesExistingKey(t *testing.T) {
	s := New()

	_ = s.Apply(Command{Op: OpPut, Key: "x", Value: "1"})
	_ = s.Apply(Command{Op: OpPut, Key: "x", Value: "2"})

	v, _ := s.Get("x")
	if v != "2" {
		t.Fatalf("una seconda PUT sulla stessa chiave deve sovrascrivere il valore, ottenuto %q", v)
	}
}

func TestApplyDelete(t *testing.T) {
	s := New()

	_ = s.Apply(Command{Op: OpPut, Key: "x", Value: "1"})
	if err := s.Apply(Command{Op: OpDelete, Key: "x"}); err != nil {
		t.Fatalf("Apply(DELETE) inatteso errore: %v", err)
	}

	if _, ok := s.Get("x"); ok {
		t.Fatalf("la chiave dovrebbe essere sparita dopo DELETE")
	}
}

func TestApplyDeleteOnMissingKeyIsNoOp(t *testing.T) {
	s := New()

	if err := s.Apply(Command{Op: OpDelete, Key: "mai-esistita"}); err != nil {
		t.Fatalf("DELETE su chiave assente non deve fallire, ottenuto errore: %v", err)
	}
}

func TestApplyUnknownOpReturnsError(t *testing.T) {
	s := New()

	err := s.Apply(Command{Op: OpType(99), Key: "x", Value: "1"})
	if err == nil {
		t.Fatalf("Apply con OpType sconosciuto dovrebbe restituire un errore")
	}
}

func TestLen(t *testing.T) {
	s := New()

	if s.Len() != 0 {
		t.Fatalf("uno store nuovo deve avere Len() == 0")
	}

	_ = s.Apply(Command{Op: OpPut, Key: "a", Value: "1"})
	_ = s.Apply(Command{Op: OpPut, Key: "b", Value: "2"})

	if s.Len() != 2 {
		t.Fatalf("dopo 2 PUT su chiavi diverse Len() deve essere 2, ottenuto %d", s.Len())
	}
}

func TestSnapshotReflectsAppliedCommands(t *testing.T) {
	s := New()
	_ = s.Apply(Command{Op: OpPut, Key: "a", Value: "1"})
	_ = s.Apply(Command{Op: OpPut, Key: "b", Value: "2"})

	snap := s.Snapshot()
	if len(snap) != 2 || snap["a"] != "1" || snap["b"] != "2" {
		t.Fatalf("snapshot atteso {a:1, b:2}, ottenuto %v", snap)
	}
}

func TestSnapshotReturnsIndependentCopy(t *testing.T) {
	s := New()
	_ = s.Apply(Command{Op: OpPut, Key: "a", Value: "1"})

	snap := s.Snapshot()
	snap["a"] = "modificato-fuori-dallo-store"

	v, _ := s.Get("a")
	if v != "1" {
		t.Fatalf("modificare la mappa restituita da Snapshot non deve alterare lo Store, ottenuto %q", v)
	}
}

// TestConcurrentAccess verifica che Store sia sicuro sotto accesso
// concorrente: molte goroutine scrivono e leggono in parallelo. Va
// eseguito con `go test -race` per far emergere eventuali data race.
func TestConcurrentAccess(t *testing.T) {
	s := New()
	var wg sync.WaitGroup

	const n = 100
	for i := 0; i < n; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			_ = s.Apply(Command{Op: OpPut, Key: "chiave", Value: "v"})
		}(i)
		go func(i int) {
			defer wg.Done()
			s.Get("chiave")
		}(i)
	}
	wg.Wait()
}
