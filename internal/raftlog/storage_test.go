package raftlog

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Cellu83/sdcc-Progetto-b5-kvstor/internal/kvstore"
)

func TestOpenFreshDataDir(t *testing.T) {
	dir := t.TempDir()

	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open su data dir vuota non dovrebbe fallire: %v", err)
	}

	if s.CurrentTerm() != 0 {
		t.Fatalf("un nodo mai avviato deve avere CurrentTerm=0, ottenuto %d", s.CurrentTerm())
	}
	if s.VotedFor() != "" {
		t.Fatalf("un nodo mai avviato deve avere VotedFor vuoto, ottenuto %q", s.VotedFor())
	}
	if len(s.Log()) != 0 {
		t.Fatalf("un nodo mai avviato deve avere log vuoto, ottenuto %d entry", len(s.Log()))
	}
}

func TestSetTermAndVotePersistsAcrossRestart(t *testing.T) {
	dir := t.TempDir()

	s1, err := Open(dir)
	if err != nil {
		t.Fatalf("Open fallita: %v", err)
	}
	if err := s1.SetTermAndVote(5, "node2"); err != nil {
		t.Fatalf("SetTermAndVote fallita: %v", err)
	}

	// Simula un riavvio: nuova istanza di Storage sulla stessa data dir,
	// nessuno stato condiviso in memoria con s1.
	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("riapertura dopo 'riavvio' fallita: %v", err)
	}
	if s2.CurrentTerm() != 5 {
		t.Fatalf("CurrentTerm atteso 5 dopo riavvio, ottenuto %d", s2.CurrentTerm())
	}
	if s2.VotedFor() != "node2" {
		t.Fatalf("VotedFor atteso 'node2' dopo riavvio, ottenuto %q", s2.VotedFor())
	}
}

func TestAppendPersistsAcrossRestart(t *testing.T) {
	dir := t.TempDir()

	s1, err := Open(dir)
	if err != nil {
		t.Fatalf("Open fallita: %v", err)
	}

	entries := []Entry{
		{Term: 1, Index: 1, Command: kvstore.Command{Op: kvstore.OpPut, Key: "x", Value: "1"}},
		{Term: 1, Index: 2, Command: kvstore.Command{Op: kvstore.OpPut, Key: "y", Value: "2"}},
	}
	if err := s1.Append(entries...); err != nil {
		t.Fatalf("Append fallita: %v", err)
	}

	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("riapertura dopo 'riavvio' fallita: %v", err)
	}

	log := s2.Log()
	if len(log) != 2 {
		t.Fatalf("attese 2 entry dopo riavvio, ottenute %d", len(log))
	}
	if log[0] != entries[0] || log[1] != entries[1] {
		t.Fatalf("le entry rilette da disco non coincidono con quelle scritte: %+v", log)
	}
}

// TestCombinedStateSurvivesRestart è il test richiesto dalla Fase 2: scrive
// term, voto e log, "riavvia" il nodo (nuova istanza sulla stessa data dir),
// e verifica che lo stato osservato sia identico a quello scritto.
func TestCombinedStateSurvivesRestart(t *testing.T) {
	dir := t.TempDir()

	s1, _ := Open(dir)
	if err := s1.SetTermAndVote(3, "node1"); err != nil {
		t.Fatalf("SetTermAndVote fallita: %v", err)
	}
	entry := Entry{Term: 3, Index: 1, Command: kvstore.Command{Op: kvstore.OpPut, Key: "k", Value: "v"}}
	if err := s1.Append(entry); err != nil {
		t.Fatalf("Append fallita: %v", err)
	}

	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("riapertura dopo 'riavvio' fallita: %v", err)
	}

	if s2.CurrentTerm() != s1.CurrentTerm() {
		t.Fatalf("CurrentTerm non coincide dopo riavvio: prima=%d, dopo=%d", s1.CurrentTerm(), s2.CurrentTerm())
	}
	if s2.VotedFor() != s1.VotedFor() {
		t.Fatalf("VotedFor non coincide dopo riavvio: prima=%q, dopo=%q", s1.VotedFor(), s2.VotedFor())
	}
	log1, log2 := s1.Log(), s2.Log()
	if len(log1) != len(log2) || log1[0] != log2[0] {
		t.Fatalf("log non coincide dopo riavvio: prima=%+v, dopo=%+v", log1, log2)
	}
}

func TestLastLogIndexAndTermOnEmptyLog(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir)

	if s.LastLogIndex() != 0 {
		t.Fatalf("LastLogIndex su log vuoto deve essere 0, ottenuto %d", s.LastLogIndex())
	}
	if s.LastLogTerm() != 0 {
		t.Fatalf("LastLogTerm su log vuoto deve essere 0, ottenuto %d", s.LastLogTerm())
	}
}

func TestLastLogIndexAndTermAfterAppend(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir)

	_ = s.Append(
		Entry{Term: 1, Index: 1, Command: kvstore.Command{Op: kvstore.OpPut, Key: "a", Value: "1"}},
		Entry{Term: 2, Index: 2, Command: kvstore.Command{Op: kvstore.OpPut, Key: "b", Value: "2"}},
	)

	if s.LastLogIndex() != 2 {
		t.Fatalf("LastLogIndex atteso 2, ottenuto %d", s.LastLogIndex())
	}
	if s.LastLogTerm() != 2 {
		t.Fatalf("LastLogTerm atteso 2, ottenuto %d", s.LastLogTerm())
	}
}

func TestTruncateRemovesEntryAndEverythingAfter(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir)

	_ = s.Append(
		Entry{Term: 1, Index: 1, Command: kvstore.Command{Op: kvstore.OpPut, Key: "a", Value: "1"}},
		Entry{Term: 1, Index: 2, Command: kvstore.Command{Op: kvstore.OpPut, Key: "b", Value: "2"}},
		Entry{Term: 1, Index: 3, Command: kvstore.Command{Op: kvstore.OpPut, Key: "c", Value: "3"}},
	)

	if err := s.Truncate(2); err != nil {
		t.Fatalf("Truncate fallita: %v", err)
	}

	log := s.Log()
	if len(log) != 1 {
		t.Fatalf("dopo Truncate(2) deve restare solo 1 entry, ottenute %d", len(log))
	}
	if log[0].Index != 1 {
		t.Fatalf("l'unica entry rimasta deve avere Index=1, ottenuto %d", log[0].Index)
	}
}

func TestTruncateBeyondLogIsNoOp(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir)
	_ = s.Append(Entry{Term: 1, Index: 1, Command: kvstore.Command{Op: kvstore.OpPut, Key: "a", Value: "1"}})

	if err := s.Truncate(5); err != nil {
		t.Fatalf("Truncate oltre la fine del log non dovrebbe fallire: %v", err)
	}
	if len(s.Log()) != 1 {
		t.Fatalf("Truncate oltre la fine del log non deve rimuovere nulla, ottenute %d entry", len(s.Log()))
	}
}

func TestTruncatePersistsAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	s1, _ := Open(dir)
	_ = s1.Append(
		Entry{Term: 1, Index: 1, Command: kvstore.Command{Op: kvstore.OpPut, Key: "a", Value: "1"}},
		Entry{Term: 1, Index: 2, Command: kvstore.Command{Op: kvstore.OpPut, Key: "b", Value: "2"}},
	)
	_ = s1.Truncate(2)

	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("riapertura dopo 'riavvio' fallita: %v", err)
	}
	if len(s2.Log()) != 1 {
		t.Fatalf("il troncamento deve sopravvivere a un riavvio, ottenute %d entry", len(s2.Log()))
	}
}

// TestNoLingeringTmpFileAfterPersist verifica che il file temporaneo usato
// per la scrittura atomica non resti sul disco dopo una persist riuscita.
func TestNoLingeringTmpFileAfterPersist(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir)

	if err := s.SetTermAndVote(1, "node1"); err != nil {
		t.Fatalf("SetTermAndVote fallita: %v", err)
	}

	tmpPath := filepath.Join(dir, stateFileName+".tmp")
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Fatalf("file temporaneo %q non dovrebbe esistere dopo una persist riuscita", tmpPath)
	}
}
