package proxy

import (
	"testing"
	"time"
)

func TestCircuitBreaker_StartsClosedAndAllows(t *testing.T) {
	cb := newCircuitBreaker(3, 100*time.Millisecond)
	if !cb.Allow() {
		t.Fatalf("un circuit breaker nuovo deve essere chiuso e permettere le chiamate")
	}
}

func TestCircuitBreaker_OpensAfterThresholdFailures(t *testing.T) {
	cb := newCircuitBreaker(3, time.Minute)

	cb.RecordFailure()
	cb.RecordFailure()
	if !cb.Allow() {
		t.Fatalf("prima di raggiungere la soglia il circuito deve restare chiuso")
	}

	cb.RecordFailure() // terzo fallimento consecutivo: raggiunge la soglia
	if cb.Allow() {
		t.Fatalf("dopo aver raggiunto la soglia di fallimenti il circuito deve aprirsi")
	}
}

func TestCircuitBreaker_SuccessResetsFailureCount(t *testing.T) {
	cb := newCircuitBreaker(3, time.Minute)

	cb.RecordFailure()
	cb.RecordFailure()
	cb.RecordSuccess() // azzera il contatore

	cb.RecordFailure()
	cb.RecordFailure()
	if !cb.Allow() {
		t.Fatalf("il contatore azzerato da un successo non deve far aprire il circuito dopo solo 2 nuovi fallimenti")
	}
}

func TestCircuitBreaker_HalfOpenAfterResetTimeout(t *testing.T) {
	cb := newCircuitBreaker(1, 20*time.Millisecond)

	cb.RecordFailure() // apre subito il circuito (soglia = 1)
	if cb.Allow() {
		t.Fatalf("il circuito appena aperto deve rifiutare le chiamate")
	}

	time.Sleep(30 * time.Millisecond)
	if !cb.Allow() {
		t.Fatalf("dopo resetTimeout il circuito deve concedere un tentativo di prova (half-open)")
	}
}

func TestCircuitBreaker_FailedTrialInHalfOpenReopens(t *testing.T) {
	cb := newCircuitBreaker(1, 20*time.Millisecond)

	cb.RecordFailure()
	time.Sleep(30 * time.Millisecond)
	cb.Allow() // consuma il tentativo half-open

	cb.RecordFailure() // il tentativo di prova fallisce
	if cb.Allow() {
		t.Fatalf("un tentativo di prova fallito in half-open deve far riaprire subito il circuito")
	}
}

func TestCircuitBreaker_SuccessfulTrialInHalfOpenCloses(t *testing.T) {
	cb := newCircuitBreaker(1, 20*time.Millisecond)

	cb.RecordFailure()
	time.Sleep(30 * time.Millisecond)
	cb.Allow() // consuma il tentativo half-open

	cb.RecordSuccess() // il tentativo di prova riesce
	if !cb.Allow() {
		t.Fatalf("un tentativo di prova riuscito deve richiudere il circuito")
	}
	if cb.state != closed {
		t.Fatalf("dopo un tentativo di prova riuscito lo stato deve essere closed, ottenuto %v", cb.state)
	}
}

func TestBreakerRegistry_IsolatesFailuresPerAddress(t *testing.T) {
	reg := newBreakerRegistry(1, time.Minute)

	cbA := reg.get("nodeA:1")
	cbB := reg.get("nodeB:1")

	cbA.RecordFailure() // apre il circuito solo per nodeA

	if cbA.Allow() {
		t.Fatalf("il circuito di nodeA deve essere aperto")
	}
	if !cbB.Allow() {
		t.Fatalf("il fallimento su nodeA non deve influenzare il circuito di nodeB")
	}
}

func TestBreakerRegistry_ReturnsSameBreakerForSameAddress(t *testing.T) {
	reg := newBreakerRegistry(3, time.Minute)

	cb1 := reg.get("nodeA:1")
	cb1.RecordFailure()

	cb2 := reg.get("nodeA:1")
	if cb1 != cb2 {
		t.Fatalf("get sullo stesso indirizzo deve restituire la stessa istanza di circuitBreaker")
	}
}
