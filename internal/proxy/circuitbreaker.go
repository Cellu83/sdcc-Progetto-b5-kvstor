package proxy

import (
	"sync"
	"time"
)

// breakerState rappresenta i 3 stati classici del pattern Circuit Breaker.
type breakerState int

const (
	// closed: funzionamento normale, le chiamate passano.
	closed breakerState = iota
	// open: troppi fallimenti consecutivi, le chiamate vengono rifiutate
	// subito, senza nemmeno provare la rete, finché non scade resetTimeout.
	open
	// halfOpen: resetTimeout è scaduto, concediamo UNA chiamata di prova
	// per capire se il nodo è tornato disponibile.
	halfOpen
)

// circuitBreaker implementa il pattern Circuit Breaker per le chiamate
// verso un singolo nodo consensus. Serve a evitare che il proxy continui a
// martellare di richieste (e ad aspettare inutilmente il timeout di rete)
// un nodo che sappiamo già essere caduto: dopo troppi fallimenti
// consecutivi, l'interruttore "si apre" e le chiamate verso quel nodo
// vengono scartate immediatamente per un periodo di raffreddamento.
type circuitBreaker struct {
	mu sync.Mutex

	state            breakerState
	failureCount     int
	failureThreshold int
	resetTimeout     time.Duration
	openUntil        time.Time
}

func newCircuitBreaker(failureThreshold int, resetTimeout time.Duration) *circuitBreaker {
	return &circuitBreaker{
		failureThreshold: failureThreshold,
		resetTimeout:     resetTimeout,
	}
}

// Allow decide se una nuova chiamata può essere tentata ora. Se
// l'interruttore è aperto ma il periodo di raffreddamento è scaduto,
// transiziona da solo a halfOpen e concede il tentativo di prova.
func (cb *circuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case closed:
		return true
	case halfOpen:
		return true
	case open:
		if time.Now().After(cb.openUntil) {
			cb.state = halfOpen
			return true
		}
		return false
	default:
		return true
	}
}

// RecordSuccess segnala che l'ultima chiamata è andata a buon fine:
// l'interruttore torna (o resta) chiuso, e il contatore dei fallimenti si
// azzera.
func (cb *circuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failureCount = 0
	cb.state = closed
}

// RecordFailure segnala che l'ultima chiamata è fallita. Se eravamo in
// halfOpen (il tentativo di prova è fallito) o abbiamo raggiunto la soglia
// di fallimenti consecutivi, l'interruttore si apre per resetTimeout.
func (cb *circuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failureCount++
	if cb.state == halfOpen || cb.failureCount >= cb.failureThreshold {
		cb.state = open
		cb.openUntil = time.Now().Add(cb.resetTimeout)
		cb.failureCount = 0
	}
}

// breakerRegistry tiene un circuitBreaker distinto per ogni indirizzo di
// nodo contattato: un nodo caduto non deve "contaminare" lo stato del
// circuito degli altri nodi, sani.
type breakerRegistry struct {
	mu               sync.Mutex
	breakers         map[string]*circuitBreaker
	failureThreshold int
	resetTimeout     time.Duration
}

func newBreakerRegistry(failureThreshold int, resetTimeout time.Duration) *breakerRegistry {
	return &breakerRegistry{
		breakers:         make(map[string]*circuitBreaker),
		failureThreshold: failureThreshold,
		resetTimeout:     resetTimeout,
	}
}

func (r *breakerRegistry) get(addr string) *circuitBreaker {
	r.mu.Lock()
	defer r.mu.Unlock()

	cb, ok := r.breakers[addr]
	if !ok {
		cb = newCircuitBreaker(r.failureThreshold, r.resetTimeout)
		r.breakers[addr] = cb
	}
	return cb
}
