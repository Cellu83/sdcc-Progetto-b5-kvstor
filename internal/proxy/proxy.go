// Package proxy implementa il Client proxy service: riceve le richieste
// Get/Put/Delete dall'esterno, scopre dinamicamente (via internal/discovery)
// quale nodo consensus è l'attuale Leader, instrada la richiesta lì, e
// gestisce in modo trasparente sia il cambio di Leader sia i nodi che non
// rispondono (tramite un Circuit Breaker per nodo).
package proxy

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Cellu83/sdcc-Progetto-b5-kvstor/internal/discovery"
	kvstorepb "github.com/Cellu83/sdcc-Progetto-b5-kvstor/proto/kvstore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Config raccoglie i parametri con cui costruire un Proxy, tutti
// provenienti dalla configurazione (nessun valore hard-coded).
type Config struct {
	Discovery               *discovery.Discovery
	RPCTimeout              time.Duration
	MaxRetries              int
	RetryBackoff            time.Duration
	CircuitBreakerThreshold int
	CircuitBreakerReset     time.Duration
}

// Proxy è il Client proxy service. È stateless: non conserva nessun dato
// applicativo, solo la vista del cluster (in Discovery) e lo stato dei
// circuit breaker per nodo — entrambi ricostruibili da zero se il proxy
// stesso viene riavviato.
type Proxy struct {
	discovery    *discovery.Discovery
	breakers     *breakerRegistry
	rpcTimeout   time.Duration
	maxRetries   int
	retryBackoff time.Duration
}

// New costruisce un Proxy pronto all'uso.
func New(cfg Config) *Proxy {
	return &Proxy{
		discovery:    cfg.Discovery,
		breakers:     newBreakerRegistry(cfg.CircuitBreakerThreshold, cfg.CircuitBreakerReset),
		rpcTimeout:   cfg.RPCTimeout,
		maxRetries:   cfg.MaxRetries,
		retryBackoff: cfg.RetryBackoff,
	}
}

// Get instrada una lettura verso il Leader corrente.
func (p *Proxy) Get(ctx context.Context, key string) (value string, found bool, err error) {
	err = p.callLeader(ctx, func(callCtx context.Context, client kvstorepb.KVStoreServiceClient) error {
		resp, err := client.Get(callCtx, &kvstorepb.GetRequest{Key: key})
		if err != nil {
			return err
		}
		value, found = resp.GetValue(), resp.GetFound()
		return nil
	})
	return value, found, err
}

// Put instrada una scrittura verso il Leader corrente.
func (p *Proxy) Put(ctx context.Context, key, value string) error {
	return p.callLeader(ctx, func(callCtx context.Context, client kvstorepb.KVStoreServiceClient) error {
		_, err := client.Put(callCtx, &kvstorepb.PutRequest{Key: key, Value: value})
		return err
	})
}

// Delete instrada una cancellazione verso il Leader corrente.
func (p *Proxy) Delete(ctx context.Context, key string) error {
	return p.callLeader(ctx, func(callCtx context.Context, client kvstorepb.KVStoreServiceClient) error {
		_, err := client.Delete(callCtx, &kvstorepb.DeleteRequest{Key: key})
		return err
	})
}

// callLeader è il cuore del proxy: trova l'indirizzo del Leader secondo la
// vista corrente di Discovery, verifica il Circuit Breaker di quel nodo,
// esegue fn, e in caso di fallimento aggiorna la vista del cluster e
// riprova (fino a maxRetries). È così che il proxy "si ridirige da solo"
// se il Leader che conosceva è nel frattempo crashato o è cambiato.
func (p *Proxy) callLeader(ctx context.Context, fn func(ctx context.Context, client kvstorepb.KVStoreServiceClient) error) error {
	var lastErr error

	for attempt := 0; attempt < p.maxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-time.After(p.retryBackoff):
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		addr, ok := p.discovery.LeaderAddress()
		if !ok {
			if err := p.discovery.Refresh(ctx); err != nil {
				lastErr = err
				continue
			}
			addr, ok = p.discovery.LeaderAddress()
			if !ok {
				lastErr = errors.New("leader sconosciuto dopo la discovery")
				continue
			}
		}

		breaker := p.breakers.get(addr)
		if !breaker.Allow() {
			lastErr = fmt.Errorf("circuito aperto per il nodo %s, salto il tentativo", addr)
			_ = p.discovery.Refresh(ctx)
			continue
		}

		err := p.invoke(ctx, addr, fn)
		if err == nil {
			breaker.RecordSuccess()
			return nil
		}

		breaker.RecordFailure()
		lastErr = err
		// La chiamata è fallita: potrebbe essere perché il Leader è
		// cambiato o è caduto. Aggiorniamo subito la vista del cluster,
		// così il prossimo tentativo (se resta budget) punta al nodo giusto.
		_ = p.discovery.Refresh(ctx)
	}

	return fmt.Errorf("richiesta fallita dopo %d tentativi: %w", p.maxRetries, lastErr)
}

func (p *Proxy) invoke(ctx context.Context, addr string, fn func(ctx context.Context, client kvstorepb.KVStoreServiceClient) error) error {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()

	callCtx, cancel := context.WithTimeout(ctx, p.rpcTimeout)
	defer cancel()

	return fn(callCtx, kvstorepb.NewKVStoreServiceClient(conn))
}
