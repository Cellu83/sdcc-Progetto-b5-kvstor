// Package discovery implementa il pattern architetturale Service Discovery
// lato client: invece di avere indirizzi fissi del Leader scritti nel
// codice (che diventerebbero subito obsoleti, dato che il Leader cambia nel
// tempo), un componente esterno al cluster — qui, il Client proxy service —
// parte da un piccolo elenco statico di nodi "seed" e da lì costruisce e
// mantiene aggiornata da solo una vista di chi sono i peer e chi è
// l'attuale Leader, interrogando dinamicamente il cluster.
package discovery

import (
	"context"
	"fmt"
	"sync"
	"time"

	raftpb "github.com/Cellu83/sdcc-Progetto-b5-kvstor/proto/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Discovery mantiene la vista corrente del cluster (peer noti e Leader
// attuale), aggiornabile chiamando Refresh.
type Discovery struct {
	mu         sync.RWMutex
	peers      map[string]string // nodeID -> indirizzo gRPC, vista corrente
	leaderID   string
	rpcTimeout time.Duration
}

// New crea un Discovery a partire da una lista iniziale di nodi "seed" — il
// bootstrap minimo necessario per iniziare (esattamente come ogni
// consensus node riceve la propria lista di peer dalla configurazione).
// Da qui in avanti la vista si aggiorna da sola tramite Refresh.
func New(seeds map[string]string, rpcTimeout time.Duration) *Discovery {
	peers := make(map[string]string, len(seeds))
	for id, addr := range seeds {
		peers[id] = addr
	}
	return &Discovery{peers: peers, rpcTimeout: rpcTimeout}
}

// Refresh interroga i nodi conosciuti (non necessariamente il Leader,
// qualunque nodo raggiungibile risponde a GetStatus) finché non ne trova
// uno che risponde, e aggiorna la vista di peer e Leader con quanto
// riportato. Ritorna errore solo se NESSUN nodo conosciuto è raggiungibile.
func (d *Discovery) Refresh(ctx context.Context) error {
	d.mu.RLock()
	candidates := make(map[string]string, len(d.peers))
	for id, addr := range d.peers {
		candidates[id] = addr
	}
	d.mu.RUnlock()

	var lastErr error
	for _, addr := range candidates {
		status, err := d.queryStatus(ctx, addr)
		if err != nil {
			lastErr = err
			continue
		}

		d.mu.Lock()
		d.leaderID = status.GetLeaderId()
		if len(status.GetPeers()) > 0 {
			newPeers := make(map[string]string, len(status.GetPeers()))
			for _, p := range status.GetPeers() {
				newPeers[p.GetId()] = p.GetAddress()
			}
			d.peers = newPeers
		}
		d.mu.Unlock()
		return nil
	}

	return fmt.Errorf("nessun nodo raggiungibile per la discovery: %w", lastErr)
}

// queryStatus apre una connessione una tantum verso addr e chiama
// GetStatus. Non riusiamo/mettiamo in cache le connessioni qui: Refresh non
// è un percorso ad alta frequenza (viene chiamato solo quando la vista
// corrente si rivela sbagliata), quindi la semplicità conta più della
// prestazione marginale di una connessione riusata.
func (d *Discovery) queryStatus(ctx context.Context, addr string) (*raftpb.GetStatusResponse, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	callCtx, cancel := context.WithTimeout(ctx, d.rpcTimeout)
	defer cancel()

	return raftpb.NewRaftServiceClient(conn).GetStatus(callCtx, &raftpb.GetStatusRequest{})
}

// LeaderAddress restituisce l'indirizzo del Leader secondo la vista
// corrente, e true se lo conosciamo. Non fa I/O: legge solo lo stato in
// cache, aggiornato dall'ultima Refresh riuscita.
func (d *Discovery) LeaderAddress() (string, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.leaderID == "" {
		return "", false
	}
	addr, ok := d.peers[d.leaderID]
	return addr, ok
}

// Peers restituisce una copia della vista corrente dei peer noti.
func (d *Discovery) Peers() map[string]string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	out := make(map[string]string, len(d.peers))
	for id, addr := range d.peers {
		out[id] = addr
	}
	return out
}
