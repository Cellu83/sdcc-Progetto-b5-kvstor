package proxy

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/Cellu83/sdcc-Progetto-b5-kvstor/internal/discovery"
	"github.com/Cellu83/sdcc-Progetto-b5-kvstor/internal/kvstore"
	"github.com/Cellu83/sdcc-Progetto-b5-kvstor/internal/raft"
	"github.com/Cellu83/sdcc-Progetto-b5-kvstor/internal/raftlog"
	kvstorepb "github.com/Cellu83/sdcc-Progetto-b5-kvstor/proto/kvstore"
	raftpb "github.com/Cellu83/sdcc-Progetto-b5-kvstor/proto/raft"
	"google.golang.org/grpc"
)

// testNode raggruppa un Node Raft reale con il proprio server gRPC (che
// espone sia RaftService che KVStoreService, esattamente come fa
// cmd/consensus-node), per poter testare il Proxy end-to-end senza
// avviare processi separati.
type testNode struct {
	node   *raft.Node
	server *grpc.Server
}

// startTestCluster avvia n consensus node reali su porte locali libere.
func startTestCluster(t *testing.T, n int) (nodes []*testNode, peers map[string]string) {
	t.Helper()

	ids := make([]string, n)
	listeners := make([]net.Listener, n)
	for i := 0; i < n; i++ {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("net.Listen fallita: %v", err)
		}
		listeners[i] = lis
		ids[i] = fmt.Sprintf("node%d", i+1)
	}

	peers = make(map[string]string, n)
	for i, id := range ids {
		peers[id] = listeners[i].Addr().String()
	}

	for i := 0; i < n; i++ {
		storage, err := raftlog.Open(t.TempDir())
		if err != nil {
			t.Fatalf("raftlog.Open fallita: %v", err)
		}
		store := kvstore.New()
		node := raft.NewNode(raft.Config{
			ID:                 ids[i],
			Peers:              peers,
			ElectionTimeoutMin: 100 * time.Millisecond,
			ElectionTimeoutMax: 200 * time.Millisecond,
			HeartbeatInterval:  20 * time.Millisecond,
			RPCTimeout:         50 * time.Millisecond,
		}, storage, store, raft.NewClient())

		grpcServer := grpc.NewServer()
		raftpb.RegisterRaftServiceServer(grpcServer, &raft.Server{Node: node})
		kvstorepb.RegisterKVStoreServiceServer(grpcServer, &raft.KVServer{Node: node})

		tn := &testNode{node: node, server: grpcServer}
		nodes = append(nodes, tn)

		go grpcServer.Serve(listeners[i])
		node.Run()
	}

	t.Cleanup(func() {
		for _, tn := range nodes {
			tn.node.Stop()
			tn.server.Stop()
		}
	})

	return nodes, peers
}

func waitForClusterLeader(t *testing.T, nodes []*testNode, timeout time.Duration) *raft.Node {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, tn := range nodes {
			if tn.node.State() == raft.Leader {
				return tn.node
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("nessun leader eletto entro %s", timeout)
	return nil
}

func newTestProxy(peers map[string]string) *Proxy {
	disc := discovery.New(peers, 100*time.Millisecond)
	return New(Config{
		Discovery:               disc,
		RPCTimeout:              100 * time.Millisecond,
		MaxRetries:              20,
		RetryBackoff:            20 * time.Millisecond,
		CircuitBreakerThreshold: 2,
		CircuitBreakerReset:     100 * time.Millisecond,
	})
}

func TestProxy_PutAndGetGoThroughLeader(t *testing.T) {
	nodes, peers := startTestCluster(t, 3)
	waitForClusterLeader(t, nodes, 2*time.Second)

	p := newTestProxy(peers)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := p.Put(ctx, "nome", "federico"); err != nil {
		t.Fatalf("Put tramite il proxy fallita: %v", err)
	}

	value, found, err := p.Get(ctx, "nome")
	if err != nil {
		t.Fatalf("Get tramite il proxy fallita: %v", err)
	}
	if !found || value != "federico" {
		t.Fatalf("valore atteso 'federico', found=%v value=%q", found, value)
	}
}

// TestProxy_RedirectsAfterLeaderCrash è la verifica centrale della Fase 6:
// uccidiamo il Leader che il proxy stava usando, e verifichiamo che una
// nuova richiesta (dopo la ri-elezione del cluster) vada comunque a buon
// fine, senza nessun intervento manuale — il proxy si ridirige da solo.
func TestProxy_RedirectsAfterLeaderCrash(t *testing.T) {
	nodes, peers := startTestCluster(t, 3)
	leader := waitForClusterLeader(t, nodes, 2*time.Second)

	p := newTestProxy(peers)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := p.Put(ctx, "x", "1"); err != nil {
		t.Fatalf("Put iniziale fallita: %v", err)
	}

	// Troviamo e "uccidiamo" il nodo Leader: fermiamo sia il suo server
	// gRPC (diventa irraggiungibile) sia il suo ciclo Raft.
	for _, tn := range nodes {
		if tn.node == leader {
			tn.server.Stop()
			tn.node.Stop()
			break
		}
	}

	// Il cluster superstite deve rieleggere un nuovo leader da solo
	// (Fase 4); il proxy deve accorgersi che il vecchio leader non
	// risponde più, aggiornare la propria vista via Discovery, e
	// ritrovare il nuovo leader — tutto dentro la stessa chiamata Put,
	// grazie al meccanismo di retry.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel2()

	if err := p.Put(ctx2, "y", "2"); err != nil {
		t.Fatalf("Put dopo il crash del leader avrebbe dovuto ridirigersi da sola e riuscire, invece: %v", err)
	}

	value, found, err := p.Get(ctx2, "y")
	if err != nil || !found || value != "2" {
		t.Fatalf("Get dopo il failover fallita: err=%v found=%v value=%q", err, found, value)
	}
}
