package raft

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/Cellu83/sdcc-Progetto-b5-kvstor/internal/kvstore"
	"github.com/Cellu83/sdcc-Progetto-b5-kvstor/internal/raftlog"
	raftpb "github.com/Cellu83/sdcc-Progetto-b5-kvstor/proto/raft"
	"google.golang.org/grpc"
)

// testCluster avvia n nodi Raft reali, ciascuno con un vero server gRPC su
// localhost (porta assegnata dal sistema operativo), per testare elezione e
// replica end-to-end senza dover avviare processi separati a mano.
type testCluster struct {
	nodes   []*Node
	servers []*grpc.Server
}

func newTestCluster(t *testing.T, n int) *testCluster {
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

	peers := make(map[string]string, n)
	for i, id := range ids {
		peers[id] = listeners[i].Addr().String()
	}

	cluster := &testCluster{}
	for i := 0; i < n; i++ {
		storage, err := raftlog.Open(t.TempDir())
		if err != nil {
			t.Fatalf("raftlog.Open fallita: %v", err)
		}
		store := kvstore.New()
		node := NewNode(Config{
			ID:                 ids[i],
			Peers:              peers,
			ElectionTimeoutMin: 100 * time.Millisecond,
			ElectionTimeoutMax: 200 * time.Millisecond,
			HeartbeatInterval:  20 * time.Millisecond,
			RPCTimeout:         50 * time.Millisecond,
		}, storage, store, NewClient())

		grpcServer := grpc.NewServer()
		raftpb.RegisterRaftServiceServer(grpcServer, &Server{Node: node})

		cluster.nodes = append(cluster.nodes, node)
		cluster.servers = append(cluster.servers, grpcServer)

		go grpcServer.Serve(listeners[i])
		node.Run()
	}

	t.Cleanup(func() {
		for _, node := range cluster.nodes {
			node.Stop()
		}
		for _, s := range cluster.servers {
			s.Stop()
		}
	})

	return cluster
}

// waitForLeader interroga il cluster finché non emerge esattamente un
// Leader, o fallisce il test se non succede entro timeout.
func (c *testCluster) waitForLeader(t *testing.T, timeout time.Duration) *Node {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, node := range c.nodes {
			if node.State() == Leader {
				return node
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("nessun leader eletto entro %s", timeout)
	return nil
}

// TestReplication_WriteOnLeaderReachesFollowers è la verifica end-to-end
// della Fase 5: una scrittura proposta sul Leader deve, dopo la replica,
// essere leggibile dalla state machine di ogni nodo del cluster, Leader
// compreso.
func TestReplication_WriteOnLeaderReachesFollowers(t *testing.T) {
	cluster := newTestCluster(t, 3)
	leader := cluster.waitForLeader(t, 2*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := leader.Propose(ctx, kvstore.Command{Op: kvstore.OpPut, Key: "x", Value: "42"}); err != nil {
		t.Fatalf("Propose sul leader fallita: %v", err)
	}

	for _, node := range cluster.nodes {
		deadline := time.Now().Add(time.Second)
		var value string
		var found bool
		for time.Now().Before(deadline) {
			value, found = node.Get("x")
			if found {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		if !found || value != "42" {
			t.Fatalf("il nodo non ha replicato correttamente la scrittura: found=%v value=%q", found, value)
		}
	}
}

// TestReplication_FollowerRejectsDirectWrite verifica che un nodo che non
// è Leader rifiuti sempre una Propose diretta, invece di accettare
// scritture che scavalcherebbero il consenso.
func TestReplication_FollowerRejectsDirectWrite(t *testing.T) {
	cluster := newTestCluster(t, 3)
	leader := cluster.waitForLeader(t, 2*time.Second)

	var follower *Node
	for _, node := range cluster.nodes {
		if node != leader {
			follower = node
			break
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err := follower.Propose(ctx, kvstore.Command{Op: kvstore.OpPut, Key: "x", Value: "1"})
	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("una Propose su un follower deve fallire con ErrNotLeader, ottenuto %v", err)
	}
}
