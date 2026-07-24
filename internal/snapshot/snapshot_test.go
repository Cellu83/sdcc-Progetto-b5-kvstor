package snapshot

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Cellu83/sdcc-Progetto-b5-kvstor/internal/discovery"
	"github.com/Cellu83/sdcc-Progetto-b5-kvstor/internal/kvstore"
	"github.com/Cellu83/sdcc-Progetto-b5-kvstor/internal/raft"
	"github.com/Cellu83/sdcc-Progetto-b5-kvstor/internal/raftlog"
	raftpb "github.com/Cellu83/sdcc-Progetto-b5-kvstor/proto/raft"
	"google.golang.org/grpc"
)

// testNode raggruppa un Node Raft reale con il proprio server gRPC, per
// testare lo Snapshot service end-to-end senza avviare processi separati.
type testNode struct {
	node   *raft.Node
	server *grpc.Server
}

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

func readCheckpoint(t *testing.T, dir string) Checkpoint {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, checkpointFileName))
	if err != nil {
		t.Fatalf("lettura checkpoint fallita: %v", err)
	}
	var c Checkpoint
	if err := json.Unmarshal(data, &c); err != nil {
		t.Fatalf("parsing checkpoint fallito: %v", err)
	}
	return c
}

// TestRunOnce_WritesCheckpointReflectingState è la verifica centrale della
// Fase 7: dopo alcune scritture sul cluster, un ciclo di RunOnce deve
// produrre un checkpoint coerente con lo stato applicato.
func TestRunOnce_WritesCheckpointReflectingState(t *testing.T) {
	nodes, peers := startTestCluster(t, 3)
	leader := waitForClusterLeader(t, nodes, 2*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := leader.Propose(ctx, kvstore.Command{Op: kvstore.OpPut, Key: "x", Value: "1"}); err != nil {
		t.Fatalf("Propose fallita: %v", err)
	}
	if err := leader.Propose(ctx, kvstore.Command{Op: kvstore.OpPut, Key: "y", Value: "2"}); err != nil {
		t.Fatalf("Propose fallita: %v", err)
	}

	outputDir := t.TempDir()
	disc := discovery.New(peers, 100*time.Millisecond)
	svc := New(disc, outputDir, 200*time.Millisecond)

	if err := svc.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce fallita: %v", err)
	}

	checkpoint := readCheckpoint(t, outputDir)
	if len(checkpoint.Data) != 2 || checkpoint.Data["x"] != "1" || checkpoint.Data["y"] != "2" {
		t.Fatalf("checkpoint atteso {x:1, y:2}, ottenuto %v", checkpoint.Data)
	}
	if checkpoint.LastIncludedIndex < 2 {
		t.Fatalf("lastIncludedIndex atteso >= 2, ottenuto %d", checkpoint.LastIncludedIndex)
	}
}

// TestRunOnce_CheckpointUpdatesAfterMoreWrites verifica che, dopo altre
// scritture, un nuovo ciclo produca un checkpoint aggiornato: lo snapshot
// non resta "congelato" al primo giro.
func TestRunOnce_CheckpointUpdatesAfterMoreWrites(t *testing.T) {
	nodes, peers := startTestCluster(t, 3)
	leader := waitForClusterLeader(t, nodes, 2*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_ = leader.Propose(ctx, kvstore.Command{Op: kvstore.OpPut, Key: "x", Value: "1"})

	outputDir := t.TempDir()
	disc := discovery.New(peers, 100*time.Millisecond)
	svc := New(disc, outputDir, 200*time.Millisecond)

	if err := svc.RunOnce(ctx); err != nil {
		t.Fatalf("primo RunOnce fallito: %v", err)
	}
	first := readCheckpoint(t, outputDir)
	if len(first.Data) != 1 {
		t.Fatalf("primo checkpoint atteso con 1 chiave, ottenuto %v", first.Data)
	}

	_ = leader.Propose(ctx, kvstore.Command{Op: kvstore.OpPut, Key: "y", Value: "2"})

	if err := svc.RunOnce(ctx); err != nil {
		t.Fatalf("secondo RunOnce fallito: %v", err)
	}
	second := readCheckpoint(t, outputDir)
	if len(second.Data) != 2 {
		t.Fatalf("secondo checkpoint atteso con 2 chiavi, ottenuto %v", second.Data)
	}
	if !second.CreatedAt.After(first.CreatedAt) {
		t.Fatalf("il secondo checkpoint deve avere un timestamp più recente del primo")
	}
}

func TestRunOnce_FailsWhenNoNodeReachable(t *testing.T) {
	disc := discovery.New(map[string]string{"nodeX": "127.0.0.1:1"}, 50*time.Millisecond)
	svc := New(disc, t.TempDir(), 50*time.Millisecond)

	if err := svc.RunOnce(context.Background()); err == nil {
		t.Fatalf("RunOnce dovrebbe fallire se nessun nodo è raggiungibile")
	}
}

// TestWriteCheckpoint_IsAtomic verifica che non resti un file temporaneo
// dopo una scrittura riuscita — stessa garanzia già richiesta a
// raftlog.Storage in Fase 2.
func TestWriteCheckpoint_IsAtomic(t *testing.T) {
	outputDir := t.TempDir()
	svc := New(discovery.New(nil, time.Second), outputDir, time.Second)

	err := svc.writeCheckpoint(Checkpoint{
		Data:      map[string]string{"a": "1"},
		CreatedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("writeCheckpoint fallita: %v", err)
	}

	tmpPath := filepath.Join(outputDir, checkpointFileName+".tmp")
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Fatalf("file temporaneo %q non dovrebbe esistere dopo una scrittura riuscita", tmpPath)
	}
}
