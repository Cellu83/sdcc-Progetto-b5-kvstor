package discovery

import (
	"context"
	"net"
	"testing"
	"time"

	raftpb "github.com/Cellu83/sdcc-Progetto-b5-kvstor/proto/raft"
	"google.golang.org/grpc"
)

// fakeRaftServer implementa solo GetStatus, con una risposta configurabile
// dal test: basta per esercitare Discovery senza dover avviare un vero
// cluster Raft.
type fakeRaftServer struct {
	raftpb.UnimplementedRaftServiceServer
	response *raftpb.GetStatusResponse
}

func (f *fakeRaftServer) GetStatus(ctx context.Context, req *raftpb.GetStatusRequest) (*raftpb.GetStatusResponse, error) {
	return f.response, nil
}

func startFakeNode(t *testing.T, response *raftpb.GetStatusResponse) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen fallita: %v", err)
	}
	grpcServer := grpc.NewServer()
	raftpb.RegisterRaftServiceServer(grpcServer, &fakeRaftServer{response: response})
	go grpcServer.Serve(lis)
	t.Cleanup(grpcServer.Stop)
	return lis.Addr().String()
}

func TestRefresh_UpdatesLeaderAndPeersFromReachableNode(t *testing.T) {
	addr := startFakeNode(t, &raftpb.GetStatusResponse{
		NodeId:   "node1",
		LeaderId: "node2",
		Peers: []*raftpb.PeerInfo{
			{Id: "node1", Address: "127.0.0.1:1"},
			{Id: "node2", Address: "127.0.0.1:2"},
		},
	})

	d := New(map[string]string{"node1": addr}, time.Second)

	if _, ok := d.LeaderAddress(); ok {
		t.Fatalf("prima di Refresh non dovremmo conoscere alcun leader")
	}

	if err := d.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh fallita: %v", err)
	}

	leaderAddr, ok := d.LeaderAddress()
	if !ok {
		t.Fatalf("dopo Refresh dovremmo conoscere il leader")
	}
	if leaderAddr != "127.0.0.1:2" {
		t.Fatalf("indirizzo leader atteso 127.0.0.1:2, ottenuto %q", leaderAddr)
	}

	peers := d.Peers()
	if len(peers) != 2 {
		t.Fatalf("attesi 2 peer nella vista aggiornata, ottenuti %d", len(peers))
	}
}

func TestRefresh_FailsWhenNoSeedIsReachable(t *testing.T) {
	d := New(map[string]string{"nodeX": "127.0.0.1:1"}, 50*time.Millisecond)

	if err := d.Refresh(context.Background()); err == nil {
		t.Fatalf("Refresh dovrebbe fallire se nessun seed è raggiungibile")
	}
}

func TestRefresh_SkipsUnreachableSeedAndUsesReachableOne(t *testing.T) {
	addr := startFakeNode(t, &raftpb.GetStatusResponse{
		NodeId:   "node2",
		LeaderId: "node2",
		Peers: []*raftpb.PeerInfo{
			{Id: "node2", Address: "127.0.0.1:2"},
		},
	})

	d := New(map[string]string{
		"nodeDown": "127.0.0.1:1", // nessuno in ascolto qui
		"node2":    addr,
	}, 100*time.Millisecond)

	if err := d.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh dovrebbe riuscire appoggiandosi al seed raggiungibile: %v", err)
	}
	if _, ok := d.LeaderAddress(); !ok {
		t.Fatalf("dopo Refresh dovremmo conoscere il leader nonostante un seed fosse giù")
	}
}
