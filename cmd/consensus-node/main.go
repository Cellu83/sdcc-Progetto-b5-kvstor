package main

import (
	"flag"
	"fmt"
	"log"
	"net"

	"github.com/Cellu83/sdcc-Progetto-b5-kvstor/internal/config"
	"github.com/Cellu83/sdcc-Progetto-b5-kvstor/internal/raft"
	raftpb "github.com/Cellu83/sdcc-Progetto-b5-kvstor/proto/raft"
	"google.golang.org/grpc"
)

func main() {
	configPath := flag.String("config", "", "percorso del file di configurazione YAML del nodo")
	flag.Parse()

	if *configPath == "" {
		log.Fatal("è obbligatorio specificare -config <percorso file YAML>")
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("impossibile caricare la configurazione: %v", err)
	}

	fmt.Printf("Consensus node avviato con configurazione:\n")
	fmt.Printf("  Node ID:        %s\n", cfg.Node.ID)
	fmt.Printf("  Bind address:   %s:%d\n", cfg.Node.BindAddress, cfg.Node.RaftPort)
	fmt.Printf("  Data dir:       %s\n", cfg.Node.DataDir)
	fmt.Printf("  Peers noti:\n")
	for _, p := range cfg.Cluster.Peers {
		fmt.Printf("    - %s @ %s\n", p.ID, p.Address)
	}
	minTimeout, maxTimeout := cfg.ElectionTimeoutRange()
	fmt.Printf("  Election timeout: [%s, %s]\n", minTimeout, maxTimeout)
	fmt.Printf("  Heartbeat interval: %s\n", cfg.HeartbeatInterval())
	fmt.Printf("  Snapshot interval: %ds\n", cfg.Snapshot.IntervalSeconds)
	fmt.Printf("  Log level: %s\n", cfg.LogLevel)

	addr := fmt.Sprintf("%s:%d", cfg.Node.BindAddress, cfg.Node.RaftPort)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("impossibile aprire il listener su %s: %v", addr, err)
	}

	grpcServer := grpc.NewServer()
	raftpb.RegisterRaftServiceServer(grpcServer, &raft.Server{NodeID: cfg.Node.ID})

	log.Printf("[%s] RaftService in ascolto su %s", cfg.Node.ID, addr)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("errore del server gRPC: %v", err)
	}
}
