package main

import (
	"flag"
	"fmt"
	"log"
	"net"

	"github.com/Cellu83/sdcc-Progetto-b5-kvstor/internal/config"
	"github.com/Cellu83/sdcc-Progetto-b5-kvstor/internal/discovery"
	"github.com/Cellu83/sdcc-Progetto-b5-kvstor/internal/proxy"
	kvstorepb "github.com/Cellu83/sdcc-Progetto-b5-kvstor/proto/kvstore"
	"google.golang.org/grpc"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	configPath := flag.String("config", "", "percorso del file di configurazione YAML del proxy")
	flag.Parse()

	if *configPath == "" {
		log.Fatal("è obbligatorio specificare -config <percorso file YAML>")
	}

	cfg, err := config.LoadProxyConfig(*configPath)
	if err != nil {
		log.Fatalf("impossibile caricare la configurazione: %v", err)
	}

	fmt.Printf("Client proxy avviato con configurazione:\n")
	fmt.Printf("  Bind address: %s:%d\n", cfg.Proxy.BindAddress, cfg.Proxy.Port)
	fmt.Printf("  Nodi seed:\n")
	for _, p := range cfg.Cluster.Peers {
		fmt.Printf("    - %s @ %s\n", p.ID, p.Address)
	}
	fmt.Printf("  RPC timeout: %s, max retries: %d, retry backoff: %s\n", cfg.RPCTimeout(), cfg.RPC.MaxRetries, cfg.RetryBackoff())
	fmt.Printf("  Circuit breaker: soglia=%d, reset=%s\n", cfg.CircuitBreaker.FailureThreshold, cfg.CircuitBreakerResetTimeout())

	seeds := make(map[string]string, len(cfg.Cluster.Peers))
	for _, p := range cfg.Cluster.Peers {
		seeds[p.ID] = p.Address
	}

	disc := discovery.New(seeds, cfg.RPCTimeout())
	p := proxy.New(proxy.Config{
		Discovery:               disc,
		RPCTimeout:              cfg.RPCTimeout(),
		MaxRetries:              cfg.RPC.MaxRetries,
		RetryBackoff:            cfg.RetryBackoff(),
		CircuitBreakerThreshold: cfg.CircuitBreaker.FailureThreshold,
		CircuitBreakerReset:     cfg.CircuitBreakerResetTimeout(),
	})

	addr := fmt.Sprintf("%s:%d", cfg.Proxy.BindAddress, cfg.Proxy.Port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("impossibile aprire il listener su %s: %v", addr, err)
	}

	grpcServer := grpc.NewServer()
	kvstorepb.RegisterKVStoreServiceServer(grpcServer, &proxy.Server{Proxy: p})

	log.Printf("Client proxy (KVStoreService) in ascolto su %s", addr)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("errore del server gRPC: %v", err)
	}
}
