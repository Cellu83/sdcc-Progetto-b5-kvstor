package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os/signal"
	"syscall"

	"github.com/Cellu83/sdcc-Progetto-b5-kvstor/internal/config"
	"github.com/Cellu83/sdcc-Progetto-b5-kvstor/internal/discovery"
	"github.com/Cellu83/sdcc-Progetto-b5-kvstor/internal/snapshot"
)

func main() {
	configPath := flag.String("config", "", "percorso del file di configurazione YAML dello snapshot service")
	flag.Parse()

	if *configPath == "" {
		log.Fatal("è obbligatorio specificare -config <percorso file YAML>")
	}

	cfg, err := config.LoadSnapshotConfig(*configPath)
	if err != nil {
		log.Fatalf("impossibile caricare la configurazione: %v", err)
	}

	fmt.Printf("Snapshot & backup service avviato con configurazione:\n")
	fmt.Printf("  Output dir: %s\n", cfg.Snapshot.OutputDir)
	fmt.Printf("  Intervallo: %s\n", cfg.Interval())
	fmt.Printf("  RPC timeout: %s\n", cfg.RPCTimeout())
	fmt.Printf("  Nodi seed:\n")
	for _, p := range cfg.Cluster.Peers {
		fmt.Printf("    - %s @ %s\n", p.ID, p.Address)
	}

	seeds := make(map[string]string, len(cfg.Cluster.Peers))
	for _, p := range cfg.Cluster.Peers {
		seeds[p.ID] = p.Address
	}

	disc := discovery.New(seeds, cfg.RPCTimeout())
	svc := snapshot.New(disc, cfg.Snapshot.OutputDir, cfg.RPCTimeout())

	// Il ciclo di Run() gira per sempre finché ctx non viene cancellato:
	// colleghiamo la cancellazione a SIGINT/SIGTERM per uno spegnimento
	// pulito (Ctrl+C) invece che affidarci solo a un kill del processo.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Printf("Snapshot & backup service in esecuzione (intervallo %s)", cfg.Interval())
	svc.Run(ctx, cfg.Interval())
	log.Printf("Snapshot & backup service terminato")
}
