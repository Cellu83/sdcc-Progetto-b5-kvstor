package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/federicocola/sdcc-b5-kvstore/internal/config"
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
}
