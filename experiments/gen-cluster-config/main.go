// gen-cluster-config genera i file di configurazione YAML per un cluster di
// N consensus node più un proxy, per gli esperimenti di scalabilità della
// Fase 10. Riusa direttamente le struct di internal/config, così i file
// prodotti sono garantiti nello stesso formato che config.Load si aspetta
// — non c'è modo di introdurre un errore di battitura scrivendo YAML a mano.
//
// Tutti i parametri Raft (timeout di elezione, heartbeat, rpc timeout)
// restano fissi tra una dimensione di cluster e l'altra: l'unica variabile
// che cambia è N, il numero di nodi — è quello che rende l'esperimento di
// scalabilità un confronto valido (una sola variabile indipendente).
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/Cellu83/sdcc-Progetto-b5-kvstor/internal/config"
	"gopkg.in/yaml.v3"
)

func main() {
	n := flag.Int("n", 3, "numero di consensus node da generare")
	basePort := flag.Int("base-port", 52000, "porta base: il proxy usa base-port, i nodi usano base-port+1..+n")
	outDir := flag.String("out-dir", "", "cartella di output per i file YAML (obbligatoria)")
	dataDirBase := flag.String("data-dir", "", "cartella base per i data_dir dei nodi (default: ./data/scale/n<N>)")
	flag.Parse()

	if *n < 1 {
		log.Fatal("-n deve essere >= 1")
	}
	if *outDir == "" {
		log.Fatal("è obbligatorio specificare -out-dir")
	}

	dataBase := *dataDirBase
	if dataBase == "" {
		dataBase = fmt.Sprintf("./data/scale/n%d", *n)
	}

	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		log.Fatalf("creazione cartella di output %q: %v", *outDir, err)
	}

	// La vista del cluster (Peers) è identica in ogni file: ogni nodo, e il
	// proxy, devono conoscere tutti gli N nodi fin dal bootstrap.
	peers := make([]config.Peer, *n)
	for i := 1; i <= *n; i++ {
		id := fmt.Sprintf("node%d", i)
		peers[i-1] = config.Peer{ID: id, Address: fmt.Sprintf("localhost:%d", *basePort+i)}
	}

	generatedPaths := make([]string, 0, *n+1)

	for i := 1; i <= *n; i++ {
		id := fmt.Sprintf("node%d", i)
		cfg := config.Config{
			Node: config.NodeConfig{
				ID:          id,
				BindAddress: "0.0.0.0",
				RaftPort:    *basePort + i,
				DataDir:     filepath.Join(dataBase, id),
			},
			Cluster: config.ClusterConfig{Peers: peers},
			Raft: config.RaftConfig{
				ElectionTimeoutMinMs: 150,
				ElectionTimeoutMaxMs: 300,
				HeartbeatIntervalMs:  50,
				RPCTimeoutMs:         50,
			},
			LogLevel: "info",
		}
		path := filepath.Join(*outDir, id+".yaml")
		writeYAML(path, cfg)
		generatedPaths = append(generatedPaths, path)
	}

	proxyCfg := config.ProxyConfig{
		Proxy: config.ProxyNodeConfig{
			BindAddress: "0.0.0.0",
			Port:        *basePort,
		},
		Cluster: config.ClusterConfig{Peers: peers},
		RPC: config.ProxyRPCConfig{
			TimeoutMs:      300,
			MaxRetries:     6,
			RetryBackoffMs: 50,
		},
		CircuitBreaker: config.CircuitBreakerConfig{
			FailureThreshold: 2,
			ResetTimeoutMs:   500,
		},
		LogLevel: "info",
	}
	proxyPath := filepath.Join(*outDir, "proxy.yaml")
	writeYAML(proxyPath, proxyCfg)

	// Autocontrollo: rileggiamo ogni file appena scritto con la stessa
	// funzione (config.Load) che userà davvero cmd/consensus-node — se il
	// generatore avesse prodotto qualcosa di non valido, lo scopriamo qui,
	// non a metà di un esperimento.
	for _, p := range generatedPaths {
		if _, err := config.Load(p); err != nil {
			log.Fatalf("autocontrollo fallito su %s: %v", p, err)
		}
	}
	if _, err := config.LoadProxyConfig(proxyPath); err != nil {
		log.Fatalf("autocontrollo fallito su %s: %v", proxyPath, err)
	}

	fmt.Printf("Generati e validati %d file di configurazione nodo + 1 proxy in %s\n", *n, *outDir)
	fmt.Printf("Proxy in ascolto su porta %d, nodi su %d..%d\n", *basePort, *basePort+1, *basePort+*n)
}

func writeYAML(path string, v any) {
	data, err := yaml.Marshal(v)
	if err != nil {
		log.Fatalf("serializzazione YAML per %s: %v", path, err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		log.Fatalf("scrittura %s: %v", path, err)
	}
}
