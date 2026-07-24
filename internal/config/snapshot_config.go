package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// SnapshotServiceParams raccoglie i parametri operativi dello Snapshot &
// backup service.
type SnapshotServiceParams struct {
	OutputDir       string `yaml:"output_dir"`
	IntervalSeconds int    `yaml:"interval_seconds"`
	RPCTimeoutMs    int    `yaml:"rpc_timeout_ms"`
}

// SnapshotConfig è la configurazione completa letta dal file YAML dello
// Snapshot & backup service.
type SnapshotConfig struct {
	Snapshot SnapshotServiceParams `yaml:"snapshot"`
	Cluster  ClusterConfig         `yaml:"cluster"` // stessi Peer usati dai consensus node: i "seed" della Service Discovery
	LogLevel string                `yaml:"log_level"`
}

// Interval restituisce l'intervallo tra un ciclo di snapshot e il
// successivo come time.Duration.
func (c *SnapshotConfig) Interval() time.Duration {
	return time.Duration(c.Snapshot.IntervalSeconds) * time.Second
}

// RPCTimeout restituisce il timeout per le RPC verso i consensus node come
// time.Duration.
func (c *SnapshotConfig) RPCTimeout() time.Duration {
	return time.Duration(c.Snapshot.RPCTimeoutMs) * time.Millisecond
}

// LoadSnapshotConfig legge e valida un file di configurazione YAML dello
// Snapshot & backup service dal path indicato.
func LoadSnapshotConfig(path string) (*SnapshotConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("lettura file di configurazione %q: %w", path, err)
	}

	var cfg SnapshotConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing YAML %q: %w", path, err)
	}

	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("configurazione non valida (%q): %w", path, err)
	}

	return &cfg, nil
}

func (c *SnapshotConfig) validate() error {
	if c.Snapshot.OutputDir == "" {
		return fmt.Errorf("snapshot.output_dir è obbligatorio")
	}
	if c.Snapshot.IntervalSeconds <= 0 {
		return fmt.Errorf("snapshot.interval_seconds deve essere > 0")
	}
	if c.Snapshot.RPCTimeoutMs <= 0 {
		return fmt.Errorf("snapshot.rpc_timeout_ms deve essere > 0")
	}
	if len(c.Cluster.Peers) == 0 {
		return fmt.Errorf("cluster.peers deve contenere almeno un nodo seed")
	}
	return nil
}
