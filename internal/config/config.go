// Package config carica la configurazione di un nodo da file YAML.
// Nessun parametro operativo del sistema (ID nodo, peer, porte, timeout, ...)
// deve essere hard-coded nel codice: tutto passa da qui.
package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Peer identifica un nodo del cluster di consenso raggiungibile via gRPC.
type Peer struct {
	ID      string `yaml:"id"`
	Address string `yaml:"address"`
}

// NodeConfig raccoglie i parametri di identità e rete di un singolo nodo.
type NodeConfig struct {
	ID          string `yaml:"id"`
	BindAddress string `yaml:"bind_address"`
	RaftPort    int    `yaml:"raft_port"`
	DataDir     string `yaml:"data_dir"`
}

// ClusterConfig descrive la vista statica iniziale del cluster (bootstrap).
type ClusterConfig struct {
	Peers []Peer `yaml:"peers"`
}

// RaftConfig raccoglie i parametri temporali dell'algoritmo di consenso.
type RaftConfig struct {
	ElectionTimeoutMinMs int `yaml:"election_timeout_min_ms"`
	ElectionTimeoutMaxMs int `yaml:"election_timeout_max_ms"`
	HeartbeatIntervalMs  int `yaml:"heartbeat_interval_ms"`
	RPCTimeoutMs         int `yaml:"rpc_timeout_ms"`
}

// SnapshotConfig raccoglie i parametri dello Snapshot & backup service.
type SnapshotConfig struct {
	IntervalSeconds int `yaml:"interval_seconds"`
}

// Config è la configurazione completa letta dal file YAML di un nodo.
type Config struct {
	Node     NodeConfig     `yaml:"node"`
	Cluster  ClusterConfig  `yaml:"cluster"`
	Raft     RaftConfig     `yaml:"raft"`
	Snapshot SnapshotConfig `yaml:"snapshot"`
	LogLevel string         `yaml:"log_level"`
}

// ElectionTimeoutRange restituisce i timeout di elezione come time.Duration.
func (c *Config) ElectionTimeoutRange() (time.Duration, time.Duration) {
	return time.Duration(c.Raft.ElectionTimeoutMinMs) * time.Millisecond,
		time.Duration(c.Raft.ElectionTimeoutMaxMs) * time.Millisecond
}

// HeartbeatInterval restituisce l'intervallo di heartbeat come time.Duration.
func (c *Config) HeartbeatInterval() time.Duration {
	return time.Duration(c.Raft.HeartbeatIntervalMs) * time.Millisecond
}

// RPCTimeout restituisce il timeout massimo di attesa per una RPC verso un
// peer come time.Duration.
func (c *Config) RPCTimeout() time.Duration {
	return time.Duration(c.Raft.RPCTimeoutMs) * time.Millisecond
}

// Load legge e valida un file di configurazione YAML dal path indicato.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("lettura file di configurazione %q: %w", path, err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing YAML %q: %w", path, err)
	}

	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("configurazione non valida (%q): %w", path, err)
	}

	return &cfg, nil
}

func (c *Config) validate() error {
	if c.Node.ID == "" {
		return fmt.Errorf("node.id è obbligatorio")
	}
	if c.Node.RaftPort == 0 {
		return fmt.Errorf("node.raft_port è obbligatorio")
	}
	if c.Node.DataDir == "" {
		return fmt.Errorf("node.data_dir è obbligatorio")
	}
	if c.Raft.ElectionTimeoutMinMs <= 0 || c.Raft.ElectionTimeoutMaxMs <= 0 {
		return fmt.Errorf("raft.election_timeout_min_ms e raft.election_timeout_max_ms devono essere > 0")
	}
	if c.Raft.ElectionTimeoutMinMs >= c.Raft.ElectionTimeoutMaxMs {
		return fmt.Errorf("raft.election_timeout_min_ms deve essere < raft.election_timeout_max_ms")
	}
	if c.Raft.HeartbeatIntervalMs <= 0 {
		return fmt.Errorf("raft.heartbeat_interval_ms deve essere > 0")
	}
	if c.Raft.HeartbeatIntervalMs >= c.Raft.ElectionTimeoutMinMs {
		return fmt.Errorf("raft.heartbeat_interval_ms deve essere minore del timeout di elezione minimo")
	}
	if c.Raft.RPCTimeoutMs <= 0 {
		return fmt.Errorf("raft.rpc_timeout_ms deve essere > 0")
	}
	if c.Raft.RPCTimeoutMs >= c.Raft.ElectionTimeoutMinMs {
		return fmt.Errorf("raft.rpc_timeout_ms deve essere minore del timeout di elezione minimo")
	}
	return nil
}
