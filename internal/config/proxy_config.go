package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// ProxyNodeConfig raccoglie i parametri di identità e rete del Client
// proxy service.
type ProxyNodeConfig struct {
	BindAddress string `yaml:"bind_address"`
	Port        int    `yaml:"port"`
}

// ProxyRPCConfig raccoglie i parametri delle chiamate del proxy verso i
// consensus node.
type ProxyRPCConfig struct {
	TimeoutMs      int `yaml:"timeout_ms"`
	MaxRetries     int `yaml:"max_retries"`
	RetryBackoffMs int `yaml:"retry_backoff_ms"`
}

// CircuitBreakerConfig raccoglie i parametri del pattern Circuit Breaker.
type CircuitBreakerConfig struct {
	FailureThreshold int `yaml:"failure_threshold"`
	ResetTimeoutMs   int `yaml:"reset_timeout_ms"`
}

// ProxyConfig è la configurazione completa letta dal file YAML del proxy.
type ProxyConfig struct {
	Proxy          ProxyNodeConfig      `yaml:"proxy"`
	Cluster        ClusterConfig        `yaml:"cluster"` // stessi Peer usati dai consensus node: sono i "seed" della Service Discovery
	RPC            ProxyRPCConfig       `yaml:"rpc"`
	CircuitBreaker CircuitBreakerConfig `yaml:"circuit_breaker"`
	LogLevel       string               `yaml:"log_level"`
}

// RPCTimeout restituisce il timeout per RPC verso i consensus node come
// time.Duration.
func (c *ProxyConfig) RPCTimeout() time.Duration {
	return time.Duration(c.RPC.TimeoutMs) * time.Millisecond
}

// RetryBackoff restituisce la pausa tra un tentativo e il successivo come
// time.Duration.
func (c *ProxyConfig) RetryBackoff() time.Duration {
	return time.Duration(c.RPC.RetryBackoffMs) * time.Millisecond
}

// CircuitBreakerResetTimeout restituisce il periodo di raffreddamento del
// circuit breaker come time.Duration.
func (c *ProxyConfig) CircuitBreakerResetTimeout() time.Duration {
	return time.Duration(c.CircuitBreaker.ResetTimeoutMs) * time.Millisecond
}

// LoadProxyConfig legge e valida un file di configurazione YAML del proxy
// dal path indicato.
func LoadProxyConfig(path string) (*ProxyConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("lettura file di configurazione %q: %w", path, err)
	}

	var cfg ProxyConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing YAML %q: %w", path, err)
	}

	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("configurazione non valida (%q): %w", path, err)
	}

	return &cfg, nil
}

func (c *ProxyConfig) validate() error {
	if c.Proxy.Port == 0 {
		return fmt.Errorf("proxy.port è obbligatorio")
	}
	if len(c.Cluster.Peers) == 0 {
		return fmt.Errorf("cluster.peers deve contenere almeno un nodo seed")
	}
	if c.RPC.TimeoutMs <= 0 {
		return fmt.Errorf("rpc.timeout_ms deve essere > 0")
	}
	if c.RPC.MaxRetries <= 0 {
		return fmt.Errorf("rpc.max_retries deve essere > 0")
	}
	if c.RPC.RetryBackoffMs <= 0 {
		return fmt.Errorf("rpc.retry_backoff_ms deve essere > 0")
	}
	if c.CircuitBreaker.FailureThreshold <= 0 {
		return fmt.Errorf("circuit_breaker.failure_threshold deve essere > 0")
	}
	if c.CircuitBreaker.ResetTimeoutMs <= 0 {
		return fmt.Errorf("circuit_breaker.reset_timeout_ms deve essere > 0")
	}
	return nil
}
