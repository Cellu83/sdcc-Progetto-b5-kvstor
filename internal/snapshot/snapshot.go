// Package snapshot implementa lo Snapshot & backup service: un servizio
// asincrono e stateless che interroga periodicamente i consensus node per
// scaricare lo stato corrente della state machine chiave-valore, e lo
// scrive in un file di checkpoint stabile sul proprio disco. I consensus
// node non vengono in alcun modo modificati da questo processo — il
// servizio si limita a leggere e a compattare quanto legge in un'unica
// istantanea (invece di conservare l'intera storia delle scritture,
// il checkpoint conserva solo lo stato risultante).
package snapshot

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/Cellu83/sdcc-Progetto-b5-kvstor/internal/discovery"
	raftpb "github.com/Cellu83/sdcc-Progetto-b5-kvstor/proto/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const checkpointFileName = "checkpoint.json"

// Checkpoint è il contenuto di un file di checkpoint: lo stato completo
// della state machine chiave-valore in un dato istante, insieme ai
// metadati che indicano fino a quale entry di log quello stato riflette.
type Checkpoint struct {
	LastIncludedIndex uint64            `json:"last_included_index"`
	LastIncludedTerm  uint64            `json:"last_included_term"`
	Data              map[string]string `json:"data"`
	CreatedAt         time.Time         `json:"created_at"`
	SourceNode        string            `json:"source_node"`
}

// Service è lo Snapshot & backup service.
type Service struct {
	discovery  *discovery.Discovery
	outputDir  string
	rpcTimeout time.Duration
}

// New costruisce un Service pronto all'uso.
func New(disc *discovery.Discovery, outputDir string, rpcTimeout time.Duration) *Service {
	return &Service{
		discovery:  disc,
		outputDir:  outputDir,
		rpcTimeout: rpcTimeout,
	}
}

// Run esegue un ciclo di snapshot subito, poi ripete a intervalli regolari
// (interval), finché ctx non viene cancellato.
func (s *Service) Run(ctx context.Context, interval time.Duration) {
	s.runOnceAndLog(ctx)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.runOnceAndLog(ctx)
		case <-ctx.Done():
			return
		}
	}
}

func (s *Service) runOnceAndLog(ctx context.Context) {
	if err := s.RunOnce(ctx); err != nil {
		log.Printf("snapshot: ciclo fallito: %v", err)
		return
	}
	log.Printf("snapshot: checkpoint scritto in %s", filepath.Join(s.outputDir, checkpointFileName))
}

// RunOnce esegue un singolo ciclo: aggiorna la vista del cluster (Service
// Discovery), sceglie un nodo raggiungibile (preferendo il Leader, se
// conosciuto, altrimenti un peer qualsiasi), scarica il suo stato via
// GetSnapshot, e scrive il checkpoint su disco.
func (s *Service) RunOnce(ctx context.Context) error {
	// Best-effort: se il refresh fallisce ma abbiamo già una vista
	// precedente valida, proviamo comunque con quella.
	_ = s.discovery.Refresh(ctx)

	var lastErr error

	if addr, ok := s.discovery.LeaderAddress(); ok {
		err := s.fetchAndWrite(ctx, addr)
		if err == nil {
			return nil
		}
		lastErr = err
	}

	for _, addr := range s.discovery.Peers() {
		err := s.fetchAndWrite(ctx, addr)
		if err == nil {
			return nil
		}
		lastErr = err
	}

	return fmt.Errorf("nessun nodo raggiungibile per lo snapshot: %w", lastErr)
}

func (s *Service) fetchAndWrite(ctx context.Context, addr string) error {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()

	callCtx, cancel := context.WithTimeout(ctx, s.rpcTimeout)
	defer cancel()

	resp, err := raftpb.NewRaftServiceClient(conn).GetSnapshot(callCtx, &raftpb.GetSnapshotRequest{})
	if err != nil {
		return err
	}

	data := make(map[string]string, len(resp.GetData()))
	for _, kv := range resp.GetData() {
		data[kv.GetKey()] = kv.GetValue()
	}

	checkpoint := Checkpoint{
		LastIncludedIndex: resp.GetLastIncludedIndex(),
		LastIncludedTerm:  resp.GetLastIncludedTerm(),
		Data:              data,
		CreatedAt:         time.Now().UTC(),
		SourceNode:        addr,
	}

	return s.writeCheckpoint(checkpoint)
}

// writeCheckpoint scrive il checkpoint su disco in modo atomico: prima su
// un file temporaneo, poi con una rename sopra il file definitivo — lo
// stesso schema già usato da raftlog.Storage, per lo stesso motivo: se il
// processo crashasse a metà scrittura, il checkpoint precedente resta
// intatto invece di corrompersi.
func (s *Service) writeCheckpoint(c Checkpoint) error {
	if err := os.MkdirAll(s.outputDir, 0o755); err != nil {
		return fmt.Errorf("creazione output dir %q: %w", s.outputDir, err)
	}

	payload, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return fmt.Errorf("serializzazione checkpoint: %w", err)
	}

	finalPath := filepath.Join(s.outputDir, checkpointFileName)
	tmpPath := finalPath + ".tmp"

	if err := os.WriteFile(tmpPath, payload, 0o644); err != nil {
		return fmt.Errorf("scrittura file temporaneo %q: %w", tmpPath, err)
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		return fmt.Errorf("rename %q -> %q: %w", tmpPath, finalPath, err)
	}
	return nil
}
