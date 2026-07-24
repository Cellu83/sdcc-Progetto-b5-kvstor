package raft

import (
	"context"
	"errors"

	"github.com/Cellu83/sdcc-Progetto-b5-kvstor/internal/kvstore"
	kvstorepb "github.com/Cellu83/sdcc-Progetto-b5-kvstor/proto/kvstore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// KVServer è l'adattatore gRPC per KVStoreService: l'API applicativa che
// un client (e, dalla Fase 6, il Client proxy service) usa per leggere e
// scrivere. Solo il nodo Leader accetta richieste — sia le scritture, per
// ovvie ragioni di correttezza, sia (in questa implementazione) anche le
// letture: rispondere con dati letti da un follower potrebbe restituire un
// valore non ancora replicato, violando la consistenza forte richiesta
// dalla spec del progetto.
type KVServer struct {
	kvstorepb.UnimplementedKVStoreServiceServer

	Node *Node
}

// Get legge il valore corrente di una chiave. Rifiutata se il nodo non è
// Leader.
func (s *KVServer) Get(ctx context.Context, req *kvstorepb.GetRequest) (*kvstorepb.GetResponse, error) {
	if s.Node.State() != Leader {
		return nil, status.Error(codes.FailedPrecondition, ErrNotLeader.Error())
	}
	value, found := s.Node.Get(req.GetKey())
	return &kvstorepb.GetResponse{Value: value, Found: found}, nil
}

// Put propone una scrittura e attende che venga committata dalla
// maggioranza del cluster prima di rispondere.
func (s *KVServer) Put(ctx context.Context, req *kvstorepb.PutRequest) (*kvstorepb.PutResponse, error) {
	cmd := kvstore.Command{Op: kvstore.OpPut, Key: req.GetKey(), Value: req.GetValue()}
	if err := s.Node.Propose(ctx, cmd); err != nil {
		return nil, translateProposeError(err)
	}
	return &kvstorepb.PutResponse{Success: true}, nil
}

// Delete propone una cancellazione e attende che venga committata dalla
// maggioranza del cluster prima di rispondere.
func (s *KVServer) Delete(ctx context.Context, req *kvstorepb.DeleteRequest) (*kvstorepb.DeleteResponse, error) {
	cmd := kvstore.Command{Op: kvstore.OpDelete, Key: req.GetKey()}
	if err := s.Node.Propose(ctx, cmd); err != nil {
		return nil, translateProposeError(err)
	}
	return &kvstorepb.DeleteResponse{Success: true}, nil
}

// translateProposeError converte gli errori interni di Node.Propose in
// codici di stato gRPC standard, così un client (o il futuro Proxy) può
// distinguere "non sono il leader, riprova altrove" da un timeout o da un
// errore generico.
func translateProposeError(err error) error {
	if errors.Is(err, ErrNotLeader) {
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.DeadlineExceeded, "timeout in attesa del commit della scrittura")
	}
	if errors.Is(err, context.Canceled) {
		return status.Error(codes.Canceled, "richiesta annullata")
	}
	return status.Error(codes.Internal, err.Error())
}
