package proxy

import (
	"context"

	kvstorepb "github.com/Cellu83/sdcc-Progetto-b5-kvstor/proto/kvstore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server è l'adattatore gRPC del Client proxy service: espone lo stesso
// contratto KVStoreService già usato dai consensus node (proto/kvstore),
// così un client esterno lo chiama esattamente allo stesso modo — non deve
// sapere se sta parlando col proxy o direttamente con un nodo.
type Server struct {
	kvstorepb.UnimplementedKVStoreServiceServer

	Proxy *Proxy
}

func (s *Server) Get(ctx context.Context, req *kvstorepb.GetRequest) (*kvstorepb.GetResponse, error) {
	value, found, err := s.Proxy.Get(ctx, req.GetKey())
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	return &kvstorepb.GetResponse{Value: value, Found: found}, nil
}

func (s *Server) Put(ctx context.Context, req *kvstorepb.PutRequest) (*kvstorepb.PutResponse, error) {
	if err := s.Proxy.Put(ctx, req.GetKey(), req.GetValue()); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	return &kvstorepb.PutResponse{Success: true}, nil
}

func (s *Server) Delete(ctx context.Context, req *kvstorepb.DeleteRequest) (*kvstorepb.DeleteResponse, error) {
	if err := s.Proxy.Delete(ctx, req.GetKey()); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	return &kvstorepb.DeleteResponse{Success: true}, nil
}
