package raft

import (
	"context"
	"fmt"
	"sync"

	raftpb "github.com/Cellu83/sdcc-Progetto-b5-kvstor/proto/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client gestisce le connessioni gRPC verso gli altri consensus node e
// traduce le chiamate RequestVote/AppendEntries dal formato "puro Go" di
// Node (RequestVoteArgs, ecc.) al formato protobuf generato da raft.proto,
// e viceversa per le risposte. Tenere questa traduzione qui, separata da
// node.go, permette a Node di non dipendere direttamente da gRPC.
type Client struct {
	mu    sync.Mutex
	conns map[string]*grpc.ClientConn // indirizzo -> connessione riusata
}

// NewClient crea un Client senza connessioni ancora aperte: verranno
// stabilite pigramente (lazy), alla prima chiamata verso ogni indirizzo.
func NewClient() *Client {
	return &Client{conns: make(map[string]*grpc.ClientConn)}
}

// connFor restituisce una connessione gRPC verso addr, riusando quella già
// aperta se esiste (evita di ristabilire una connessione TCP a ogni RPC).
func (c *Client) connFor(addr string) (*grpc.ClientConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if conn, ok := c.conns[addr]; ok {
		return conn, nil
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("connessione a %s: %w", addr, err)
	}
	c.conns[addr] = conn
	return conn, nil
}

// RequestVote chiama la RPC RequestVote sul nodo all'indirizzo addr.
func (c *Client) RequestVote(ctx context.Context, addr string, args RequestVoteArgs) (RequestVoteResult, error) {
	conn, err := c.connFor(addr)
	if err != nil {
		return RequestVoteResult{}, err
	}

	resp, err := raftpb.NewRaftServiceClient(conn).RequestVote(ctx, &raftpb.RequestVoteRequest{
		Term:         args.Term,
		CandidateId:  args.CandidateID,
		LastLogIndex: args.LastLogIndex,
		LastLogTerm:  args.LastLogTerm,
	})
	if err != nil {
		return RequestVoteResult{}, err
	}

	return RequestVoteResult{
		Term:        resp.GetTerm(),
		VoteGranted: resp.GetVoteGranted(),
	}, nil
}

// AppendEntries chiama la RPC AppendEntries sul nodo all'indirizzo addr.
func (c *Client) AppendEntries(ctx context.Context, addr string, args AppendEntriesArgs) (AppendEntriesResult, error) {
	conn, err := c.connFor(addr)
	if err != nil {
		return AppendEntriesResult{}, err
	}

	resp, err := raftpb.NewRaftServiceClient(conn).AppendEntries(ctx, &raftpb.AppendEntriesRequest{
		Term:         args.Term,
		LeaderId:     args.LeaderID,
		PrevLogIndex: args.PrevLogIndex,
		PrevLogTerm:  args.PrevLogTerm,
		Entries:      toProtoEntries(args.Entries),
		LeaderCommit: args.LeaderCommit,
	})
	if err != nil {
		return AppendEntriesResult{}, err
	}

	return AppendEntriesResult{
		Term:    resp.GetTerm(),
		Success: resp.GetSuccess(),
	}, nil
}
