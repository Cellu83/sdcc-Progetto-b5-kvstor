package raft

import (
	"context"
	"fmt"
	"sync"
	"time"

	raftpb "github.com/Cellu83/sdcc-Progetto-b5-kvstor/proto/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
)

// reconnectBackoff sostituisce il backoff di riconnessione di default di
// gRPC (BaseDelay 1s, pensato per reti geografiche reali) con uno molto
// più breve. Senza questo, un nodo che cade e rientra in pochi centinaia
// di millisecondi (es. un restart di container Docker) non riceve
// comunque un heartbeat in tempo utile: i peer restano fermi al loro
// timer di backoff da almeno un secondo prima di riprovare a
// riconnettersi, molto più lungo del timeout di elezione (150-300ms) —
// così il nodo appena rientrato scade sempre prima e si candida sempre,
// anche quando basterebbe riconoscere il Leader già attivo.
var reconnectBackoff = backoff.Config{
	BaseDelay:  50 * time.Millisecond,
	Multiplier: 1.6,
	Jitter:     0.2,
	MaxDelay:   1 * time.Second,
}

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

	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithConnectParams(grpc.ConnectParams{Backoff: reconnectBackoff}),
	)
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
