package raft

import (
	"context"

	raftpb "github.com/Cellu83/sdcc-Progetto-b5-kvstor/proto/raft"
)

// Server è l'adattatore gRPC verso Node: traduce i messaggi protobuf in
// arrivo negli argomenti "puro Go" definiti in node.go, chiama la vera
// logica di consenso, e traduce la risposta di nuovo in protobuf. Non
// contiene nessuna regola di Raft: quella vive tutta in Node.
type Server struct {
	raftpb.UnimplementedRaftServiceServer

	Node *Node
}

// RequestVote delega la decisione a Node.HandleRequestVote.
func (s *Server) RequestVote(ctx context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	result := s.Node.HandleRequestVote(RequestVoteArgs{
		Term:         req.GetTerm(),
		CandidateID:  req.GetCandidateId(),
		LastLogIndex: req.GetLastLogIndex(),
		LastLogTerm:  req.GetLastLogTerm(),
	})

	return &raftpb.RequestVoteResponse{
		Term:        result.Term,
		VoteGranted: result.VoteGranted,
	}, nil
}

// AppendEntries delega la decisione a Node.HandleAppendEntries, che ora
// (Fase 5) applica davvero le entry ricevute e avanza il commitIndex.
func (s *Server) AppendEntries(ctx context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	result := s.Node.HandleAppendEntries(AppendEntriesArgs{
		Term:         req.GetTerm(),
		LeaderID:     req.GetLeaderId(),
		PrevLogIndex: req.GetPrevLogIndex(),
		PrevLogTerm:  req.GetPrevLogTerm(),
		Entries:      fromProtoEntries(req.GetEntries()),
		LeaderCommit: req.GetLeaderCommit(),
	})

	return &raftpb.AppendEntriesResponse{
		Term:    result.Term,
		Success: result.Success,
	}, nil
}

// GetStatus espone Node.ClusterStatus via gRPC: è la RPC di sola lettura
// che alimenta il pattern Service Discovery lato client (Fase 6).
func (s *Server) GetStatus(ctx context.Context, req *raftpb.GetStatusRequest) (*raftpb.GetStatusResponse, error) {
	leaderID, peers := s.Node.ClusterStatus()

	peerInfos := make([]*raftpb.PeerInfo, 0, len(peers))
	for id, addr := range peers {
		peerInfos = append(peerInfos, &raftpb.PeerInfo{Id: id, Address: addr})
	}

	return &raftpb.GetStatusResponse{
		NodeId:   s.Node.ID(),
		LeaderId: leaderID,
		Peers:    peerInfos,
	}, nil
}
