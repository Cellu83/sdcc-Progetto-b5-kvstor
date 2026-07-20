// Package raft conterrà, a partire dalla Fase 4, il vero algoritmo di
// consenso (elezione del leader, replicazione del log). Per ora contiene
// solo il "plumbing" gRPC: un server che implementa l'interfaccia generata
// da raft.proto, con risposte finte, per verificare che due nodi possano
// davvero parlarsi in rete prima di scrivere la logica reale.
package raft

import (
	"context"
	"log"

	raftpb "github.com/Cellu83/sdcc-Progetto-b5-kvstor/proto/raft"
)

// Server implementa raftpb.RaftServiceServer. Le risposte sono
// deliberatamente finte (stub): confermano solo che l'RPC è arrivato ed è
// stato deserializzato correttamente. La logica vera arriva in Fase 4
// (RequestVote) e Fase 5 (AppendEntries).
type Server struct {
	raftpb.UnimplementedRaftServiceServer

	NodeID string
}

// RequestVote logga la richiesta ricevuta e risponde sempre concedendo il
// voto: è uno stub, non applica ancora nessuna delle regole di sicurezza
// di Raft.
func (s *Server) RequestVote(ctx context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	log.Printf("[%s] RequestVote ricevuta: term=%d candidateId=%s lastLogIndex=%d lastLogTerm=%d",
		s.NodeID, req.GetTerm(), req.GetCandidateId(), req.GetLastLogIndex(), req.GetLastLogTerm())

	return &raftpb.RequestVoteResponse{
		Term:        req.GetTerm(),
		VoteGranted: true,
	}, nil
}

// AppendEntries logga la richiesta ricevuta e risponde sempre con successo:
// è uno stub, non scrive ancora nulla nel log persistito.
func (s *Server) AppendEntries(ctx context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	log.Printf("[%s] AppendEntries ricevuta: term=%d leaderId=%s prevLogIndex=%d entries=%d leaderCommit=%d",
		s.NodeID, req.GetTerm(), req.GetLeaderId(), req.GetPrevLogIndex(), len(req.GetEntries()), req.GetLeaderCommit())

	return &raftpb.AppendEntriesResponse{
		Term:    req.GetTerm(),
		Success: true,
	}, nil
}
