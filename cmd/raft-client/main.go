// raft-client è un piccolo strumento manuale per interrogare via gRPC un
// consensus node già in esecuzione, senza dover scrivere codice ad hoc.
// Utile per verificare a mano la tubatura di rete (Fase 3) e, più avanti,
// per fare debug interattivo durante lo sviluppo di elezione e replica.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	raftpb "github.com/Cellu83/sdcc-Progetto-b5-kvstor/proto/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	addr := flag.String("addr", "localhost:50051", "indirizzo del consensus node da contattare")
	rpc := flag.String("rpc", "requestvote", "RPC da chiamare: requestvote | appendentries")
	candidateID := flag.String("candidate-id", "debug-client", "candidateId da inviare (RequestVote)")
	leaderID := flag.String("leader-id", "debug-client", "leaderId da inviare (AppendEntries)")
	term := flag.Uint64("term", 1, "term da inviare")
	flag.Parse()

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("impossibile connettersi a %s: %v", *addr, err)
	}
	defer conn.Close()

	client := raftpb.NewRaftServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	switch *rpc {
	case "requestvote":
		resp, err := client.RequestVote(ctx, &raftpb.RequestVoteRequest{
			Term:        *term,
			CandidateId: *candidateID,
		})
		if err != nil {
			log.Fatalf("RequestVote fallita: %v", err)
		}
		fmt.Printf("RequestVoteResponse: term=%d voteGranted=%t\n", resp.GetTerm(), resp.GetVoteGranted())

	case "appendentries":
		resp, err := client.AppendEntries(ctx, &raftpb.AppendEntriesRequest{
			Term:     *term,
			LeaderId: *leaderID,
		})
		if err != nil {
			log.Fatalf("AppendEntries fallita: %v", err)
		}
		fmt.Printf("AppendEntriesResponse: term=%d success=%t\n", resp.GetTerm(), resp.GetSuccess())

	default:
		log.Fatalf("valore -rpc sconosciuto: %q (atteso requestvote|appendentries)", *rpc)
	}
}
