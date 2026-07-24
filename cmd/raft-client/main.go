// raft-client è un piccolo strumento manuale per interrogare via gRPC un
// consensus node già in esecuzione, senza dover scrivere codice ad hoc.
// Utile per verificare a mano la tubatura di rete (Fase 3), la logica di
// elezione (Fase 4), e ora (Fase 5) anche per fare Get/Put/Delete reali
// contro l'API applicativa KVStoreService.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	kvstorepb "github.com/Cellu83/sdcc-Progetto-b5-kvstor/proto/kvstore"
	raftpb "github.com/Cellu83/sdcc-Progetto-b5-kvstor/proto/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	addr := flag.String("addr", "localhost:50051", "indirizzo del consensus node da contattare")
	rpc := flag.String("rpc", "requestvote", "RPC da chiamare: requestvote | appendentries | get | put | delete")
	candidateID := flag.String("candidate-id", "debug-client", "candidateId da inviare (RequestVote)")
	leaderID := flag.String("leader-id", "debug-client", "leaderId da inviare (AppendEntries)")
	term := flag.Uint64("term", 1, "term da inviare")
	key := flag.String("key", "", "chiave da leggere/scrivere/cancellare (get|put|delete)")
	value := flag.String("value", "", "valore da scrivere (put)")
	timeout := flag.Duration("timeout", 5*time.Second, "timeout della chiamata")
	flag.Parse()

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("impossibile connettersi a %s: %v", *addr, err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	switch *rpc {
	case "requestvote":
		client := raftpb.NewRaftServiceClient(conn)
		resp, err := client.RequestVote(ctx, &raftpb.RequestVoteRequest{
			Term:        *term,
			CandidateId: *candidateID,
		})
		if err != nil {
			log.Fatalf("RequestVote fallita: %v", err)
		}
		fmt.Printf("RequestVoteResponse: term=%d voteGranted=%t\n", resp.GetTerm(), resp.GetVoteGranted())

	case "appendentries":
		client := raftpb.NewRaftServiceClient(conn)
		resp, err := client.AppendEntries(ctx, &raftpb.AppendEntriesRequest{
			Term:     *term,
			LeaderId: *leaderID,
		})
		if err != nil {
			log.Fatalf("AppendEntries fallita: %v", err)
		}
		fmt.Printf("AppendEntriesResponse: term=%d success=%t\n", resp.GetTerm(), resp.GetSuccess())

	case "get":
		requireKey(*key)
		client := kvstorepb.NewKVStoreServiceClient(conn)
		resp, err := client.Get(ctx, &kvstorepb.GetRequest{Key: *key})
		if err != nil {
			log.Fatalf("Get fallita: %v", err)
		}
		fmt.Printf("GetResponse: value=%q found=%t\n", resp.GetValue(), resp.GetFound())

	case "put":
		requireKey(*key)
		client := kvstorepb.NewKVStoreServiceClient(conn)
		resp, err := client.Put(ctx, &kvstorepb.PutRequest{Key: *key, Value: *value})
		if err != nil {
			log.Fatalf("Put fallita: %v", err)
		}
		fmt.Printf("PutResponse: success=%t\n", resp.GetSuccess())

	case "delete":
		requireKey(*key)
		client := kvstorepb.NewKVStoreServiceClient(conn)
		resp, err := client.Delete(ctx, &kvstorepb.DeleteRequest{Key: *key})
		if err != nil {
			log.Fatalf("Delete fallita: %v", err)
		}
		fmt.Printf("DeleteResponse: success=%t\n", resp.GetSuccess())

	default:
		log.Fatalf("valore -rpc sconosciuto: %q (atteso requestvote|appendentries|get|put|delete)", *rpc)
	}
}

func requireKey(key string) {
	if key == "" {
		log.Fatal("è obbligatorio specificare -key per questa operazione")
	}
}
