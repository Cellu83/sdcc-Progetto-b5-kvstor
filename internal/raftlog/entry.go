// Package raftlog definisce le entry del log replicato di Raft.
// A partire dalla Fase 2 conterrà anche la loro persistenza su disco.
package raftlog

import "github.com/Cellu83/sdcc-Progetto-b5-kvstor/internal/kvstore"

// Entry è un singolo elemento del log replicato di Raft.
//
// Term e Index sono i metadati richiesti dall'algoritmo di consenso:
//   - Term è il mandato del leader durante il quale l'entry è stata proposta.
//     Serve a rilevare log obsoleti e a decidere chi vince un'elezione.
//   - Index è la posizione univoca e progressiva dell'entry nel log.
//
// Command è l'operazione applicativa (definita in kvstore) che, una volta
// che l'entry è committata dalla maggioranza dei nodi, verrà eseguita
// sulla state machine chiave-valore.
type Entry struct {
	Term    uint64
	Index   uint64
	Command kvstore.Command
}
