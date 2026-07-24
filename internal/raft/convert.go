package raft

import (
	"github.com/Cellu83/sdcc-Progetto-b5-kvstor/internal/kvstore"
	"github.com/Cellu83/sdcc-Progetto-b5-kvstor/internal/raftlog"
	raftpb "github.com/Cellu83/sdcc-Progetto-b5-kvstor/proto/raft"
)

// toProtoEntries converte le entry di log dal tipo Go interno (usato da
// Node e raftlog.Storage) al messaggio protobuf da spedire in rete.
func toProtoEntries(entries []raftlog.Entry) []*raftpb.LogEntry {
	out := make([]*raftpb.LogEntry, len(entries))
	for i, e := range entries {
		out[i] = &raftpb.LogEntry{
			Term:  e.Term,
			Index: e.Index,
			Command: &raftpb.Command{
				Op:    toProtoOp(e.Command.Op),
				Key:   e.Command.Key,
				Value: e.Command.Value,
			},
		}
	}
	return out
}

// fromProtoEntries fa il percorso inverso: dal messaggio protobuf ricevuto
// in rete al tipo Go interno.
func fromProtoEntries(entries []*raftpb.LogEntry) []raftlog.Entry {
	out := make([]raftlog.Entry, len(entries))
	for i, e := range entries {
		out[i] = raftlog.Entry{
			Term:  e.GetTerm(),
			Index: e.GetIndex(),
			Command: kvstore.Command{
				Op:    fromProtoOp(e.GetCommand().GetOp()),
				Key:   e.GetCommand().GetKey(),
				Value: e.GetCommand().GetValue(),
			},
		}
	}
	return out
}

func toProtoOp(op kvstore.OpType) raftpb.OpType {
	if op == kvstore.OpDelete {
		return raftpb.OpType_OP_DELETE
	}
	return raftpb.OpType_OP_PUT
}

func fromProtoOp(op raftpb.OpType) kvstore.OpType {
	if op == raftpb.OpType_OP_DELETE {
		return kvstore.OpDelete
	}
	return kvstore.OpPut
}
