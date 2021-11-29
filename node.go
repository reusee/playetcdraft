package main

import (
	"go.etcd.io/etcd/raft/v3"
)

type NodeScope struct{}

type NodeID uint64

func (_ NodeScope) MemStore() *raft.MemoryStorage {
	return raft.NewMemoryStorage()
}

func (_ NodeScope) Config(
	storage *raft.MemoryStorage,
	nodeID NodeID,
) *raft.Config {
	return &raft.Config{
		ID:              uint64(nodeID),
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		MaxSizePerMsg:   4096,
		MaxInflightMsgs: 256,
	}
}
