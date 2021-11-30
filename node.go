package main

import (
	"bytes"

	"github.com/cockroachdb/pebble"
	"github.com/reusee/sb"
	"go.etcd.io/etcd/raft/v3"
)

type NodeScope struct{}

type NodeID uint64

func (_ NodeScope) Config(
	storage *Storage,
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

type NodeIsInDB func(nodeID NodeID) (bool, error)

func (_ Global) NodeIsInDB(
	peb *pebble.DB,
) NodeIsInDB {
	return func(nodeID NodeID) (exists bool, err error) {
		defer he(&err)

		lowerBoundBuf := new(bytes.Buffer)
		ce(sb.Copy(
			sb.Marshal(func() (Namespace, NodeID) {
				return NamespaceRaft, nodeID
			}),
			sb.Encode(lowerBoundBuf),
		))
		lowerBound := lowerBoundBuf.Bytes()

		iter := peb.NewIter(&pebble.IterOptions{
			LowerBound: lowerBound,
		})
		defer func() {
			ce(iter.Error())
			ce(iter.Close())
		}()

		n := 0
		for iter.First(); iter.Valid(); iter.Next() {
			n++
			break
		}

		return n > 0, nil
	}
}
