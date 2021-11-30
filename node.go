package main

import (
	"bytes"

	"github.com/cockroachdb/pebble"
	"github.com/reusee/sb"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
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
			sb.Marshal(func() (Namespace, NodeID, *sb.Token) {
				return NamespaceRaft, nodeID, sb.Min
			}),
			sb.Encode(lowerBoundBuf),
		))
		lowerBound := lowerBoundBuf.Bytes()
		upperBoundBuf := new(bytes.Buffer)
		ce(sb.Copy(
			sb.Marshal(func() (Namespace, NodeID, *sb.Token) {
				return NamespaceRaft, nodeID, sb.Max
			}),
			sb.Encode(upperBoundBuf),
		))
		upperBound := upperBoundBuf.Bytes()

		iter := peb.NewIter(&pebble.IterOptions{
			LowerBound: lowerBound,
			UpperBound: upperBound,
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

type SetInitialState func(nodeID NodeID) error

func (_ NodeScope) SetInitialState(
	peb *pebble.DB,
	peers Peers,
) SetInitialState {
	return func(nodeID NodeID) (err error) {
		defer he(&err)

		batch := peb.NewBatch()
		defer func() {
			if err == nil {
				err = batch.Commit(writeOptions)
			} else {
				batch.Close()
			}
		}()

		// set entry
		key, err := entryKey(nodeID, 42)
		ce(err)
		buf := new(bytes.Buffer)
		ce(sb.Copy(
			sb.Marshal(raftpb.Entry{
				Term:  0,
				Index: 42,
				Type:  raftpb.EntryNormal,
			}),
			sb.Encode(buf),
		))
		value := buf.Bytes()
		ce(batch.Set(key, value, writeOptions))

		// set conf state
		key, err = confStateKey(nodeID)
		ce(err)
		buf.Reset()
		ce(sb.Copy(
			sb.Marshal(raftpb.ConfState{
				Voters: func() (ret []uint64) {
					for id := range peers {
						ret = append(ret, uint64(id))
					}
					return
				}(),
			}),
			sb.Encode(buf),
		))
		value = buf.Bytes()
		ce(batch.Set(key, value, writeOptions))

		return
	}
}
