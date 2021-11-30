package main

import (
	"bytes"

	"github.com/cockroachdb/pebble"
	"github.com/reusee/sb"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

func (_ Global) PebbleDB() *pebble.DB {
	db, err := pebble.Open("raftdb", &pebble.Options{
		Comparer: &pebble.Comparer{
			Compare: func(a, b []byte) int {
				return sb.MustCompareBytes(a, b)
			},
			Equal: func(a, b []byte) bool {
				return bytes.Equal(a, b)
			},
			AbbreviatedKey: func(key []byte) uint64 {
				return 0
			},
			Separator: func(dst, a, b []byte) []byte {
				return a
			},
			Successor: func(dst, a []byte) []byte {
				return a
			},
			Name: "sb-comparer",
		},
	})
	ce(err)
	return db
}

type Namespace uint8

const (
	NamespaceRaft Namespace = iota + 1
	NamespaceKV
)

type DBKey uint8

const (
	DBKeyHardState DBKey = iota + 1
	DBKeyEntry
	DBKeySnapshot
	DBKeyConfState
)

var writeOptions = &pebble.WriteOptions{
	Sync: true,
}

type SaveHardState func(batch *pebble.Batch, state raftpb.HardState) error

func (_ NodeScope) SaveHardState(
	nodeID NodeID,
) SaveHardState {
	// (NodeID, DBKeyHardState) -> HardState
	return func(batch *pebble.Batch, state raftpb.HardState) (err error) {
		defer he(&err)

		key, err := hardStateKey(nodeID)
		ce(err)

		valueBuf := new(bytes.Buffer)
		ce(sb.Copy(
			sb.Marshal(state),
			sb.Encode(valueBuf),
		))
		value := valueBuf.Bytes()

		ce(batch.Set(key, value, writeOptions))

		return
	}
}

func hardStateKey(nodeID NodeID) ([]byte, error) {
	keyBuf := new(bytes.Buffer)
	if err := sb.Copy(
		sb.Marshal(func() (Namespace, NodeID, DBKey) {
			return NamespaceRaft, nodeID, DBKeyHardState
		}),
		sb.Encode(keyBuf),
	); err != nil {
		return nil, err
	}
	key := keyBuf.Bytes()
	return key, nil
}

type SaveEntry func(batch *pebble.Batch, entry raftpb.Entry) error

func (_ NodeScope) SaveEntry(
	nodeID NodeID,
	peb *pebble.DB,
) SaveEntry {
	// (NodeID, DBKeyEntry, entry.Index) -> Entry
	return func(batch *pebble.Batch, entry raftpb.Entry) (err error) {
		defer he(&err)

		key, err := entryKey(nodeID, entry.Index)
		ce(err)

		// discard
		upperBoundBuf := new(bytes.Buffer)
		ce(sb.Copy(
			sb.Marshal(func() (Namespace, NodeID, DBKey, *sb.Token) {
				return NamespaceRaft, nodeID, DBKeyEntry, sb.Max
			}),
			sb.Encode(upperBoundBuf),
		))
		upperBound := upperBoundBuf.Bytes()

		iter := batch.NewIter(&pebble.IterOptions{
			LowerBound: key,
			UpperBound: upperBound,
		})
		defer func() {
			ce(iter.Error())
			ce(iter.Close())
		}()
		for iter.First(); iter.Valid(); iter.Next() {
			key := iter.Key()
			ce(batch.Delete(key, writeOptions))
		}

		// save
		valueBuf := new(bytes.Buffer)
		ce(sb.Copy(
			sb.Marshal(entry),
			sb.Encode(valueBuf),
		))
		value := valueBuf.Bytes()

		ce(batch.Set(key, value, writeOptions))

		return
	}
}

func entryKey(nodeID NodeID, index uint64) ([]byte, error) {
	keyBuf := new(bytes.Buffer)
	if err := sb.Copy(
		sb.Marshal(func() (Namespace, NodeID, DBKey, uint64) {
			return NamespaceRaft, nodeID, DBKeyEntry, index
		}),
		sb.Encode(keyBuf),
	); err != nil {
		return nil, err
	}
	key := keyBuf.Bytes()
	return key, nil
}

type SaveSnapshot func(batch *pebble.Batch, snapshot raftpb.Snapshot) error

func (_ NodeScope) SaveSnapshot(
	nodeID NodeID,
) SaveSnapshot {
	// (NodeID, DBKeySnapshot) -> Snapshot
	return func(batch *pebble.Batch, snapshot raftpb.Snapshot) (err error) {
		defer he(&err)

		keyBuf := new(bytes.Buffer)
		ce(sb.Copy(
			sb.Marshal(func() (Namespace, NodeID, DBKey) {
				return NamespaceRaft, nodeID, DBKeySnapshot
			}),
			sb.Encode(keyBuf),
		))
		key := keyBuf.Bytes()

		valueBuf := new(bytes.Buffer)
		ce(sb.Copy(
			sb.Marshal(snapshot),
			sb.Encode(valueBuf),
		))
		value := valueBuf.Bytes()

		ce(batch.Set(key, value, writeOptions))

		return
	}
}

type SaveConfState func(*raftpb.ConfState) error

func (_ NodeScope) SaveConfState(
	nodeID NodeID,
	peb *pebble.DB,
) SaveConfState {
	// (NodeID, DBKeyConfState) -> ConfState
	return func(confState *raftpb.ConfState) (err error) {
		defer he(&err)

		key, err := confStateKey(nodeID)
		ce(err)

		valueBuf := new(bytes.Buffer)
		ce(sb.Copy(
			sb.Marshal(confState),
			sb.Encode(valueBuf),
		))
		value := valueBuf.Bytes()

		ce(peb.Set(key, value, writeOptions))

		return
	}
}

func confStateKey(nodeID NodeID) ([]byte, error) {
	keyBuf := new(bytes.Buffer)
	if err := sb.Copy(
		sb.Marshal(func() (Namespace, NodeID, DBKey) {
			return NamespaceRaft, nodeID, DBKeyConfState
		}),
		sb.Encode(keyBuf),
	); err != nil {
		return nil, err
	}
	key := keyBuf.Bytes()
	return key, nil
}

type SaveReady func(ready raft.Ready) error

func (_ NodeScope) SaveReady(
	peb *pebble.DB,
	saveEntry SaveEntry,
	saveHardState SaveHardState,
	saveSnapshot SaveSnapshot,
) SaveReady {
	return func(ready raft.Ready) (err error) {
		if len(ready.Entries) == 0 &&
			raft.IsEmptyHardState(ready.HardState) &&
			raft.IsEmptySnap(ready.Snapshot) {
			return nil
		}

		batch := peb.NewIndexedBatch()
		defer func() {
			if err != nil {
				batch.Close()
			} else {
				err = batch.Commit(writeOptions)
			}
		}()

		for _, entry := range ready.Entries {
			ce(saveEntry(batch, entry))
		}
		if !raft.IsEmptyHardState(ready.HardState) {
			ce(saveHardState(batch, ready.HardState))
		}
		if !raft.IsEmptySnap(ready.Snapshot) {
			ce(saveSnapshot(batch, ready.Snapshot))
		}

		return
	}
}
