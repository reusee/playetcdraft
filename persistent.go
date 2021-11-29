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
			Separator: func(dst, a, b []byte) []byte {
				return a
			},
			Successor: func(dst, a []byte) []byte {
				return a
			},
		},
	})
	ce(err)
	return db
}

const (
	DBKeyHardState = iota + 1
	DBKeyEntry
	DBKeySnapshot
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

		keyBuf := new(bytes.Buffer)
		ce(sb.Copy(
			sb.Marshal(func() (NodeID, uint8) {
				return nodeID, DBKeyHardState
			}),
			sb.Encode(keyBuf),
		))
		key := keyBuf.Bytes()

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

type SaveEntry func(batch *pebble.Batch, entry raftpb.Entry) error

func (_ NodeScope) SaveEntry(
	nodeID NodeID,
	peb *pebble.DB,
) SaveEntry {
	// (NodeID, DBKeyEntry, entry.Index) -> Entry
	return func(batch *pebble.Batch, entry raftpb.Entry) (err error) {
		defer he(&err)

		keyBuf := new(bytes.Buffer)
		ce(sb.Copy(
			sb.Marshal(func() (NodeID, uint8, uint64) {
				return nodeID, DBKeyEntry, entry.Index
			}),
			sb.Encode(keyBuf),
		))
		key := keyBuf.Bytes()

		// discard
		upperBoundBuf := new(bytes.Buffer)
		ce(sb.Copy(
			sb.Marshal(func() (NodeID, uint8, *sb.Token) {
				return nodeID, DBKeyEntry, sb.Max
			}),
			sb.Encode(upperBoundBuf),
		))
		upperBound := upperBoundBuf.Bytes()

		iter := batch.NewIter(&pebble.IterOptions{
			LowerBound: key,
			UpperBound: upperBound,
		})
		for iter.First(); iter.Valid(); iter.Next() {
			key := iter.Key()
			ce(batch.Delete(key, writeOptions))
		}
		ce(iter.Error())
		ce(iter.Close())

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

type SaveSnapshot func(batch *pebble.Batch, snapshot raftpb.Snapshot) error

func (_ NodeScope) SaveSnapshot(
	nodeID NodeID,
) SaveSnapshot {
	// (NodeID, DBKeySnapshot) -> Snapshot
	return func(batch *pebble.Batch, snapshot raftpb.Snapshot) (err error) {
		defer he(&err)

		keyBuf := new(bytes.Buffer)
		ce(sb.Copy(
			sb.Marshal(func() (NodeID, uint8) {
				return nodeID, DBKeySnapshot
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

var emptyHardState = raftpb.HardState{}

type SaveReady func(ready raft.Ready) error

func (_ NodeScope) SaveReady(
	peb *pebble.DB,
	saveEntry SaveEntry,
	saveHardState SaveHardState,
	saveSnapshot SaveSnapshot,
) SaveReady {
	return func(ready raft.Ready) (err error) {
		if ready.Entries == nil &&
			ready.HardState == emptyHardState &&
			ready.Snapshot.Size() == 0 {
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
		if ready.HardState != emptyHardState {
			ce(saveHardState(batch, ready.HardState))
		}
		if ready.Snapshot.Size() > 0 {
			ce(saveSnapshot(batch, ready.Snapshot))
		}

		return
	}
}
