package main

import (
	"bytes"

	"github.com/cockroachdb/pebble"
	"github.com/reusee/e4"
	"github.com/reusee/sb"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type Storage struct {
	peb    *pebble.DB
	nodeID NodeID
}

var _ raft.Storage = new(Storage)

func (_ NodeScope) Storage(
	peb *pebble.DB,
	nodeID NodeID,
) *Storage {
	return &Storage{
		peb:    peb,
		nodeID: nodeID,
	}
}

func (s *Storage) InitialState() (hardState raftpb.HardState, confState raftpb.ConfState, err error) {
	defer he(&err)

	snapshot := s.peb.NewSnapshot()
	defer snapshot.Close()

	key, err := hardStateKey(s.nodeID)
	ce(err)
	data, cl, err := snapshot.Get(key)
	ce(err, e4.Ignore(pebble.ErrNotFound))
	if err == pebble.ErrNotFound {
		err = nil
	} else {
		ce(sb.Copy(
			sb.Decode(bytes.NewReader(data)),
			sb.Unmarshal(&hardState),
		), e4.Close(cl))
		ce(cl.Close())
	}

	key, err = confStateKey(s.nodeID)
	ce(err)
	data, cl, err = snapshot.Get(key)
	ce(err, e4.Ignore(pebble.ErrNotFound))
	if err == pebble.ErrNotFound {
		err = nil
	} else {
		ce(sb.Copy(
			sb.Decode(bytes.NewReader(data)),
			sb.Unmarshal(&confState),
		), e4.Close(cl))
		ce(cl.Close())
	}

	return
}

func (s *Storage) Entries(low, high, max uint64) (entries []raftpb.Entry, err error) {
	defer he(&err)

	lowerBoundBuf := new(bytes.Buffer)
	ce(sb.Copy(
		sb.Marshal(func() (Namespace, NodeID, Atom, uint64) {
			return NamespaceRaft, s.nodeID, AtomEntry, low
		}),
		sb.Encode(lowerBoundBuf),
	))
	lowerBound := lowerBoundBuf.Bytes()

	upperBoundBuf := new(bytes.Buffer)
	ce(sb.Copy(
		sb.Marshal(func() (Namespace, NodeID, Atom, uint64) {
			return NamespaceRaft, s.nodeID, AtomEntry, high
		}),
		sb.Encode(upperBoundBuf),
	))
	upperBound := upperBoundBuf.Bytes()

	iter := s.peb.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	defer func() {
		ce(iter.Error())
		ce(iter.Close())
	}()

	if !iter.First() {
		return nil, raft.ErrUnavailable
	}

	var size uint64
	for ; iter.Valid(); iter.Next() {
		var entry raftpb.Entry
		ce(sb.Copy(
			sb.Decode(bytes.NewReader(iter.Value())),
			sb.Unmarshal(&entry),
		))
		entries = append(entries, entry)
		size += uint64(entry.Size())
		if size > max {
			break
		}
	}

	return
}

func (s *Storage) Term(index uint64) (term uint64, err error) {
	defer he(&err)
	key, err := entryKey(s.nodeID, index)
	ce(err)
	data, cl, err := s.peb.Get(key)
	ce(err, e4.Ignore(pebble.ErrNotFound))
	if err == pebble.ErrNotFound {
		err = nil
	} else {
		defer cl.Close()
		var entry raftpb.Entry
		ce(sb.Copy(
			sb.Decode(bytes.NewReader(data)),
			sb.Unmarshal(&entry),
		))
		term = entry.Term
	}
	return
}

func (s *Storage) LastIndex() (idx uint64, err error) {
	defer he(&err)

	lowerBoundBuf := new(bytes.Buffer)
	ce(sb.Copy(
		sb.Marshal(func() (Namespace, NodeID, Atom, *sb.Token) {
			return NamespaceRaft, s.nodeID, AtomEntry, sb.Min
		}),
		sb.Encode(lowerBoundBuf),
	))
	lowerBound := lowerBoundBuf.Bytes()

	upperBoundBuf := new(bytes.Buffer)
	ce(sb.Copy(
		sb.Marshal(func() (Namespace, NodeID, Atom, *sb.Token) {
			return NamespaceRaft, s.nodeID, AtomEntry, sb.Max
		}),
		sb.Encode(upperBoundBuf),
	))
	upperBound := upperBoundBuf.Bytes()

	iter := s.peb.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	defer func() {
		ce(iter.Error())
		ce(iter.Close())
	}()

	if !iter.Last() {
		panic("impossible")
	}

	var entry raftpb.Entry
	ce(sb.Copy(
		sb.Decode(bytes.NewReader(iter.Value())),
		sb.Unmarshal(&entry),
	))

	return entry.Index, nil
}

func (s *Storage) FirstIndex() (idx uint64, err error) {
	defer he(&err)

	lowerBoundBuf := new(bytes.Buffer)
	ce(sb.Copy(
		sb.Marshal(func() (Namespace, NodeID, Atom, *sb.Token) {
			return NamespaceRaft, s.nodeID, AtomEntry, sb.Min
		}),
		sb.Encode(lowerBoundBuf),
	))
	lowerBound := lowerBoundBuf.Bytes()

	upperBoundBuf := new(bytes.Buffer)
	ce(sb.Copy(
		sb.Marshal(func() (Namespace, NodeID, Atom, *sb.Token) {
			return NamespaceRaft, s.nodeID, AtomEntry, sb.Max
		}),
		sb.Encode(upperBoundBuf),
	))
	upperBound := upperBoundBuf.Bytes()

	iter := s.peb.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	defer func() {
		ce(iter.Error())
		ce(iter.Close())
	}()

	if !iter.First() {
		panic("impossible")
	}

	var entry raftpb.Entry
	ce(sb.Copy(
		sb.Decode(bytes.NewReader(iter.Value())),
		sb.Unmarshal(&entry),
	))

	return entry.Index, nil
}

func (s *Storage) Snapshot() (raftpb.Snapshot, error) {
	//TODO
	panic("fixme")
}
