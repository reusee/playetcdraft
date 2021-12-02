package main

import (
	"bytes"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/reusee/dscope"
	"github.com/reusee/sb"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type NodeScope struct{}

type NewNodeScope func(raft.Peer) Scope

func (_ Global) NewNodeScope(
	scope Scope,
	peers Peers,
) NewNodeScope {
	return func(raftPeer raft.Peer) Scope {
		nodeID := NodeID(raftPeer.ID)
		peer := peers[nodeID]

		defs := dscope.Methods(new(NodeScope))
		defs = append(defs, &raftPeer, &nodeID, &peer)

		return scope.Fork(defs...)
	}
}

type NodeID uint64

type Reading struct {
	*sync.Map
}

func (_ NodeScope) RaftNode(
	nodeExists NodeExists,
	nodeID NodeID,
	config *raft.Config,
	setInit SetInitialState,
	wt NodeWaitTree,
	raftPeers []raft.Peer,
	peb *pebble.DB,
	ticker *time.Ticker,
	receive ReceiveMessage,
	saveReady SaveReady,
	raftPeer raft.Peer,
	send SendMessage,
	saveConfState SaveConfState,
) (
	node raft.Node,
	reading Reading,
) {

	exists, err := nodeExists(nodeID)
	ce(err)
	if exists {
		pt("restart node %v\n", nodeID)
		node = raft.RestartNode(config)
	} else {
		pt("start node %v\n", nodeID)
		ce(setInit(nodeID))
		node = raft.StartNode(config, raftPeers)
	}

	// apply loop
	cond := sync.NewCond(new(sync.Mutex))
	var appliedIndex uint64
	var readStates []raft.ReadState
	applyCh := make(chan raftpb.Entry)
	wt.Go(func() {
		for {
			select {
			case <-wt.Ctx.Done():
				return
			case entry := <-applyCh:
				proposal := new(SetProposal)
				ce(proto.Unmarshal(entry.Data, proposal))
				ce(peb.Set(proposal.Key, proposal.Value, writeOptions))
				pt("apply %d\n", entry.Index)
				cond.L.Lock()
				if entry.Index > appliedIndex {
					appliedIndex = entry.Index
				}
				cond.L.Unlock()
				cond.Signal()
			}
		}
	})

	reading = Reading{
		Map: new(sync.Map),
	}

	// read state loop
	wt.Go(func() {
		//TODO handle ctx cancel
		for {
			cond.L.Lock()
			for len(readStates) == 0 ||
				readStates[0].Index > appliedIndex {
				cond.Wait()
			}
			newReadStates := readStates[:0]
			for _, state := range readStates {
				if state.Index > appliedIndex {
					newReadStates = append(newReadStates, state)
				} else {
					pt("%d ready\n", state.Index)
					key := *(*[8]byte)(state.RequestCtx)
					if v, ok := reading.Load(key); ok {
						close(v.(chan struct{}))
					}
				}
			}
			readStates = newReadStates
			cond.L.Unlock()
		}
	})

	// loop
	wt.Go(func() {

		for {
			select {

			case <-wt.Ctx.Done():
				return

			case <-ticker.C:
				node.Tick()

			case msg := <-receive:
				node.Step(wt.Ctx, msg)

			case ready := <-node.Ready():
				ce(saveReady(ready))

				for _, msg := range ready.Messages {
					if msg.To == raftPeer.ID {
						node.Step(wt.Ctx, msg)
					} else {
						err := send(msg)
						if msg.Type == raftpb.MsgSnap {
							if err != nil {
								node.ReportSnapshot(msg.To, raft.SnapshotFailure)
							} else {
								node.ReportSnapshot(msg.To, raft.SnapshotFinish)
							}
						}
						ce(err)
					}
				}

				if !raft.IsEmptySnap(ready.Snapshot) {
					//TODO apply snapshot
					pt("fixme: apply snapshot %+v\n", ready.Snapshot)
				}

				for _, entry := range ready.CommittedEntries {
					switch entry.Type {

					case raftpb.EntryConfChange:
						var cc raftpb.ConfChange
						ce(cc.Unmarshal(entry.Data))
						state := node.ApplyConfChange(cc)
						ce(saveConfState(state))

					case raftpb.EntryNormal:
						if len(entry.Data) > 0 {
							applyCh <- entry
						}

					}

				}

				cond.L.Lock()
				readStates = append(readStates, ready.ReadStates...)
				cond.L.Unlock()
				cond.Signal()

				node.Advance()

			}
		}

	})

	return
}

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

type NodeExists func(nodeID NodeID) (bool, error)

func (_ Global) NodeExists(
	peb *pebble.DB,
) NodeExists {
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
