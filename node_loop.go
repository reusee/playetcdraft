package main

import (
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/reusee/pr"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type RunLoop func(
	node *raft.Node,
	nodeOK chan struct{},
)

type RunInLoop func(fn func())

type Reading struct {
	*sync.Map
}

func (_ NodeScope) RunLoop(
	wt *pr.WaitTree,
	nodeExists NodeExists,
	nodeID NodeID,
	setInit SetInitialState,
	config *raft.Config,
	ticker *time.Ticker,
	raftPeers []raft.Peer,
	receive ReceiveMessage,
	saveReady SaveReady,
	raftPeer raft.Peer,
	send SendMessage,
	saveConfState SaveConfState,
	peb *pebble.DB,
) (
	run RunLoop,
	runInLoop RunInLoop,
	reading Reading,
) {

	fns := make(chan func())

	reading = Reading{
		Map: new(sync.Map),
	}

	run = func(
		nodePtr *raft.Node,
		nodeOK chan struct{},
	) {

		// apply
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

		// read state
		wt.Go(func() {
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

			exists, err := nodeExists(nodeID)
			ce(err)
			if exists {
				pt("restart node %v\n", nodeID)
				*nodePtr = raft.RestartNode(config)
			} else {
				pt("start node %v\n", nodeID)
				ce(setInit(nodeID))
				*nodePtr = raft.StartNode(config, raftPeers)
			}
			close(nodeOK)
			node := *nodePtr

			for {
				select {

				case <-wt.Ctx.Done():
					return

				case fn := <-fns:
					fn()

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
					for _, state := range ready.ReadStates {
						readStates = append(readStates, state)
					}
					cond.L.Unlock()
					cond.Signal()

					node.Advance()

				}
			}

		})
	}

	runInLoop = func(fn func()) {
		fns <- fn
	}

	return
}
