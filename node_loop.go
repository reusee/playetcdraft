package main

import (
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

type Reading map[[8]byte]chan struct{}

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

	reading = make(map[[8]byte]chan struct{})

	run = func(
		nodePtr *raft.Node,
		nodeOK chan struct{},
	) {

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
								proposal := new(SetProposal)
								ce(proto.Unmarshal(entry.Data, proposal))
								ce(peb.Set(proposal.Key, proposal.Value, writeOptions))
							}

						}

					}

					for _, state := range ready.ReadStates {
						key := *(*[8]byte)(state.RequestCtx)
						if c, ok := reading[key]; ok {
							close(c)
						}
					}

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
