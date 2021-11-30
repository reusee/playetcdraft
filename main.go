package main

import (
	"time"

	"github.com/reusee/dscope"
	"github.com/reusee/pr"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type Global struct{}

func main() {

	global := dscope.New(dscope.Methods(new(Global))...)

	global.Call(func(
		raftPeers []raft.Peer,
		peers Peers,
	) {

		ticker := time.NewTicker(time.Millisecond * 100)

		for _, raftPeer := range raftPeers {
			raftPeer := raftPeer
			nodeID := NodeID(raftPeer.ID)
			peer := peers[nodeID]

			defs := dscope.Methods(new(NodeScope))
			defs = append(defs, &nodeID, &peer)
			nodeScope := global.Fork(defs...)

			nodeScope.Call(func(
				config *raft.Config,
				nodeID NodeID,
				saveReady SaveReady,
				saveConfState SaveConfState,
				wt *pr.WaitTree,
				nodeExists NodeExists,
				send SendMessage,
				receive ReceiveMessage,
				setInit SetInitialState,
			) {

				wt.Go(func() {

					exists, err := nodeExists(nodeID)
					ce(err)
					var node raft.Node
					if exists {
						pt("restart node %v\n", nodeID)
						node = raft.RestartNode(config)
					} else {
						pt("start node %v\n", nodeID)
						ce(setInit(nodeID))
						node = raft.StartNode(config, raftPeers)
					}

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
								//TODO commit entry
								pt("fixme: commit entry %+v\n", entry)
								if entry.Type == raftpb.EntryConfChange {
									var cc raftpb.ConfChange
									ce(cc.Unmarshal(entry.Data))
									state := node.ApplyConfChange(cc)
									ce(saveConfState(state))
								}
							}

							node.Advance()

						}
					}

				})

			})

		}
	})

	select {}
}
