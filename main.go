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
	) {

		ticker := time.NewTicker(time.Millisecond * 100)

		for _, peer := range raftPeers {
			peer := peer
			nodeID := NodeID(peer.ID)

			defs := dscope.Methods(new(NodeScope))
			defs = append(defs, &nodeID)
			nodeScope := global.Fork(defs...)

			nodeScope.Call(func(
				config *raft.Config,
				nodeID NodeID,
				saveReady SaveReady,
				saveConfState SaveConfState,
				wt *pr.WaitTree,
				nodeIsInDB NodeIsInDB,
			) {

				wt.Go(func() {

					exists, err := nodeIsInDB(nodeID)
					ce(err)
					var node raft.Node
					if exists {
						pt("restart node %v\n", nodeID)
						node = raft.RestartNode(config)
					} else {
						pt("start node %v\n", nodeID)
						node = raft.StartNode(config, raftPeers)
					}

					for {
						select {

						case <-wt.Ctx.Done():
							return

						case <-ticker.C:
							node.Tick()

						case ready := <-node.Ready():
							ce(saveReady(ready))

							for _, msg := range ready.Messages {
								if msg.To == peer.ID {
									node.Step(wt.Ctx, msg)
								} else {
									//TODO send
									pt("fixme: send %+v\n", msg)
								}
							}

							if !raft.IsEmptySnap(ready.Snapshot) {
								//TODO apply snapshot
								pt("fixme: apply snapshot %+v\n", ready.Snapshot)
							}

							for _, entry := range ready.CommittedEntries {
								//TODO commit entry
								pt("fixme: process entry %+v\n", entry)
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
