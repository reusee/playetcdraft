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

	var defs []interface{}
	defs = append(defs, dscope.Methods(new(Global))...)
	defs = append(defs, dscope.Methods(new(NodeScope))...)
	defs = append(defs, func() NodeID {
		panic("fork with NodeID")
	})
	scope := dscope.New(defs...)

	var peers []raft.Peer
	for nodeID := uint64(1); nodeID <= 3; nodeID++ {
		peers = append(peers, raft.Peer{
			ID: nodeID,
		})
	}

	ticker := time.NewTicker(time.Millisecond * 100)

	for _, id := range peers {
		nodeID := NodeID(id.ID)
		nodeScope := scope.Fork(&nodeID)

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
					node = raft.StartNode(config, peers)
				}

				id := uint64(nodeID)

				for {
					select {

					case <-wt.Ctx.Done():
						return

					case <-ticker.C:
						node.Tick()

					case ready := <-node.Ready():
						ce(saveReady(ready))

						for _, msg := range ready.Messages {
							if msg.To == id {
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

	select {}
}
