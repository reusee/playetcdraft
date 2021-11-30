package main

import (
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/reusee/dscope"
	"github.com/reusee/pr"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
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
				peb *pebble.DB,
			) {

				var node raft.Node
				nodeOK := make(chan struct{})

				// raft
				wt.Go(func() {

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
					close(nodeOK)

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
										proposal := new(SetProposal)
										ce(proto.Unmarshal(entry.Data, proposal))
										ce(peb.Set(proposal.Key, proposal.Value, writeOptions))
									}

								}

							}

							node.Advance()

						}
					}

				})

				// kv
				wt.Go(func() {
					<-nodeOK
					kvDefs := dscope.Methods(new(KVScope))
					kvDefs = append(kvDefs, &node)
					kvScope := nodeScope.Fork(kvDefs...)

					kvScope.Call(func(
						set Set,
						get Get,
					) {

						ce(set(42, 42))

						time.Sleep(time.Second)
						var i int
						ce(get(42, &i))
						pt("%d\n", i)

					})
				})

			})

		}
	})

	select {}
}
