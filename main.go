package main

import (
	"time"

	"github.com/reusee/dscope"
	"go.etcd.io/etcd/raft/v3"
)

type Global struct{}

func main() {

	global := dscope.New(dscope.Methods(new(Global))...)

	global.Call(func(
		raftPeers []raft.Peer,
		peers Peers,
	) {

		for _, raftPeer := range raftPeers {
			raftPeer := raftPeer
			nodeID := NodeID(raftPeer.ID)
			peer := peers[nodeID]

			defs := dscope.Methods(new(NodeScope))
			defs = append(defs, &nodeID, &peer, &raftPeer)
			nodeScope := global.Fork(defs...)

			nodeScope.Call(func(
				node raft.Node,
				wt NodeWaitTree,
			) {

				// kv
				wt.Go(func() {
					kvDefs := dscope.Methods(new(KVScope))
					kvDefs = append(kvDefs, &node)
					kvScope := nodeScope.Fork(kvDefs...)

					kvScope.Call(func(
						set Set,
						get Get,
					) {

						ce(set(nodeID, int(nodeID)))

						time.Sleep(time.Second)

						for n := range peers {
							var i int
							ce(get(n, &i))
							pt("%d %d %d\n", nodeID, n, i)
							if int(n) != i {
								panic("bad value")
							}
						}

					})
				})

			})

		}
	})

	select {}
}
