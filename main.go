package main

import (
	"math/rand"
	"time"

	"github.com/reusee/dscope"
	"github.com/reusee/pr"
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
				runLoop RunLoop,
				wt *pr.WaitTree,
			) {

				var node raft.Node
				nodeOK := make(chan struct{})

				// raft
				runLoop(&node, nodeOK)

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

						n := rand.Int63()
						ce(set(n, n))

						time.Sleep(time.Second)

						var i int64
						ce(get(n, &i))
						pt("%v\n", i)

					})
				})

			})

		}
	})

	select {}
}
