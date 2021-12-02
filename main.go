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
		newNode NewNodeScope,
	) {

		for _, raftPeer := range raftPeers {
			nodeScope := newNode(raftPeer)
			var newKV NewKVScope
			nodeScope.Assign(&newKV)
			kv := newKV()

			kv.Call(func(
				set Set,
				get Get,
				nodeID NodeID,
				peers Peers,
				wt NodeWaitTree,
			) {

				wt.Go(func() {

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

		}
	})

	select {}
}
