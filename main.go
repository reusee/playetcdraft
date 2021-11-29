package main

import (
	"github.com/reusee/dscope"
	"go.etcd.io/etcd/raft/v3"
)

type Global struct{}

func main() {

	var defs []interface{}
	defs = append(defs, dscope.Methods(new(Global))...)
	defs = append(defs, dscope.Methods(new(NodeScope))...)
	defs = append(defs, func() NodeID {
		return 42
	})
	scope := dscope.New(defs...)

	scope.Call(func(
		config *raft.Config,
		nodeID NodeID,
		saveReady SaveReady,
	) {

		node := raft.StartNode(config, []raft.Peer{
			{
				ID: uint64(nodeID),
			},
		})

		for {
			select {

			case ready := <-node.Ready():
				ce(saveReady(ready))

				//TODO

			}
		}

	})

}
