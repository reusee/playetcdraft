package main

import "go.etcd.io/etcd/raft/v3"

type Peer struct {
	ListenAddr string
	DialAddr   string
}

type Peers = map[NodeID]Peer

func (_ Global) Peers() (
	peers Peers,
) {
	return Peers{
		42: {
			ListenAddr: "localhost:50001",
			DialAddr:   "localhost:50001",
		},
		43: {
			ListenAddr: "localhost:50002",
			DialAddr:   "localhost:50002",
		},
		44: {
			ListenAddr: "localhost:50003",
			DialAddr:   "localhost:50003",
		},
		45: {
			ListenAddr: "localhost:50004",
			DialAddr:   "localhost:50004",
		},
		46: {
			ListenAddr: "localhost:50005",
			DialAddr:   "localhost:50005",
		},
	}
}

func (_ Global) RaftPeers(
	peers Peers,
) (ret []raft.Peer) {
	for nodeID := range peers {
		ret = append(ret, raft.Peer{
			ID: uint64(nodeID),
		})
	}
	return
}
