package main

import (
	"fmt"
	"net"
	"time"

	"github.com/reusee/sb"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type SendMessage func(msg raftpb.Message) error

type ReceiveMessage chan raftpb.Message

func (_ NodeScope) Message(
	peer Peer,
	wt NodeWaitTree,
	peers Peers,
) (
	send SendMessage,
	receive ReceiveMessage,
) {

	outboundConns := make(map[NodeID]net.Conn)
	dialer := &net.Dialer{
		Timeout: time.Second * 16,
	}

	getConn := func(nodeID NodeID) (net.Conn, error) {
		if conn, ok := outboundConns[nodeID]; ok {
			return conn, nil
		}
		toPeer, ok := peers[nodeID]
		if !ok {
			return nil, we.With(
				fmt.Errorf("%d not found", nodeID),
			)(ErrNodeNotFound)
		}
		retry := 10
	connect:
		conn, err := dialer.DialContext(wt.Ctx, "tcp", toPeer.DialAddr)
		if err != nil {
			if retry > 0 {
				retry--
				time.Sleep(time.Second)
				goto connect
			}
			return nil, we(err)
		}
		outboundConns[nodeID] = conn
		return conn, nil
	}

	send = func(msg raftpb.Message) (err error) {
		defer he(&err)
		conn, err := getConn(NodeID(msg.To))
		ce(err)
		defer func() {
			if err != nil {
				conn.Close()
				delete(outboundConns, NodeID(msg.To))
			}
		}()
		ce(conn.SetWriteDeadline(time.Now().Add(time.Second * 32)))
		ce(sb.Copy(
			sb.Marshal(msg),
			sb.Encode(conn),
		))
		return
	}

	receive = make(chan raftpb.Message, 128)

	wt.Go(func() {
		listenConfig := &net.ListenConfig{}
		ln, err := listenConfig.Listen(wt.Ctx, "tcp", peer.ListenAddr)
		ce(err)
		defer ln.Close()

		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			wt.Go(func() {
				defer conn.Close()

				for {
					var msg raftpb.Message
					if err := sb.Copy(
						sb.Decode(conn),
						sb.Unmarshal(&msg),
					); err != nil {
						return
					}
					select {
					case receive <- msg:
					case <-wt.Ctx.Done():
						return
					}
				}
			})
		}

	})

	return
}
