package main

import (
	"bytes"

	"github.com/cockroachdb/pebble"
	"github.com/reusee/e4"
	"github.com/reusee/pr"
	"github.com/reusee/sb"
	"go.etcd.io/etcd/raft/v3"
)

type KVScope struct{}

type Set func(key any, value any) error

type Get func(key any, target any) error

type SetProposal struct {
	Key   any
	Value any
}

func (_ KVScope) KV(
	peb *pebble.DB,
	node raft.Node,
	wt *pr.WaitTree,
) (
	set Set,
	get Get,
) {

	set = func(key any, value any) (err error) {
		defer he(&err)
		buf := new(bytes.Buffer)
		ce(sb.Copy(
			sb.Marshal(SetProposal{
				Key:   key,
				Value: value,
			}),
			sb.Encode(buf),
		))
		ce(node.Propose(wt.Ctx, buf.Bytes()))
		return
	}

	get = func(key any, target any) (err error) {
		defer he(&err)

		buf := new(bytes.Buffer)
		// (NamespaceKV, key) -> value
		ce(sb.Copy(
			sb.Marshal(func() (Namespace, any) {
				return NamespaceKV, key
			}),
			sb.Encode(buf),
		))
		bsKey := buf.Bytes()

		value, cl, err := peb.Get(bsKey)
		if err == pebble.ErrNotFound {
			return we(ErrKeyNotFound)
		}
		ce(sb.Copy(
			sb.Decode(bytes.NewReader(value)),
			sb.Unmarshal(target),
		), e4.Close(cl))
		ce(cl.Close())

		return
	}

	return
}
