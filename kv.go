package main

import (
	"bytes"

	"github.com/cockroachdb/pebble"
	"github.com/reusee/e4"
	"github.com/reusee/pr"
	"github.com/reusee/sb"
	"go.etcd.io/etcd/raft/v3"
	"google.golang.org/protobuf/proto"
)

type KVScope struct{}

type Set func(key any, value any) error

type Get func(key any, target any) error

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

		keyBuf := new(bytes.Buffer)
		ce(sb.Copy(
			sb.Marshal(func() (Namespace, any) {
				return NamespaceKV, key
			}),
			sb.Encode(keyBuf),
		))
		bsKey := keyBuf.Bytes()

		valueBuf := new(bytes.Buffer)
		ce(sb.Copy(
			sb.Marshal(value),
			sb.Encode(valueBuf),
		))
		bsValue := valueBuf.Bytes()

		data, err := proto.Marshal(&SetProposal{
			Key:   bsKey,
			Value: bsValue,
		})
		ce(err)
		ce(node.Propose(wt.Ctx, data))

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
