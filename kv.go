package main

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/reusee/e4"
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
	wt NodeWaitTree,
	reading Reading,
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

		data := make([]byte, 8)
		binary.LittleEndian.PutUint64(data, uint64(rand.Int63()))
		ce(node.ReadIndex(wt.Ctx, data))
		rKey := *(*[8]byte)(data)

		ready := make(chan struct{})
		reading.Store(rKey, ready)
		select {
		case <-ready:
		case <-time.After(time.Second * 8):
			return we(ErrTimeout)
		}

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
