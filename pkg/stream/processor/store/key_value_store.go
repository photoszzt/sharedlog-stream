package store

import (
	"context"
	"sharedlog-stream/pkg/stream/processor/commtypes"
)

type KeyValueStore interface {
	StateStore
	Init(ctx StoreContext)
	Get(ctx context.Context, key KeyT) (ValueT, bool, error)
	Range(from KeyT, to KeyT, iterFunc func(KeyT, ValueT) error) error
	ReverseRange(from KeyT, to KeyT, iterFunc func(KeyT, ValueT) error) error
	PrefixScan(prefix interface{}, prefixKeyEncoder commtypes.Encoder, iterFunc func(KeyT, ValueT) error) error
	ApproximateNumEntries(ctx context.Context) (uint64, error)
	Put(ctx context.Context, key KeyT, value ValueT) error
	PutIfAbsent(ctx context.Context, key KeyT, value ValueT) (ValueT, error)
	PutAll(context.Context, []*commtypes.Message) error
	Delete(ctx context.Context, key KeyT) error
}

type Segment interface {
	KeyValueStore
	destroy()
	deleteRange(keyFrom interface{}, keyTo interface{})
}
