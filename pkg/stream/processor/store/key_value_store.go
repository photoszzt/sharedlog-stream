package store

import (
	"context"
	"sharedlog-stream/pkg/stream/processor/commtypes"
)

type KeyValueStore interface {
	StateStore
	Init(ctx ProcessorContext)
	Get(key KeyT) (ValueT, bool, error)
	Range(from KeyT, to KeyT) KeyValueIterator
	ReverseRange(from KeyT, to KeyT) KeyValueIterator
	PrefixScan(prefix interface{}, prefixKeyEncoder commtypes.Encoder) KeyValueIterator
	ApproximateNumEntries() uint64
	Put(ctx context.Context, key KeyT, value ValueT) error
	PutIfAbsent(ctx context.Context, key KeyT, value ValueT) (ValueT, error)
	PutAll(context.Context, []*commtypes.Message) error
	Delete(ctx context.Context, key KeyT) error
}
