package store

import "sharedlog-stream/pkg/stream/processor/commtypes"

type KeyValueStore interface {
	StateStore
	Init(ctx ProcessorContext)
	Get(key KeyT) (ValueT, bool, error)
	Range(from KeyT, to KeyT) KeyValueIterator
	ReverseRange(from KeyT, to KeyT) KeyValueIterator
	PrefixScan(prefix interface{}, prefixKeyEncoder commtypes.Encoder) KeyValueIterator
	ApproximateNumEntries() uint64
	Put(key KeyT, value ValueT) error
	PutIfAbsent(key KeyT, value ValueT) (ValueT, error)
	PutAll([]*commtypes.Message) error
	Delete(key KeyT) error
}
