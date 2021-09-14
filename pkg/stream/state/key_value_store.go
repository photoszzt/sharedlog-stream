package state

import (
	"cs.utexas.edu/zhitingz/sharedlog-stream/pkg/stream/processor"
)

type KeyValueStore interface {
	processor.StateStore
	Get(key KeyT) (ValueT, bool)
	Range(from KeyT, to KeyT) KeyValueIterator
	ReverseRange(from KeyT, to KeyT) KeyValueIterator
	PrefixScan(prefix interface{}, prefixKeyEncoder processor.Encoder) KeyValueIterator
	ApproximateNumEntries() uint64
	Put(key KeyT, value ValueT)
	PutIfAbsent(key KeyT, value ValueT) ValueT
	PutAll([]*processor.Message)
	Delete(key KeyT)
}
