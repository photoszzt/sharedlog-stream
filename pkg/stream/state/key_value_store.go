package state

import "cs.utexas.edu/zhitingz/sharedlog-stream/pkg/stream/processor"

type KeyValueStore interface {
	processor.StateStore
	Get(key interface{}) (interface{}, bool)
	Range(from interface{}, to interface{}) Iterator
	ReverseRange(from interface{}, to interface{}) Iterator
	PrefixScan(prefix interface{}, prefixKeyEncoder processor.Encoder) Iterator
	ApproximateNumEntries() uint64
	Put(key interface{}, value interface{})
	PutIfAbsent(key interface{}, value interface{}) interface{}
	PutAll([]*processor.Message)
	Delete(key interface{})
}
