package state

import "cs.utexas.edu/zhitingz/sharedlog-stream/pkg/stream/processor"

type SegmentedBytesStore interface {
	processor.StateStore
	Fetch(key []byte, from uint64, to uint64) Iterator
	BackwardFetch(key []byte, from uint64, to uint64) Iterator
	FetchWithKeyRange(keyFrom []byte, keyTo []byte, from uint64, to uint64) Iterator
	BackwardFetchWithKeyRange(keyFrom []byte, keyTo []byte, from uint64, to uint64) Iterator
	All() (Iterator, error)
	BackwardAll() (Iterator, error)
	Remove(key []byte)
	RemoveWithTs(key []byte, timestamp uint64)
	Put(key []byte, value []byte)
	Get(key []byte) []byte
}

type HasNextCondition interface {
	HasNext(iterator Iterator) bool
}

type HasNextConditionFunc func(Iterator) bool

func (fn HasNextConditionFunc) HasNext(iterator Iterator) bool {
	return fn(iterator)
}

type KeySchema interface {
	UpperRange(key []byte, to uint64) []byte
	LowerRange(key []byte, from uint64) []byte
	ToStoreBinaryKeyPrefix(key []byte, ts uint64) []byte
	UpperRangeFixedSize(key []byte, to uint64) []byte
	LowerRangeFixedSize(key []byte, from uint64) []byte
	SegmentTimestamp(key []byte) uint64
	HasNextCondition(binaryKeyFrom []byte, binaryKeyTo []byte, from uint64, to uint64) HasNextCondition
	// SegmentsToSearch()
}
