package store

import "context"

type SegmentedBytesStore interface {
	StateStore

	// Fetch all records from the segmented store with the provided key and time range
	// from all existing segments
	Fetch(key []byte, from int64, to int64, iterFunc func(int64 /* ts */, KeyT, ValueT) error) error

	// Fetch all records from the segmented store with the provided key and time range
	// from all existing segments in backward order (from latest to earliest)
	BackwardFetch(key []byte, from int64, to int64, iterFunc func(int64 /* ts */, KeyT, ValueT) error) error
	FetchWithKeyRange(keyFrom []byte, keyTo []byte, from int64, to int64,
		iterFunc func(int64 /* ts */, KeyT, ValueT) error) error
	BackwardFetchWithKeyRange(keyFrom []byte, keyTo []byte, from int64, to int64,
		iterFunc func(int64 /* ts */, KeyT, ValueT) error) error
	FetchAll(iterFunc func(int64 /* ts */, KeyT, ValueT) error) error
	BackwardFetchAll(iterFunc func(int64 /* ts */, KeyT, ValueT) error) error
	Remove(ctx context.Context, key []byte) error
	RemoveWithTs(key []byte, timestamp uint64)
	Put(ctx context.Context, key []byte, value []byte) error
	Get(ctx context.Context, key []byte) (ValueT, bool, error)
}

type KeySchema interface {
	// Given a range of record keys and a time, construct a Segmented key that represents
	// the upper range of keys to search when performing range queries.
	UpperRange(key []byte, to int64) []byte
	LowerRange(key []byte, from int64) []byte
	ToStoreBinaryKeyPrefix(key []byte, ts int64) ([]byte, error)
	UpperRangeFixedSize(key []byte, to int64) []byte
	LowerRangeFixedSize(key []byte, from int64) []byte
	SegmentTimestamp(key []byte) int64
	HasNextCondition(curKey []byte, binaryKeyFrom []byte, binaryKeyTo []byte,
		from int64, to int64) (bool, int64)
	ExtractStoreKeyBytes(key []byte) []byte
	// SegmentsToSearch()
}
