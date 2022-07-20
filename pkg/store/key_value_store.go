package store

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/exactly_once_intr"
)

type KeyValueStore[K, V any] interface {
	CoreKeyValueStore[K, V]
	// KeyValueStoreOpForExternalStore
	KeyValueStoreOpWithChangelog[K, V]
}

type CoreKeyValueStore[K, V any] interface {
	StateStore
	Get(ctx context.Context, key K) (V, bool, error)
	Range(ctx context.Context, from K, to K,
		iterFunc func(K, V) error) error
	ReverseRange(from K, to K, iterFunc func(K, V) error) error
	ApproximateNumEntries() (uint64, error)
	Put(ctx context.Context, key K, value V) error
	PutIfAbsent(ctx context.Context, key K, value V) (V, error)
	PutAll(context.Context, []*commtypes.Message[K, V]) error
	Delete(ctx context.Context, key K) error
	TableType() TABLE_TYPE
}

/*
type KeyValueStoreOpForExternalStore interface {
	StartTransaction(ctx context.Context) error
	CommitTransaction(ctx context.Context, taskRepr string, transactionID uint64) error
	AbortTransaction(ctx context.Context) error
	GetTransactionID(ctx context.Context, taskRepr string) (uint64, bool, error)
}
*/

type KeyValueStoreOpWithChangelog[K, V any] interface {
	SetTrackParFunc(exactly_once_intr.TrackProdSubStreamFunc[K])
	PutWithoutPushToChangelog(ctx context.Context, key K, value V) error
}

type KeyValueStoreBackedByChangelog[K, V any] interface {
	CoreKeyValueStore[K, V]
	KeyValueStoreOpWithChangelog[K, V]
}

type Segment interface {
	StateStore
	Get(ctx context.Context, key []byte) ([]byte, bool, error)
	Range(ctx context.Context, from []byte, to []byte,
		iterFunc func([]byte, []byte) error) error
	ReverseRange(from []byte, to []byte, iterFunc func([]byte, []byte) error) error
	ApproximateNumEntries(ctx context.Context) (uint64, error)
	Put(ctx context.Context, key []byte, value []byte) error
	PutIfAbsent(ctx context.Context, key []byte, value []byte) ([]byte, error)
	PutAll(context.Context, []*commtypes.Message[[]byte, []byte]) error
	Delete(ctx context.Context, key []byte) error

	Destroy(ctx context.Context) error
	DeleteRange(ctx context.Context, keyFrom interface{}, keyTo interface{}) error
}
