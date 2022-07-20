package store

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/exactly_once_intr"
)

type KeyValueStore[KeyT, ValT any] interface {
	CoreKeyValueStore[KeyT, ValT]
	// KeyValueStoreOpForExternalStore
	KeyValueStoreOpWithChangelog[KeyT, ValT]
}

type CoreKeyValueStore[KeyT, ValT any] interface {
	StateStore
	Get(ctx context.Context, key KeyT) (ValT, bool, error)
	Range(ctx context.Context, from KeyT, to KeyT,
		iterFunc func(KeyT, ValT) error) error
	ApproximateNumEntries() (uint64, error)
	Put(ctx context.Context, key KeyT, value ValT) error
	PutIfAbsent(ctx context.Context, key KeyT, value ValT) (ValT, error)
	PutAll(context.Context, []*commtypes.Message[KeyT, ValT]) error
	Delete(ctx context.Context, key KeyT) error
	TableType() TABLE_TYPE
}

type KeyValueStoreOpWithChangelog[KeyT, ValT any] interface {
	SetTrackParFunc(exactly_once_intr.TrackProdSubStreamFunc)
	PutWithoutPushToChangelog(ctx context.Context, key KeyT, value ValT) error
}

type KeyValueStoreBackedByChangelog[KeyT, ValT any] interface {
	CoreKeyValueStore[KeyT, ValT]
	KeyValueStoreOpWithChangelog[KeyT, ValT]
}

type Segment[KeyT, ValT any] interface {
	StateStore
	Get(ctx context.Context, key []byte) ([]byte, bool, error)
	Range(ctx context.Context, from []byte, to []byte,
		iterFunc func([]byte, []byte) error) error
	ApproximateNumEntries(ctx context.Context) (uint64, error)
	Put(ctx context.Context, key []byte, value []byte) error
	PutIfAbsent(ctx context.Context, key []byte, value []byte) ([]byte, error)
	PutAll(context.Context, []*commtypes.Message[KeyT, ValT]) error
	Delete(ctx context.Context, key []byte) error

	Destroy(ctx context.Context) error
	DeleteRange(ctx context.Context, keyFrom interface{}, keyTo interface{}) error
}
