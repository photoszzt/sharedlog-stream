package store

import (
	"context"
	"sharedlog-stream/pkg/commtypes"

	"4d63.com/optional"
)

type CoreKeyValueStore interface {
	StateStore
	Get(ctx context.Context, key commtypes.KeyT) (commtypes.ValueT, bool, error)
	Range(ctx context.Context, from commtypes.KeyT, to commtypes.KeyT,
		iterFunc func(commtypes.KeyT, commtypes.ValueT) error) error
	ApproximateNumEntries() (uint64, error)
	Put(ctx context.Context, key commtypes.KeyT, value commtypes.ValueT) error
	PutIfAbsent(ctx context.Context, key commtypes.KeyT, value commtypes.ValueT) (commtypes.ValueT, error)
	PutAll(context.Context, []*commtypes.Message) error
	Delete(ctx context.Context, key commtypes.KeyT) error
	TableType() TABLE_TYPE
	UpdateTrackParFunc
	OnlyUpdateInMemStore
}

type CoreKeyValueStoreG[K, V any] interface {
	StateStore
	Get(ctx context.Context, key K) (V, bool, error)
	Range(ctx context.Context, from K, to K,
		iterFunc func(K, V) error) error
	ApproximateNumEntries() (uint64, error)
	Put(ctx context.Context, key K, value V) error
	PutIfAbsent(ctx context.Context, key K, value V) (optional.Optional[V], error)
	PutAll(context.Context, []*commtypes.Message) error
	Delete(ctx context.Context, key K) error
	TableType() TABLE_TYPE
	UpdateTrackParFunc
	OnlyUpdateInMemStoreG[K, V]
}

type OnlyUpdateInMemStore interface {
	PutWithoutPushToChangelog(ctx context.Context, key commtypes.KeyT, value commtypes.ValueT) error
}

type OnlyUpdateInMemStoreG[K, V any] interface {
	PutWithoutPushToChangelog(ctx context.Context, key K, value V) error
}

type KeyValueStoreOpWithChangelog interface {
	StoreBackedByChangelog
	UpdateTrackParFunc
	ChangelogIsSrc() bool
	OnlyUpdateInMemStore
	ProduceRangeRecording
	SubstreamNum() uint8
}

type KeyValueStoreOpWithChangelogG[K, V any] interface {
	StoreBackedByChangelog
	UpdateTrackParFunc
	ChangelogIsSrc() bool
	OnlyUpdateInMemStoreG[K, V]
	ProduceRangeRecording
	SubstreamNum() uint8
}

type KeyValueStoreBackedByChangelog interface {
	CoreKeyValueStore
	KeyValueStoreOpWithChangelog
}

type KeyValueStoreBackedByChangelogG[K, V any] interface {
	CoreKeyValueStoreG[K, V]
	KeyValueStoreOpWithChangelogG[K, V]
}

type Segment interface {
	StateStore
	Get(ctx context.Context, key []byte) ([]byte, bool, error)
	Range(ctx context.Context, from []byte, to []byte,
		iterFunc func([]byte, []byte) error) error
	ReverseRange(from []byte, to []byte, iterFunc func([]byte, []byte) error) error
	PrefixScan(prefix interface{}, prefixKeyEncoder commtypes.Encoder, iterFunc func([]byte, []byte) error) error
	ApproximateNumEntries(ctx context.Context) (uint64, error)
	Put(ctx context.Context, key []byte, value []byte) error
	PutIfAbsent(ctx context.Context, key []byte, value []byte) ([]byte, error)
	PutAll(context.Context, []*commtypes.Message) error
	Delete(ctx context.Context, key []byte) error

	Destroy(ctx context.Context) error
	DeleteRange(ctx context.Context, keyFrom interface{}, keyTo interface{}) error
}
