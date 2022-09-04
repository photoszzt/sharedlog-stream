package store

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/optional"
	"strings"
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
	Range(ctx context.Context, from optional.Option[K], to optional.Option[K],
		iterFunc func(K, V) error) error
	ApproximateNumEntries() (uint64, error)
	Put(ctx context.Context, key K, value optional.Option[V], currentStreamTime int64) error
	PutIfAbsent(ctx context.Context, key K, value V, currentStreamTime int64) (optional.Option[V], error)
	PutAll(context.Context, []*commtypes.Message) error
	Delete(ctx context.Context, key K) error
	TableType() TABLE_TYPE
	Flush(ctx context.Context) error
	Snapshot() [][]byte
	SetKVSerde(serdeFormat commtypes.SerdeFormat, keySerde commtypes.SerdeG[K], valSerde commtypes.SerdeG[V]) error
	UpdateTrackParFunc
	OnlyUpdateInMemStore
	CachedStateStore[K, V]
}

type OnlyUpdateInMemStore interface {
	PutWithoutPushToChangelog(ctx context.Context, key commtypes.KeyT, value commtypes.ValueT) error
}

type OnlyUpdateInMemStoreG[K, V any] interface {
	PutWithoutPushToChangelogG(ctx context.Context, key K, value V) error
}

type KeyValueStoreOpWithChangelog interface {
	StoreBackedByChangelog
	RestoreKVStore
	UpdateTrackParFunc
	ChangelogIsSrc() bool
	OnlyUpdateInMemStore
	ProduceRangeRecording
	SubstreamNum() uint8
	Snapshot() [][]byte
}

type KeyValueStoreBackedByChangelog interface {
	CoreKeyValueStore
	KeyValueStoreOpWithChangelog
}

type KeyValueStoreBackedByChangelogG[K, V any] interface {
	CoreKeyValueStoreG[K, V]
	KeyValueStoreOpWithChangelog
}

type LessFunc[K any] func(k1, k2 K) bool

func IntLessFunc(k1, k2 int) bool {
	return k1 < k2
}

func Uint64LessFunc(k1, k2 uint64) bool {
	return k1 < k2
}

func StringLessFunc(k1, k2 string) bool {
	return strings.Compare(k1, k2) < 0
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
