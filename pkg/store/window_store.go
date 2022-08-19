package store

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"time"

	"4d63.com/optional"
)

type CoreWindowStore interface {
	StateStore
	Put(ctx context.Context, key commtypes.KeyT, value commtypes.ValueT, windowStartTimestamp int64) error
	Get(ctx context.Context, key commtypes.KeyT, windowStartTimestamp int64) (commtypes.ValueT, bool, error)
	Fetch(ctx context.Context, key commtypes.KeyT, timeFrom time.Time, timeTo time.Time,
		iterFunc func(int64 /* ts */, commtypes.KeyT, commtypes.ValueT) error) error
	FetchWithKeyRange(ctx context.Context, keyFrom commtypes.KeyT,
		keyTo commtypes.KeyT, timeFrom time.Time, timeTo time.Time,
		iterFunc func(int64, commtypes.KeyT, commtypes.ValueT) error) error
	FetchAll(ctx context.Context, timeFrom time.Time, timeTo time.Time,
		iterFunc func(int64, commtypes.KeyT, commtypes.ValueT) error) error
	IterAll(iterFunc func(int64, commtypes.KeyT, commtypes.ValueT) error) error
	TableType() TABLE_TYPE
	UpdateTrackParFunc
	OnlyUpdateInMemWinStore
}

type CoreWindowStoreG[K, V any] interface {
	StateStore
	Put(ctx context.Context, key K, value optional.Optional[V], windowStartTimestamp int64) error
	Get(ctx context.Context, key K, windowStartTimestamp int64) (V, bool, error)
	Fetch(ctx context.Context, key K, timeFrom time.Time, timeTo time.Time,
		iterFunc func(int64 /* ts */, K, V) error) error
	FetchWithKeyRange(ctx context.Context, keyFrom K,
		keyTo K, timeFrom time.Time, timeTo time.Time,
		iterFunc func(int64, K, V) error) error
	FetchAll(ctx context.Context, timeFrom time.Time, timeTo time.Time,
		iterFunc func(int64, K, V) error) error
	IterAll(iterFunc func(int64, K, V) error) error
	TableType() TABLE_TYPE
	UpdateTrackParFunc
}

type WindowStoreBackedByChangelog interface {
	CoreWindowStore
	WindowStoreOpWithChangelog
}

type WindowStoreBackedByChangelogG[K, V any] interface {
	CoreWindowStoreG[K, V]
	WindowStoreOpWithChangelogG[K, V]
}

type OnlyUpdateInMemWinStore interface {
	PutWithoutPushToChangelog(ctx context.Context,
		key commtypes.KeyT, value commtypes.ValueT) error
}

type OnlyUpdateInMemWinStoreG[K, V any] interface {
	PutWithoutPushToChangelog(ctx context.Context,
		key K, value V, windowStartTimestamp int64) error
}

type WindowStoreOpWithChangelog interface {
	StoreBackedByChangelog
	UpdateTrackParFunc
	OnlyUpdateInMemWinStore
	ProduceRangeRecording
	SubstreamNum() uint8
}

type WindowStoreOpWithChangelogG[K, V any] interface {
	StoreBackedByChangelog
	UpdateTrackParFunc
	OnlyUpdateInMemWinStoreG[K, V]
	ProduceRangeRecording
	SubstreamNum() uint8
}
