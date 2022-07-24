package store

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"time"
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

type WindowStoreBackedByChangelog interface {
	CoreWindowStore
	WindowStoreOpWithChangelog
}

type OnlyUpdateInMemWinStore interface {
	PutWithoutPushToChangelog(ctx context.Context,
		key commtypes.KeyT, value commtypes.ValueT, windowStartTimestamp int64) error
}

type WindowStoreOpWithChangelog interface {
	StoreBackedByChangelog
	UpdateTrackParFunc
	OnlyUpdateInMemWinStore
}
