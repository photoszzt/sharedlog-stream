package store

import (
	"context"
	"sharedlog-stream/pkg/exactly_once_intr"
	"time"

	"4d63.com/optional"
)

type WindowStore[KeyT, ValT any] interface {
	CoreWindowStore[KeyT, ValT]
	// WindowStoreOpForExternalStore
	WindowStoreOpWithChangelog[KeyT, ValT]
}

type CoreWindowStore[KeyT, ValT any] interface {
	StateStore
	Put(ctx context.Context, key KeyT, value optional.Optional[ValT], windowStartTimestamp int64) error
	Get(ctx context.Context, key KeyT, windowStartTimestamp int64) (ValT, bool, error)
	Fetch(ctx context.Context, key KeyT, timeFrom time.Time, timeTo time.Time,
		iterFunc func(int64 /* ts */, KeyT, ValT) error) error
	FetchWithKeyRange(ctx context.Context, keyFrom KeyT,
		keyTo KeyT, timeFrom time.Time, timeTo time.Time,
		iterFunc func(int64, KeyT, ValT) error) error
	FetchAll(ctx context.Context, timeFrom time.Time, timeTo time.Time,
		iterFunc func(int64, KeyT, ValT) error) error
	IterAll(iterFunc func(int64, KeyT, ValT) error) error
	TableType() TABLE_TYPE
}

type WindowStoreBackedByChangelog[KeyT, ValT any] interface {
	CoreWindowStore[KeyT, ValT]
	WindowStoreOpWithChangelog[KeyT, ValT]
}

type WindowStoreOpWithChangelog[KeyT, ValT any] interface {
	SetTrackParFunc(trackParFunc exactly_once_intr.TrackProdSubStreamFunc[KeyT])
	PutWithoutPushToChangelog(ctx context.Context,
		key KeyT, value optional.Optional[ValT], windowStartTimestamp int64) error
}
