package store

import (
	"context"
	"time"
)

type WindowStore interface {
	StateStore
	Init(ctx StoreContext)
	Put(ctx context.Context, key KeyT, value ValueT, windowStartTimestamp int64) error
	Get(key KeyT, windowStartTimestamp int64) (ValueT, bool)
	Fetch(key KeyT, timeFrom time.Time, timeTo time.Time, iterFunc func(int64 /* ts */, KeyT, ValueT) error) error
	BackwardFetch(key KeyT, timeFrom time.Time, timeTo time.Time, iterFunc func(int64, KeyT, ValueT) error) error
	FetchWithKeyRange(keyFrom KeyT, keyTo KeyT, timeFrom time.Time, timeTo time.Time, iterFunc func(int64, KeyT, ValueT) error) error
	BackwardFetchWithKeyRange(keyFrom KeyT, keyTo KeyT, timeFrom time.Time, timeTo time.Time, iterFunc func(int64, KeyT, ValueT) error) error
	FetchAll(timeFrom time.Time, timeTo time.Time, iterFunc func(int64, KeyT, ValueT) error) error
	BackwardFetchAll(timeFrom time.Time, timeTo time.Time, iterFunc func(int64, KeyT, ValueT) error) error
}
