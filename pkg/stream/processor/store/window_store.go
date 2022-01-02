package store

import (
	"context"
	"time"
)

type WindowStore interface {
	StateStore
	Init(ctx StoreContext)
	Put(ctx context.Context, key KeyT, value ValueT, windowStartTimestamp int64) error
	Get(key KeyT, windowStartTimestamp int64) (ValueT, bool, error)
	Fetch(key KeyT, timeFrom time.Time, timeTo time.Time, iterFunc func(int64, ValueT) error) error
	BackwardFetch(key KeyT, timeFrom time.Time, timeTo time.Time)
	FetchWithKeyRange(keyFrom KeyT, keyTo KeyT, timeFrom time.Time, timeTo time.Time)
	BackwardFetchWithKeyRange(keyFrom KeyT, keyTo KeyT, timeFrom time.Time, timeTo time.Time)
	FetchAll(timeFrom time.Time, timeTo time.Time)
	BackwardFetchAll(timeFrom time.Time, timeTo time.Time)
}
