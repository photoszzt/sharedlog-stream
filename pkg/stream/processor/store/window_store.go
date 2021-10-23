package store

import (
	"time"
)

type WindowStore interface {
	StateStore
	Init(ctx ProcessorContext)
	Put(key KeyT, value ValueT, windowStartTimestamp uint64) error
	Get(key KeyT, windowStartTimestamp uint64) (ValueT, bool, error)
	Fetch(key KeyT, timeFrom time.Time, timeTo time.Time, iterFunc func(uint64, ValueT)) error
	BackwardFetch(key KeyT, timeFrom time.Time, timeTo time.Time)
	FetchWithKeyRange(keyFrom KeyT, keyTo KeyT, timeFrom time.Time, timeTo time.Time)
	BackwardFetchWithKeyRange(keyFrom KeyT, keyTo KeyT, timeFrom time.Time, timeTo time.Time)
	FetchAll(timeFrom time.Time, timeTo time.Time)
	BackwardFetchAll(timeFrom time.Time, timeTo time.Time)
}
