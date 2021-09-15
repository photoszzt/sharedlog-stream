package processor

import (
	"time"
)

type WindowStore interface {
	StateStore
	Put(key KeyT, value ValueT, windowStartTimestamp uint64) error
	Get(key KeyT, windowStartTimestamp uint64) (ValueT, bool)
	Fetch(key KeyT, timeFrom time.Time, timeTo time.Time) KeyValueIterator
	BackwardFetch(key KeyT, timeFrom time.Time, timeTo time.Time) KeyValueIterator
	FetchWithKeyRange(keyFrom KeyT, keyTo KeyT, timeFrom time.Time, timeTo time.Time) KeyValueIterator
	BackwardFetchWithKeyRange(keyFrom KeyT, keyTo KeyT, timeFrom time.Time, timeTo time.Time) KeyValueIterator
	FetchAll(timeFrom time.Time, timeTo time.Time) KeyValueIterator
	BackwardFetchAll(timeFrom time.Time, timeTo time.Time) KeyValueIterator
}
