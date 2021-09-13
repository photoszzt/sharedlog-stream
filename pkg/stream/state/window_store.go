package state

import (
	"time"

	"cs.utexas.edu/zhitingz/sharedlog-stream/pkg/stream/processor"
)

type WindowStore interface {
	processor.StateStore
	Put(key interface{}, value interface{}, windowStartTimestamp uint64) error
	Fetch(key interface{}, timeFrom time.Time, timeTo time.Time) Iterator
	BackwardFetch(key interface{}, timeFrom time.Time, timeTo time.Time) Iterator
	FetchWithKeyRange(keyFrom interface{}, keyTo interface{}, timeFrom time.Time, timeTo time.Time) Iterator
	BackwardFetchWithKeyRange(keyFrom interface{}, keyTo interface{}, timeFrom time.Time, timeTo time.Time) Iterator
	FetchAll(timeFrom time.Time, timeTo time.Time) Iterator
	BackwardFetchAll(timeFrom time.Time, timeTo time.Time) Iterator
}
