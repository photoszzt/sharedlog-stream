package state

import (
	"time"

	"cs.utexas.edu/zhitingz/sharedlog-stream/pkg/stream/processor"
)

type InMemoryWindowStore struct {
	name             string
	windowSize       uint64
	sctx             processor.StateStoreContext
	retentionPeriod  uint64
	retainDuplicates bool
	open             bool
}

func NewInMemoryWindowStore(name string, retentionPeriod uint64, windowSize uint64, retainDuplicates bool) *InMemoryWindowStore {
	return &InMemoryWindowStore{
		name:             name,
		windowSize:       windowSize,
		retentionPeriod:  retentionPeriod,
		retainDuplicates: retainDuplicates,
		open:             false,
	}
}

func (s *InMemoryWindowStore) Name() string {
	return s.name
}

func (s *InMemoryWindowStore) Init(ctx processor.StateStoreContext, root processor.StateStore) {
	s.sctx = ctx
	s.open = true
}

func (s *InMemoryWindowStore) Put(key []byte, value ValueT, windowStartTimestamp uint64) error {
	panic("not implemented")
}

func (s *InMemoryWindowStore) Get(key []byte, windowStartTimestamp uint64) ValueT {
	panic("not implemented")
}

func (s *InMemoryWindowStore) Fetch(key []byte, timeFrom time.Time, timeTo time.Time) KeyValueIterator {
	panic("not implemented")
}

func (s *InMemoryWindowStore) BackwardFetch(key []byte, timeFrom time.Time, timeTo time.Time) KeyValueIterator {
	panic("not implemented")
}

func (s *InMemoryWindowStore) FetchWithKeyRange(keyFrom []byte, keyTo []byte, timeFrom time.Time, timeTo time.Time) KeyValueIterator {
	panic("not implemented")
}

func (s *InMemoryWindowStore) BackwardFetchWithKeyRange(keyFrom []byte, keyTo []byte, timeFrom time.Time, timeTo time.Time) KeyValueIterator {
	panic("not implemented")
}

func (s *InMemoryWindowStore) FetchAll(timeFrom time.Time, timeTo time.Time) KeyValueIterator {
	panic("not implemented")
}

func (s *InMemoryWindowStore) BackwardFetchAll(timeFrom time.Time, timeTo time.Time) KeyValueIterator {
	panic("not implemented")
}
