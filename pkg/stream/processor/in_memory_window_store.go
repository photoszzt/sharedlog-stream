package processor

import (
	"time"
)

type InMemoryWindowStore struct {
	name             string
	windowSize       uint64
	sctx             StateStoreContext
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

func (s *InMemoryWindowStore) Init(ctx StateStoreContext, root StateStore) {
	s.sctx = ctx
	s.open = true
}

func (s *InMemoryWindowStore) Put(key KeyT, value ValueT, windowStartTimestamp uint64) error {
	panic("not implemented")
}

func (s *InMemoryWindowStore) Get(key KeyT, windowStartTimestamp uint64) ValueT {
	panic("not implemented")
}

func (s *InMemoryWindowStore) Fetch(key KeyT, timeFrom time.Time, timeTo time.Time) KeyValueIterator {
	panic("not implemented")
}

func (s *InMemoryWindowStore) BackwardFetch(key KeyT, timeFrom time.Time, timeTo time.Time) KeyValueIterator {
	panic("not implemented")
}

func (s *InMemoryWindowStore) FetchWithKeyRange(keyFrom KeyT, keyTo KeyT, timeFrom time.Time, timeTo time.Time) KeyValueIterator {
	panic("not implemented")
}

func (s *InMemoryWindowStore) BackwardFetchWithKeyRange(keyFrom KeyT, keyTo KeyT, timeFrom time.Time, timeTo time.Time) KeyValueIterator {
	panic("not implemented")
}

func (s *InMemoryWindowStore) FetchAll(timeFrom time.Time, timeTo time.Time) KeyValueIterator {
	panic("not implemented")
}

func (s *InMemoryWindowStore) BackwardFetchAll(timeFrom time.Time, timeTo time.Time) KeyValueIterator {
	panic("not implemented")
}
