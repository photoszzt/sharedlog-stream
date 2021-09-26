package processor

import (
	"sharedlog-stream/pkg/stream/processor/concurrent_skiplist"
	"time"
)

type InMemoryBytesWindowStore struct {
	name               string
	windowSize         uint64
	sctx               ProcessorContext
	retentionPeriod    uint64
	retainDuplicates   bool
	open               bool
	store              *concurrent_skiplist.SkipList
	comparable         concurrent_skiplist.Comparable
	observedStreamTime uint64
	openedTimeRange    map[uint64]struct{}
}

func NewInMemoryWindowStore(name string, retentionPeriod uint64, windowSize uint64, retainDuplicates bool) *InMemoryBytesWindowStore {
	return &InMemoryBytesWindowStore{
		name:               name,
		windowSize:         windowSize,
		retentionPeriod:    retentionPeriod,
		retainDuplicates:   retainDuplicates,
		open:               false,
		observedStreamTime: 0,
	}
}

func (s *InMemoryBytesWindowStore) Name() string {
	return s.name
}

func (s *InMemoryBytesWindowStore) Init(ctx ProcessorContext, root WindowStore) {
	s.sctx = ctx
	s.open = true
	s.sctx.RegisterWindowStore(root)
}

func (s *InMemoryBytesWindowStore) Put(key []byte, value []byte, windowStartTimestamp uint64) error {
	panic("not implemented")
}

func (s *InMemoryBytesWindowStore) Get(key []byte, windowStartTimestamp uint64) []byte {
	panic("not implemented")
}

func (s *InMemoryBytesWindowStore) Fetch(key []byte, timeFrom time.Time, timeTo time.Time, fetFunc func([]byte, []byte)) {
	panic("not implemented")
}

func (s *InMemoryBytesWindowStore) BackwardFetch(key []byte, timeFrom time.Time, timeTo time.Time) {
	panic("not implemented")
}

func (s *InMemoryBytesWindowStore) FetchWithKeyRange(keyFrom []byte, keyTo []byte, timeFrom time.Time, timeTo time.Time) {
	panic("not implemented")
}

func (s *InMemoryBytesWindowStore) BackwardFetchWithKeyRange(keyFrom []byte, keyTo []byte, timeFrom time.Time, timeTo time.Time) KeyValueIterator {
	panic("not implemented")
}

func (s *InMemoryBytesWindowStore) FetchAll(timeFrom time.Time, timeTo time.Time) KeyValueIterator {
	panic("not implemented")
}

func (s *InMemoryBytesWindowStore) BackwardFetchAll(timeFrom time.Time, timeTo time.Time) KeyValueIterator {
	panic("not implemented")
}
