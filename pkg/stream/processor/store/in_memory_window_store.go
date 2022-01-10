package store

import (
	"fmt"
	"sharedlog-stream/pkg/stream/processor/concurrent_skiplist"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

type InMemoryWindowStore struct {
	otrMu           sync.RWMutex
	openedTimeRange map[int64]struct{}

	sctx StoreContext
	// for val comparison
	comparable concurrent_skiplist.Comparable

	store              *concurrent_skiplist.SkipList
	name               string
	windowSize         int64
	retentionPeriod    int64
	observedStreamTime int64
	retainDuplicates   bool
	open               bool
}

func NewInMemoryWindowStore(name string, retentionPeriod int64, windowSize int64, retainDuplicates bool,
	compare concurrent_skiplist.Comparable,
) *InMemoryWindowStore {
	return &InMemoryWindowStore{
		name:               name,
		windowSize:         windowSize,
		retentionPeriod:    retentionPeriod,
		retainDuplicates:   retainDuplicates,
		open:               false,
		observedStreamTime: 0,
		openedTimeRange:    make(map[int64]struct{}),
		store: concurrent_skiplist.New(concurrent_skiplist.CompareFunc(func(lhs, rhs interface{}) int {
			l := lhs.(int64)
			r := rhs.(int64)
			if l < r {
				return -1
			} else if l == r {
				return 0
			} else {
				return 1
			}
		})),
		comparable: compare,
	}
}

func (st *InMemoryWindowStore) Init(ctx StoreContext) {
	st.sctx = ctx
	st.open = true
}

func (s *InMemoryWindowStore) Name() string {
	return s.name
}

func (s *InMemoryWindowStore) Put(key interface{}, value interface{}, windowStartTimestamp int64) error {
	s.removeExpiredSegments()
	if windowStartTimestamp > s.observedStreamTime {
		s.observedStreamTime = windowStartTimestamp
	}
	if windowStartTimestamp <= s.observedStreamTime-s.retentionPeriod {
		log.Warn().Msgf("Skipping record for expired segment.")
	} else {
		if value != nil {
			s.store.SetIfAbsent(windowStartTimestamp, concurrent_skiplist.New(s.comparable))
			s.store.Get(windowStartTimestamp).Value().(*concurrent_skiplist.SkipList).Set(key, value)
		} else {
			s.store.ComputeIfPresent(windowStartTimestamp, func(e *concurrent_skiplist.Element) {
				kvmap := e.Value().(*concurrent_skiplist.SkipList)
				kvmap.Remove(key)
			})
		}
	}
	return nil
}

func (s *InMemoryWindowStore) Get(key interface{}, windowStartTimestamp int64) (interface{}, bool) {
	s.removeExpiredSegments()
	if windowStartTimestamp <= s.observedStreamTime-s.retentionPeriod {
		return nil, false
	}

	e := s.store.Get(windowStartTimestamp)
	if e == nil {
		return nil, false
	} else {
		kvmap := e.Value().(*concurrent_skiplist.SkipList)
		v := kvmap.Get(key)
		if v == nil {
			return nil, false
		} else {
			return v.Value(), true
		}
	}
}

func (s *InMemoryWindowStore) Fetch(key interface{}, timeFrom time.Time, timeTo time.Time, iterFunc func(int64, ValueT) error) error {
	s.removeExpiredSegments()

	tsFrom := timeFrom.UnixMilli()
	tsTo := timeTo.UnixMilli()

	minTime := s.observedStreamTime - s.retentionPeriod + 1
	if minTime < tsFrom {
		minTime = tsFrom
	}

	if tsTo < minTime {
		return nil
	}

	err := s.store.IterateRange(tsFrom, tsTo, func(ts concurrent_skiplist.KeyT, val concurrent_skiplist.ValueT) error {
		curT := ts.(int64)
		v := val.(*concurrent_skiplist.SkipList)
		elem := v.Get(key)
		if elem != nil {
			s.otrMu.Lock()
			s.openedTimeRange[curT] = struct{}{}
			s.otrMu.Unlock()

			err := iterFunc(curT, elem.Value())
			if err != nil {
				return fmt.Errorf("iter func failed: %v", err)
			}

			s.otrMu.Lock()
			delete(s.openedTimeRange, curT)
			s.otrMu.Unlock()
		}
		return nil
	})
	return err
}

func (s *InMemoryWindowStore) BackwardFetch(key interface{}, timeFrom time.Time, timeTo time.Time) {
	panic("not implemented")
}

func (s *InMemoryWindowStore) FetchWithKeyRange(keyFrom interface{}, keyTo interface{}, timeFrom time.Time, timeTo time.Time) {
	panic("not implemented")
}

func (s *InMemoryWindowStore) BackwardFetchWithKeyRange(keyFrom interface{}, keyTo interface{}, timeFrom time.Time, timeTo time.Time) KeyValueIterator {
	panic("not implemented")
}

func (s *InMemoryWindowStore) FetchAll(timeFrom time.Time, timeTo time.Time) KeyValueIterator {
	panic("not implemented")
}

func (s *InMemoryWindowStore) BackwardFetchAll(timeFrom time.Time, timeTo time.Time) KeyValueIterator {
	panic("not implemented")
}

func (s *InMemoryWindowStore) removeExpiredSegments() {
	minLiveTimeTmp := int64(s.observedStreamTime) - int64(s.retentionPeriod) + 1
	minLiveTime := int64(0)

	if minLiveTimeTmp > 0 {
		minLiveTime = int64(minLiveTimeTmp)
	}

	s.otrMu.RLock()
	defer s.otrMu.RUnlock()
	for minT := range s.openedTimeRange {
		if minT < minLiveTime {
			minLiveTime = minT
		}
	}
	s.store.RemoveUntil(minLiveTime)
}
