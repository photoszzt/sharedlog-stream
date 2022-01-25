package store

import (
	"fmt"
	"math"
	"sharedlog-stream/pkg/concurrent_skiplist"
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

	seqNumMu sync.Mutex
	seqNum   uint32

	retainDuplicates bool
	open             bool
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

func (st *InMemoryWindowStore) updateSeqnumForDups() {
	if st.retainDuplicates {
		st.seqNumMu.Lock()
		defer st.seqNumMu.Unlock()
		if st.seqNum == math.MaxUint32 {
			st.seqNum = 0
		}
		st.seqNum += 1
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
			s.updateSeqnumForDups()
			k := key
			if s.retainDuplicates {
				k = versionedKey{
					version: s.seqNum,
					key:     k,
				}
			}
			s.store.SetIfAbsent(windowStartTimestamp, concurrent_skiplist.New(s.comparable))
			s.store.Get(windowStartTimestamp).Value().(*concurrent_skiplist.SkipList).Set(k, value)
		} else if !s.retainDuplicates {
			// Skip if value is null and duplicates are allowed since this delete is a no-op
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

func (s *InMemoryWindowStore) Fetch(
	key interface{},
	timeFrom time.Time,
	timeTo time.Time,
	iterFunc func(int64, KeyT, ValueT) error,
) error {
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

	if !s.retainDuplicates {
		err := s.store.IterateRange(tsFrom, tsTo, func(ts concurrent_skiplist.KeyT, val concurrent_skiplist.ValueT) error {
			curT := ts.(int64)
			v := val.(*concurrent_skiplist.SkipList)
			elem := v.Get(key)
			if elem != nil {
				s.otrMu.Lock()
				s.openedTimeRange[curT] = struct{}{}
				s.otrMu.Unlock()

				err := iterFunc(curT, elem.Key(), elem.Value())
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
	} else {
		keyFrom := versionedKey{
			key:     key,
			version: 0,
		}
		keyTo := versionedKey{
			key:     key,
			version: math.MaxUint32,
		}
		return s.fetchWithKeyRange(keyFrom, keyTo, tsFrom, tsTo, iterFunc)
	}
}

func (s *InMemoryWindowStore) BackwardFetch(
	key interface{},
	timeFrom time.Time,
	timeTo time.Time,
	iterFunc func(int64, ValueT) error,
) error {
	panic("not implemented")
}

func (s *InMemoryWindowStore) FetchWithKeyRange(
	keyFrom interface{},
	keyTo interface{},
	timeFrom time.Time,
	timeTo time.Time,
	iterFunc func(int64, KeyT, ValueT) error,
) error {
	s.removeExpiredSegments()

	tsFrom := timeFrom.UnixMilli()
	tsTo := timeTo.UnixMilli()

	if tsFrom > tsTo {
		return nil
	}

	minTime := s.observedStreamTime - s.retentionPeriod + 1
	if minTime < tsFrom {
		minTime = tsFrom
	}

	if tsTo < minTime {
		return nil
	}

	return s.fetchWithKeyRange(keyFrom, keyTo, tsFrom, tsTo, iterFunc)
}

func (s *InMemoryWindowStore) fetchWithKeyRange(
	keyFrom interface{},
	keyTo interface{},
	tsFrom int64,
	tsTo int64,
	iterFunc func(int64, KeyT, ValueT) error,
) error {
	err := s.store.IterateRange(tsFrom, tsTo, func(ts concurrent_skiplist.KeyT, val concurrent_skiplist.ValueT) error {
		curT := ts.(int64)
		v := val.(*concurrent_skiplist.SkipList)
		s.otrMu.Lock()
		s.openedTimeRange[curT] = struct{}{}
		s.otrMu.Unlock()
		v.IterateRange(keyFrom, keyTo, func(kt concurrent_skiplist.KeyT, vt concurrent_skiplist.ValueT) error {
			err := iterFunc(curT, kt, vt)
			return err
		})
		s.otrMu.Lock()
		delete(s.openedTimeRange, curT)
		s.otrMu.Unlock()
		return nil
	})
	return err
}

func (s *InMemoryWindowStore) IterAll(iterFunc func(int64, KeyT, ValueT) error) error {
	s.removeExpiredSegments()
	minTime := s.observedStreamTime - s.retentionPeriod

	err := s.store.IterFrom(minTime, func(kt concurrent_skiplist.KeyT, vt concurrent_skiplist.ValueT) error {
		curT := kt.(int64)
		v := vt.(*concurrent_skiplist.SkipList)
		s.otrMu.Lock()
		s.openedTimeRange[curT] = struct{}{}
		s.otrMu.Unlock()

		for i := v.Front(); i != nil; i = i.Next() {
			err := iterFunc(curT, i.Key(), i.Value())
			if err != nil {
				return err
			}
		}

		s.otrMu.Lock()
		delete(s.openedTimeRange, curT)
		s.otrMu.Unlock()
		return nil
	})
	return err
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

func (s *InMemoryWindowStore) BackwardFetchWithKeyRange(
	keyFrom interface{},
	keyTo interface{},
	timeFrom time.Time,
	timeTo time.Time,
	iterFunc func(int64, ValueT) error,
) error {
	panic("not implemented")
}

func (s *InMemoryWindowStore) FetchAll(
	timeFrom time.Time,
	timeTo time.Time,
	iterFunc func(int64, ValueT) error,
) error {
	panic("not implemented")
}

func (s *InMemoryWindowStore) BackwardFetchAll(
	timeFrom time.Time,
	timeTo time.Time,
	iterFunc func(int64, ValueT) error,
) error {
	panic("not implemented")
}
