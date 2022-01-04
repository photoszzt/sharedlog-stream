package store

import (
	"bytes"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/concurrent_skiplist"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

type InMemoryBytesWindowStore struct {
	otrMu           sync.RWMutex
	openedTimeRange map[int64]struct{}

	sctx     StoreContext
	valSerde commtypes.Serde

	store              *concurrent_skiplist.SkipList
	name               string
	windowSize         int64
	retentionPeriod    int64
	observedStreamTime int64
	retainDuplicates   bool
	open               bool
}

func NewInMemoryBytesWindowStore(name string, retentionPeriod int64, windowSize int64, retainDuplicates bool, valSerde commtypes.Serde) *InMemoryBytesWindowStore {
	return &InMemoryBytesWindowStore{
		name:               name,
		windowSize:         windowSize,
		retentionPeriod:    retentionPeriod,
		retainDuplicates:   retainDuplicates,
		open:               false,
		observedStreamTime: 0,
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
	}
}

func (st *InMemoryBytesWindowStore) Init(ctx StoreContext) {
	st.sctx = ctx
	st.open = true
}

func (s *InMemoryBytesWindowStore) Name() string {
	return s.name
}

func (s *InMemoryBytesWindowStore) Put(key []byte, value []byte, windowStartTimestamp int64) error {
	s.removeExpiredSegments()
	if windowStartTimestamp > s.observedStreamTime {
		s.observedStreamTime = windowStartTimestamp
	}
	if windowStartTimestamp <= s.observedStreamTime-s.retentionPeriod {
		log.Warn().Msgf("Skipping record for expired segment.")
	} else {
		if value != nil {
			s.store.SetIfAbsent(windowStartTimestamp,
				concurrent_skiplist.New(concurrent_skiplist.CompareFunc(func(lhs, rhs interface{}) int {
					l := lhs.([]byte)
					r := rhs.([]byte)
					return bytes.Compare(l, r)
				})))
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

func (s *InMemoryBytesWindowStore) Get(key []byte, windowStartTimestamp int64) ([]byte, bool) {
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
			return v.Value().([]byte), true
		}
	}
}

func (s *InMemoryBytesWindowStore) Fetch(key []byte, timeFrom time.Time, timeTo time.Time, iterFunc func(int64, ValueT) error) error {
	s.removeExpiredSegments()

	tsFrom := timeFrom.Unix() * 1000
	tsTo := timeTo.Unix() * 1000

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
		s.otrMu.Lock()
		s.openedTimeRange[curT] = struct{}{}
		s.otrMu.Unlock()

		valInByte := elem.Value().([]byte)
		realVal, err := s.valSerde.Decode(valInByte)
		if err != nil {
			return err
		}
		err = iterFunc(curT, realVal)
		if err != nil {
			return err
		}

		s.otrMu.Lock()
		delete(s.openedTimeRange, curT)
		s.otrMu.Unlock()
		return nil
	})
	return err
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

func (s *InMemoryBytesWindowStore) removeExpiredSegments() {
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
