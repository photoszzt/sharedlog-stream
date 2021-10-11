package processor

import (
	"bytes"
	"sharedlog-stream/pkg/stream/processor/concurrent_skiplist"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

type InMemoryBytesWindowStore struct {
	name       string
	windowSize uint64
	sctx       ProcessorContext

	retentionPeriod    uint64
	retainDuplicates   bool
	open               bool
	store              *concurrent_skiplist.SkipList
	observedStreamTime uint64

	otrMu           sync.RWMutex
	openedTimeRange map[uint64]struct{}

	valSerde Serde
}

func NewInMemoryBytesWindowStore(name string, retentionPeriod uint64, windowSize uint64, retainDuplicates bool, valSerde Serde) *InMemoryBytesWindowStore {
	return &InMemoryBytesWindowStore{
		name:               name,
		windowSize:         windowSize,
		retentionPeriod:    retentionPeriod,
		retainDuplicates:   retainDuplicates,
		open:               false,
		observedStreamTime: 0,
		store: concurrent_skiplist.New(concurrent_skiplist.CompareFunc(func(lhs, rhs interface{}) int {
			l := lhs.(uint64)
			r := rhs.(uint64)
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

func (st *InMemoryBytesWindowStore) Init(ctx ProcessorContext) {
	st.sctx = ctx
	st.open = true
}

func (s *InMemoryBytesWindowStore) Name() string {
	return s.name
}

func (s *InMemoryBytesWindowStore) Put(key []byte, value []byte, windowStartTimestamp uint64) error {
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

func (s *InMemoryBytesWindowStore) Get(key []byte, windowStartTimestamp uint64) ([]byte, bool) {
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

func (s *InMemoryBytesWindowStore) Fetch(key []byte, timeFrom time.Time, timeTo time.Time, iterFunc func(uint64, ValueT)) error {
	s.removeExpiredSegments()

	tsFrom := timeFrom.Unix() * 1000
	tsTo := timeTo.Unix() * 1000

	minTime := int64(s.observedStreamTime) - int64(s.retentionPeriod) + 1
	if minTime < tsFrom {
		minTime = tsFrom
	}

	if tsTo < minTime {
		return nil
	}

	err := s.store.IterateRange(uint64(tsFrom), uint64(tsTo), func(ts concurrent_skiplist.KeyT, val concurrent_skiplist.ValueT) error {
		curT := ts.(uint64)
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
		iterFunc(curT, realVal)

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
	minLiveTime := uint64(0)

	if minLiveTimeTmp > 0 {
		minLiveTime = uint64(minLiveTimeTmp)
	}

	s.otrMu.RLock()
	defer s.otrMu.RUnlock()
	for minT, _ := range s.openedTimeRange {
		if minT < minLiveTime {
			minLiveTime = minT
		}
	}
	s.store.RemoveUntil(minLiveTime)
}
