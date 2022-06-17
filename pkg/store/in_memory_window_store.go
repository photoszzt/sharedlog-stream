package store

import (
	"context"
	"fmt"
	"math"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/concurrent_skiplist"
	"sharedlog-stream/pkg/exactly_once_intr"
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

var _ = WindowStore(&InMemoryWindowStore{})

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

func (st *InMemoryWindowStore) IsOpen() bool {
	return true
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

func (s *InMemoryWindowStore) Put(ctx context.Context, key commtypes.KeyT, value commtypes.ValueT, windowStartTimestamp int64) error {
	s.removeExpiredSegments()
	if windowStartTimestamp > s.observedStreamTime {
		s.observedStreamTime = windowStartTimestamp
	}
	// debug.Fprintf(os.Stderr, "start ts: %d, observed: %d\n", windowStartTimestamp, s.observedStreamTime-s.retentionPeriod)
	if windowStartTimestamp <= s.observedStreamTime-s.retentionPeriod {
		log.Warn().Msgf("Skipping record for expired segment.")
	} else {
		if value != nil {
			s.updateSeqnumForDups()
			k := key
			if s.retainDuplicates {
				k = VersionedKey{
					Version: s.seqNum,
					Key:     k,
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

func (s *InMemoryWindowStore) PutWithoutPushToChangelog(ctx context.Context, key commtypes.KeyT, value commtypes.ValueT, windowStartTimestamp int64) error {
	return s.Put(ctx, key, value, windowStartTimestamp)
}

func (s *InMemoryWindowStore) Get(ctx context.Context, key commtypes.KeyT, windowStartTimestamp int64) (commtypes.ValueT, bool, error) {
	s.removeExpiredSegments()
	if windowStartTimestamp <= s.observedStreamTime-s.retentionPeriod {
		return nil, false, nil
	}

	e := s.store.Get(windowStartTimestamp)
	if e == nil {
		return nil, false, nil
	} else {
		kvmap := e.Value().(*concurrent_skiplist.SkipList)
		v := kvmap.Get(key)
		if v == nil {
			return nil, false, nil
		} else {
			return v.Value(), true, nil
		}
	}
}

func (s *InMemoryWindowStore) Fetch(
	ctx context.Context,
	key commtypes.KeyT,
	timeFrom time.Time,
	timeTo time.Time,
	iterFunc func(int64, commtypes.KeyT, commtypes.ValueT) error,
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
		keyFrom := VersionedKey{
			Key:     key,
			Version: 0,
		}
		keyTo := VersionedKey{
			Key:     key,
			Version: math.MaxUint32,
		}
		return s.fetchWithKeyRange(keyFrom, keyTo, tsFrom, tsTo, iterFunc)
	}
}

func (s *InMemoryWindowStore) BackwardFetch(
	key commtypes.KeyT,
	timeFrom time.Time,
	timeTo time.Time,
	iterFunc func(int64, commtypes.KeyT, commtypes.ValueT) error,
) error {
	panic("not implemented")
}

func (s *InMemoryWindowStore) FetchWithKeyRange(
	ctx context.Context,
	keyFrom commtypes.KeyT,
	keyTo commtypes.KeyT,
	timeFrom time.Time,
	timeTo time.Time,
	iterFunc func(int64, commtypes.KeyT, commtypes.ValueT) error,
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
	iterFunc func(int64, commtypes.KeyT, commtypes.ValueT) error,
) error {
	err := s.store.IterateRange(tsFrom, tsTo, func(ts concurrent_skiplist.KeyT, val concurrent_skiplist.ValueT) error {
		curT := ts.(int64)
		v := val.(*concurrent_skiplist.SkipList)
		s.otrMu.Lock()
		s.openedTimeRange[curT] = struct{}{}
		s.otrMu.Unlock()
		err := v.IterateRange(keyFrom, keyTo, func(kt concurrent_skiplist.KeyT, vt concurrent_skiplist.ValueT) error {
			k := kt
			if s.retainDuplicates {
				ktTmp := kt.(VersionedKey)
				k = ktTmp.Key
			}
			err := iterFunc(curT, k, vt)
			return err
		})
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

func (s *InMemoryWindowStore) IterAll(iterFunc func(int64, commtypes.KeyT, commtypes.ValueT) error) error {
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
	keyFrom commtypes.KeyT,
	keyTo commtypes.KeyT,
	timeFrom time.Time,
	timeTo time.Time,
	iterFunc func(int64, commtypes.KeyT, commtypes.ValueT) error,
) error {
	panic("not implemented")
}

func (s *InMemoryWindowStore) FetchAll(
	ctx context.Context,
	timeFrom time.Time,
	timeTo time.Time,
	iterFunc func(int64, commtypes.KeyT, commtypes.ValueT) error,
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
	err := s.store.IterateRange(tsFrom, tsTo, func(ts concurrent_skiplist.KeyT, val concurrent_skiplist.ValueT) error {
		curT := ts.(int64)
		v := val.(*concurrent_skiplist.SkipList)
		s.otrMu.Lock()
		s.openedTimeRange[curT] = struct{}{}
		s.otrMu.Unlock()
		for i := v.Front(); i != nil; i = i.Next() {
			k := i.Key()
			if s.retainDuplicates {
				ktTmp := i.Key().(VersionedKey)
				k = ktTmp.Key
			}
			err := iterFunc(curT, k, i.Value())
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

func (s *InMemoryWindowStore) BackwardFetchAll(
	timeFrom time.Time,
	timeTo time.Time,
	iterFunc func(int64, commtypes.KeyT, commtypes.ValueT) error,
) error {
	panic("not implemented")
}

func (s *InMemoryWindowStore) DropDatabase(ctx context.Context) error {
	panic("not implemented")
}

func (s *InMemoryWindowStore) TableType() TABLE_TYPE {
	return IN_MEM
}

func (s *InMemoryWindowStore) StartTransaction(ctx context.Context) error { return nil }
func (s *InMemoryWindowStore) CommitTransaction(ctx context.Context, taskRepr string, transactionID uint64) error {
	return nil
}
func (s *InMemoryWindowStore) AbortTransaction(ctx context.Context) error { return nil }
func (s *InMemoryWindowStore) GetTransactionID(ctx context.Context, taskRepr string) (uint64, bool, error) {
	panic("not supported")
}
func (s *InMemoryWindowStore) SetTrackParFunc(trackParFunc exactly_once_intr.TrackProdSubStreamFunc) {
}
