package store

import (
	"context"
	"fmt"
	"math"
	"sharedlog-stream/pkg/concurrent_skiplist"
	"sharedlog-stream/pkg/exactly_once_intr"
	"sharedlog-stream/pkg/utils"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
)

type InMemoryWindowStore[KeyT, ValT any] struct {
	// for val comparison
	comparable concurrent_skiplist.Comparable

	store              *concurrent_skiplist.SkipList
	name               string
	windowSize         int64
	retentionPeriod    int64
	observedStreamTime int64

	seqNum uint32

	retainDuplicates bool
	open             bool
}

var _ = WindowStore[int, string](&InMemoryWindowStore[int, string]{})

func NewInMemoryWindowStore[KeyT, ValT any](name string, retentionPeriod int64, windowSize int64, retainDuplicates bool,
	compare concurrent_skiplist.Comparable,
) *InMemoryWindowStore[KeyT, ValT] {
	return &InMemoryWindowStore[KeyT, ValT]{
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
		comparable: compare,
	}
}

func (st InMemoryWindowStore[KeyT, ValT]) IsOpen() bool {
	return true
}

func (st InMemoryWindowStore[KeyT, ValT]) updateSeqnumForDups() {
	if st.retainDuplicates {
		atomic.CompareAndSwapUint32(&st.seqNum, math.MaxUint32, 0)
		atomic.AddUint32(&st.seqNum, 1)
	}
}

func (s InMemoryWindowStore[KeyT, ValT]) Name() string {
	return s.name
}

func (s InMemoryWindowStore[KeyT, ValT]) Put(ctx context.Context, key KeyT, value ValT, windowStartTimestamp int64) error {
	s.removeExpiredSegments()
	if windowStartTimestamp > s.observedStreamTime {
		s.observedStreamTime = windowStartTimestamp
	}
	// debug.Fprintf(os.Stderr, "start ts: %d, observed: %d\n", windowStartTimestamp, s.observedStreamTime-s.retentionPeriod)
	if windowStartTimestamp <= s.observedStreamTime-s.retentionPeriod {
		log.Warn().Msgf("Skipping record for expired segment.")
	} else {
		if !utils.IsNil(value) {
			s.updateSeqnumForDups()
			k := key
			if s.retainDuplicates {
				k = VersionedKey{
					Version: atomic.LoadUint32(&s.seqNum),
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

func (s InMemoryWindowStore[KeyT, ValT]) PutWithoutPushToChangelog(ctx context.Context, key KeyT, value ValT, windowStartTimestamp int64) error {
	return s.Put(ctx, key, value, windowStartTimestamp)
}

func (s InMemoryWindowStore[KeyT, ValT]) Get(ctx context.Context, key KeyT, windowStartTimestamp int64) (ValT, bool, error) {
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
			return v.Value().(ValT), true, nil
		}
	}
}

func (s InMemoryWindowStore[KeyT, ValT]) Fetch(
	ctx context.Context,
	key KeyT,
	timeFrom time.Time,
	timeTo time.Time,
	iterFunc func(int64, KeyT, ValT) error,
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
				err := iterFunc(curT, elem.Key(), elem.Value())
				if err != nil {
					return fmt.Errorf("iter func failed: %v", err)
				}
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

func (s InMemoryWindowStore[KeyT, ValT]) BackwardFetch(
	key KeyT,
	timeFrom time.Time,
	timeTo time.Time,
	iterFunc func(int64, KeyT, ValT) error,
) error {
	panic("not implemented")
}

func (s InMemoryWindowStore[KeyT, ValT]) FetchWithKeyRange(
	ctx context.Context,
	keyFrom KeyT,
	keyTo KeyT,
	timeFrom time.Time,
	timeTo time.Time,
	iterFunc func(int64, KeyT, ValT) error,
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

func (s InMemoryWindowStore[KeyT, ValT]) fetchWithKeyRange(
	keyFrom interface{},
	keyTo interface{},
	tsFrom int64,
	tsTo int64,
	iterFunc func(int64, KeyT, ValT) error,
) error {
	err := s.store.IterateRange(tsFrom, tsTo, func(ts concurrent_skiplist.KeyT, val concurrent_skiplist.ValueT) error {
		curT := ts.(int64)
		v := val.(*concurrent_skiplist.SkipList)
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
		return nil
	})
	return err
}

func (s InMemoryWindowStore[KeyT, ValT]) IterAll(iterFunc func(int64, KeyT, ValT) error) error {
	s.removeExpiredSegments()
	minTime := s.observedStreamTime - s.retentionPeriod

	err := s.store.IterFrom(minTime, func(kt concurrent_skiplist.KeyT, vt concurrent_skiplist.ValueT) error {
		curT := kt.(int64)
		v := vt.(*concurrent_skiplist.SkipList)
		for i := v.Front(); i != nil; i = i.Next() {
			err := iterFunc(curT, i.Key(), i.Value())
			if err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

func (s InMemoryWindowStore[KeyT, ValT]) removeExpiredSegments() {
	minLiveTimeTmp := int64(s.observedStreamTime) - int64(s.retentionPeriod) + 1
	minLiveTime := int64(0)

	if minLiveTimeTmp > 0 {
		minLiveTime = int64(minLiveTimeTmp)
	}

	s.store.RemoveUntil(minLiveTime)
}

func (s InMemoryWindowStore[KeyT, ValT]) BackwardFetchWithKeyRange(
	keyFrom KeyT,
	keyTo KeyT,
	timeFrom time.Time,
	timeTo time.Time,
	iterFunc func(int64, KeyT, ValT) error,
) error {
	panic("not implemented")
}

func (s InMemoryWindowStore[KeyT, ValT]) FetchAll(
	ctx context.Context,
	timeFrom time.Time,
	timeTo time.Time,
	iterFunc func(int64, KeyT, ValT) error,
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
		return nil
	})
	return err
}

func (s InMemoryWindowStore[KeyT, ValT]) BackwardFetchAll(
	timeFrom time.Time,
	timeTo time.Time,
	iterFunc func(int64, KeyT, ValT) error,
) error {
	panic("not implemented")
}

func (s InMemoryWindowStore[KeyT, ValT]) DropDatabase(ctx context.Context) error {
	panic("not implemented")
}

func (s InMemoryWindowStore[KeyT, ValT]) TableType() TABLE_TYPE {
	return IN_MEM
}

func (s InMemoryWindowStore[KeyT, ValT]) StartTransaction(ctx context.Context) error { return nil }
func (s InMemoryWindowStore[KeyT, ValT]) CommitTransaction(ctx context.Context, taskRepr string, transactionID uint64) error {
	return nil
}
func (s InMemoryWindowStore[KeyT, ValT]) AbortTransaction(ctx context.Context) error { return nil }
func (s InMemoryWindowStore[KeyT, ValT]) GetTransactionID(ctx context.Context, taskRepr string) (uint64, bool, error) {
	panic("not supported")
}
func (s InMemoryWindowStore[KeyT, ValT]) SetTrackParFunc(trackParFunc exactly_once_intr.TrackProdSubStreamFunc) {
}
