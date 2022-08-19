package store

import (
	"context"
	"fmt"
	"math"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/concurrent_skiplist"
	"sharedlog-stream/pkg/exactly_once_intr"
	"sharedlog-stream/pkg/utils"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
)

type InMemoryWindowStore struct {
	// for val comparison
	comparable         concurrent_skiplist.Comparable
	comparableForDup   concurrent_skiplist.Comparable
	store              *concurrent_skiplist.SkipList
	name               string
	windowSize         int64
	retentionPeriod    int64
	observedStreamTime int64 // protected by mux
	mux                sync.RWMutex

	seqNum           uint32
	retainDuplicates bool
}

var _ = CoreWindowStore(&InMemoryWindowStore{})

func NewInMemoryWindowStore(name string, retentionPeriod int64, windowSize int64, retainDuplicates bool,
	compare CompareFunc,
) *InMemoryWindowStore {
	return &InMemoryWindowStore{
		name:               name,
		windowSize:         windowSize,
		retentionPeriod:    retentionPeriod,
		retainDuplicates:   retainDuplicates,
		observedStreamTime: 0,
		store:              concurrent_skiplist.New(CompareFunc(Int64IntrCompare)),
		comparable:         CompareFunc(compare),
		comparableForDup: CompareFunc(func(lhs, rhs interface{}) int {
			return CompareIntrWithVersionedKey(lhs, rhs, compare)
		}),
	}
}

func updateSeqnumForDups(retainDuplicates bool, seqNum *uint32) {
	if retainDuplicates {
		atomic.CompareAndSwapUint32(seqNum, math.MaxUint32, 0)
		atomic.AddUint32(seqNum, 1)
	}
}

func (s *InMemoryWindowStore) Name() string {
	return s.name
}

func (s *InMemoryWindowStore) Put(ctx context.Context, key commtypes.KeyT, value commtypes.ValueT, windowStartTimestamp int64) error {
	s.removeExpiredSegments()
	s.mux.Lock()
	if windowStartTimestamp > s.observedStreamTime {
		s.observedStreamTime = windowStartTimestamp
	}
	expired := windowStartTimestamp <= s.observedStreamTime-s.retentionPeriod
	s.mux.Unlock()
	// debug.Fprintf(os.Stderr, "start ts: %d, observed: %d\n", windowStartTimestamp, s.observedStreamTime-s.retentionPeriod)
	if expired {
		log.Warn().Msgf("Skipping record for expired segment.")
	} else {
		if !utils.IsNil(value) {
			updateSeqnumForDups(s.retainDuplicates, &s.seqNum)
			k := key
			cmp := s.comparable
			if s.retainDuplicates {
				k = VersionedKey{
					Version: atomic.LoadUint32(&s.seqNum),
					Key:     k,
				}
				cmp = s.comparableForDup
			}
			s.store.SetIfAbsent(windowStartTimestamp, concurrent_skiplist.New(cmp))
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

func (s *InMemoryWindowStore) PutWithoutPushToChangelog(ctx context.Context, key commtypes.KeyT, value commtypes.ValueT) error {
	panic("not implemented")
}

func (s *InMemoryWindowStore) Get(ctx context.Context, key commtypes.KeyT, windowStartTimestamp int64) (commtypes.ValueT, bool, error) {
	s.removeExpiredSegments()
	s.mux.RLock()
	expired := windowStartTimestamp <= s.observedStreamTime-s.retentionPeriod
	s.mux.RUnlock()
	if expired {
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

	s.mux.RLock()
	minTime := s.observedStreamTime - s.retentionPeriod + 1
	s.mux.RUnlock()
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

	s.mux.RLock()
	minTime := s.observedStreamTime - s.retentionPeriod + 1
	s.mux.RUnlock()
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

func (s *InMemoryWindowStore) IterAll(iterFunc func(int64, commtypes.KeyT, commtypes.ValueT) error) error {
	s.removeExpiredSegments()
	s.mux.RLock()
	minTime := s.observedStreamTime - s.retentionPeriod
	s.mux.RUnlock()

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

func (s *InMemoryWindowStore) removeExpiredSegments() {
	s.mux.RLock()
	minLiveTimeTmp := int64(s.observedStreamTime) - int64(s.retentionPeriod) + 1
	s.mux.RUnlock()
	minLiveTime := int64(0)

	if minLiveTimeTmp > 0 {
		minLiveTime = int64(minLiveTimeTmp)
	}

	s.store.RemoveUntil(minLiveTime)
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

	s.mux.RLock()
	minTime := s.observedStreamTime - s.retentionPeriod + 1
	s.mux.RUnlock()
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

func (s *InMemoryWindowStore) TableType() TABLE_TYPE {
	return IN_MEM
}

func (s *InMemoryWindowStore) SetTrackParFunc(trackParFunc exactly_once_intr.TrackProdSubStreamFunc) {
}
func (s *InMemoryWindowStore) FlushChangelog(ctx context.Context) error {
	panic("not supported")
}
func (s *InMemoryWindowStore) ConsumeChangelog(ctx context.Context, parNum uint8) (*commtypes.MsgAndSeqs, error) {
	panic("not supported")
}

func (s *InMemoryWindowStore) ConfigureExactlyOnce(rem exactly_once_intr.ReadOnlyExactlyOnceManager, guarantee exactly_once_intr.GuaranteeMth, serdeFormat commtypes.SerdeFormat) error {
	panic("not supported")
}

func (s *InMemoryWindowStore) ChangelogTopicName() string {
	panic("not supported")
}
