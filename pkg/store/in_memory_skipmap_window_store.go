package store

import (
	"context"
	"fmt"
	"math"
	"os"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/exactly_once_intr"
	"sync"
	"sync/atomic"
	"time"

	"4d63.com/optional"
	"github.com/rs/zerolog/log"
	"github.com/zhangyunhao116/skipmap"
)

type InMemorySkipMapWindowStoreG[K, V any] struct {
	mux                         sync.RWMutex
	storeNoDup                  *skipmap.Int64Map[*skipmap.FuncMap[K, V]]
	storeWithDup                *skipmap.Int64Map[*skipmap.FuncMap[VersionedKeyG[K], V]]
	compareFunc                 CompareFuncG[K]
	compareFuncWithVersionedKey CompareFuncG[VersionedKeyG[K]]
	name                        string
	windowSize                  int64
	retentionPeriod             int64
	observedStreamTime          int64 // protected by mux
	seqNum                      uint32
	retainDuplicates            bool
}

var _ = CoreWindowStoreG[int, int](&InMemorySkipMapWindowStoreG[int, int]{})

func NewInMemorySkipMapWindowStore[K, V any](name string, retentionPeriod int64,
	windowSize int64, retainDuplicates bool, compareFunc CompareFuncG[K],
) *InMemorySkipMapWindowStoreG[K, V] {
	if retainDuplicates {
		return &InMemorySkipMapWindowStoreG[K, V]{
			name:               name,
			windowSize:         windowSize,
			retentionPeriod:    retentionPeriod,
			retainDuplicates:   retainDuplicates,
			observedStreamTime: 0,
			storeWithDup:       skipmap.NewInt64[*skipmap.FuncMap[VersionedKeyG[K], V]](),
			storeNoDup:         nil,
			compareFuncWithVersionedKey: func(lhs, rhs VersionedKeyG[K]) int {
				return CompareWithVersionedKey(lhs, rhs, compareFunc)
			},
		}
	} else {
		return &InMemorySkipMapWindowStoreG[K, V]{
			name:               name,
			windowSize:         windowSize,
			retentionPeriod:    retentionPeriod,
			retainDuplicates:   retainDuplicates,
			observedStreamTime: 0,
			storeNoDup:         skipmap.NewInt64[*skipmap.FuncMap[K, V]](),
			storeWithDup:       nil,
			compareFunc:        compareFunc,
		}
	}
}

func (s *InMemorySkipMapWindowStoreG[K, V]) Name() string { return s.name }
func (s *InMemorySkipMapWindowStoreG[K, V]) Put(ctx context.Context, key K, value optional.Optional[V], windowStartTimestamp int64) error {
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
		val, exists := value.Get()
		if exists {
			updateSeqnumForDups(s.retainDuplicates, &s.seqNum)
			if s.retainDuplicates {
				k := VersionedKeyG[K]{
					Version: atomic.LoadUint32(&s.seqNum),
					Key:     key,
				}
				actual, _ := s.storeWithDup.LoadOrStore(windowStartTimestamp, skipmap.NewFunc[VersionedKeyG[K], V](
					func(a, b VersionedKeyG[K]) bool {
						return s.compareFuncWithVersionedKey(a, b) < 0
					}))
				actual.Store(k, val)
			} else {
				actual, _ := s.storeNoDup.LoadOrStore(windowStartTimestamp, skipmap.NewFunc[K, V](func(a, b K) bool {
					return s.compareFunc(a, b) < 0
				}))
				actual.Store(key, val)
			}
		} else if !s.retainDuplicates {
			// Skip if value is null and duplicates are allowed since this delete is a no-op
			v, ok := s.storeNoDup.Load(windowStartTimestamp)
			if ok {
				v.Delete(key)
			}
		}
	}
	return nil
}

// Get is used in aggregates; it doesn't expects duplicates
func (s *InMemorySkipMapWindowStoreG[K, V]) Get(ctx context.Context, key K, windowStartTimestamp int64) (V, bool, error) {
	var v V
	s.removeExpiredSegments()
	s.mux.RLock()
	expired := windowStartTimestamp <= s.observedStreamTime-s.retentionPeriod
	s.mux.RUnlock()
	if expired {
		return v, false, nil
	}
	if s.retainDuplicates {
		panic("Get is not supported for window stores with duplicates")
	} else {
		kvmap, ok := s.storeNoDup.Load(windowStartTimestamp)
		if !ok {
			return v, false, nil
		}
		v, exists := kvmap.Load(key)
		return v, exists, nil
	}
}
func (s *InMemorySkipMapWindowStoreG[K, V]) PutWithoutPushToChangelog(ctx context.Context, key commtypes.KeyT, value commtypes.ValueT) error {
	panic("not implement")
}
func (s *InMemorySkipMapWindowStoreG[K, V]) Fetch(ctx context.Context, key K, timeFrom time.Time, timeTo time.Time,
	iterFunc func(int64 /* ts */, K, V) error,
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

	if s.retainDuplicates {
		keyFrom := VersionedKeyG[K]{
			Version: 0,
			Key:     key,
		}
		keyTo := VersionedKeyG[K]{
			Version: math.MaxUint32,
			Key:     key,
		}
		s.fetchWithKeyRangeWithDuplicates(ctx, keyFrom, keyTo, tsFrom, tsTo, iterFunc)
	} else {
		s.storeNoDup.RangeFrom(tsFrom, func(ts int64, kvmap *skipmap.FuncMap[K, V]) bool {
			if ts > tsTo {
				return false
			} else if ts < tsFrom {
				return true
			}
			v, exists := kvmap.Load(key)
			if exists {
				err := iterFunc(ts, key, v)
				if err != nil {
					return false
				}
			}
			return true
		})
	}
	return nil
}
func (s *InMemorySkipMapWindowStoreG[K, V]) FetchWithKeyRange(ctx context.Context, keyFrom K,
	keyTo K, timeFrom time.Time, timeTo time.Time,
	iterFunc func(int64, K, V) error,
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

	if s.retainDuplicates {
		kFrom := VersionedKeyG[K]{
			Version: 0,
			Key:     keyFrom,
		}
		kTo := VersionedKeyG[K]{
			Version: math.MaxUint32,
			Key:     keyTo,
		}
		s.fetchWithKeyRangeWithDuplicates(ctx, kFrom, kTo, tsFrom, tsTo, iterFunc)
	} else {
		s.storeNoDup.RangeFrom(tsFrom, func(ts int64, kvmap *skipmap.FuncMap[K, V]) bool {
			if ts > tsTo {
				return false
			} else if ts < tsFrom {
				return true
			}
			kvmap.RangeFrom(keyFrom, func(k K, v V) bool {
				if s.compareFunc(k, keyTo) <= 0 {
					err := iterFunc(ts, k, v)
					if err != nil {
						return false
					}
					return true
				} else {
					return false
				}
			})
			return true
		})
	}
	return nil
}

func (s *InMemorySkipMapWindowStoreG[K, V]) fetchWithKeyRangeWithDuplicates(ctx context.Context,
	keyFrom VersionedKeyG[K], keyTo VersionedKeyG[K], timeFrom int64, timeTo int64,
	iterFunc func(int64, K, V) error,
) error {
	fmt.Fprintf(os.Stderr, "keyFrom: %+v, keyTo: %+v, timeFrom: %v, timeTo: %v\n",
		keyFrom, keyTo, timeFrom, timeTo)
	s.storeWithDup.RangeFrom(timeFrom, func(ts int64, kvmap *skipmap.FuncMap[VersionedKeyG[K], V]) bool {
		if ts > timeTo {
			return false
		} else if ts < timeFrom {
			return true
		}
		fmt.Fprintf(os.Stderr, "current ts: %v\n", ts)
		kvmap.RangeFrom(keyFrom, func(k VersionedKeyG[K], v V) bool {
			fmt.Fprintf(os.Stderr, "current key: %+v\n", k)
			if s.compareFuncWithVersionedKey(k, keyTo) <= 0 {
				err := iterFunc(ts, k.Key, v)
				if err != nil {
					return false
				}
				return true
			} else {
				return false
			}
		})
		return true
	})
	return nil
}

func (s *InMemorySkipMapWindowStoreG[K, V]) FetchAll(ctx context.Context, timeFrom time.Time, timeTo time.Time,
	iterFunc func(int64, K, V) error,
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
	if s.retainDuplicates {
		s.storeWithDup.RangeFrom(tsFrom, func(ts int64, kvmap *skipmap.FuncMap[VersionedKeyG[K], V]) bool {
			if ts < tsFrom {
				return true
			} else if ts > tsTo {
				return false
			} else {
				kvmap.Range(func(k VersionedKeyG[K], v V) bool {
					err := iterFunc(ts, k.Key, v)
					if err != nil {
						return false
					}
					return true
				})
				return true
			}
		})
	} else {
		s.storeNoDup.RangeFrom(tsFrom, func(ts int64, kvmap *skipmap.FuncMap[K, V]) bool {
			if ts < tsFrom {
				return true
			} else if ts > tsTo {
				return false
			} else {
				kvmap.Range(func(k K, v V) bool {
					err := iterFunc(ts, k, v)
					if err != nil {
						return false
					}
					return true
				})
				return true
			}
		})
	}
	return nil
}

func (s *InMemorySkipMapWindowStoreG[K, V]) IterAll(iterFunc func(int64, K, V) error) error {
	s.removeExpiredSegments()
	s.mux.RLock()
	minTime := s.observedStreamTime - s.retentionPeriod
	s.mux.RUnlock()
	if s.retainDuplicates {
		s.storeWithDup.RangeFrom(minTime, func(ts int64, kvmap *skipmap.FuncMap[VersionedKeyG[K], V]) bool {
			if ts < minTime {
				return true
			}
			kvmap.Range(func(k VersionedKeyG[K], v V) bool {
				err := iterFunc(ts, k.Key, v)
				if err != nil {
					return false
				}
				return true
			})
			return true
		})
	} else {
		s.storeNoDup.RangeFrom(minTime, func(ts int64, kvmap *skipmap.FuncMap[K, V]) bool {
			if ts < minTime {
				return true
			}
			kvmap.Range(func(k K, v V) bool {
				err := iterFunc(ts, k, v)
				if err != nil {
					return false
				}
				return true
			})
			return true
		})
	}
	return nil
}

func (s *InMemorySkipMapWindowStoreG[K, V]) TableType() TABLE_TYPE { return IN_MEM }
func (s *InMemorySkipMapWindowStoreG[K, V]) SetTrackParFunc(trackParFunc exactly_once_intr.TrackProdSubStreamFunc) {
}
func (s *InMemorySkipMapWindowStoreG[K, V]) FlushChangelog(ctx context.Context) error {
	panic("not supported")
}
func (s *InMemorySkipMapWindowStoreG[K, V]) ConsumeChangelog(ctx context.Context, parNum uint8) (*commtypes.MsgAndSeqs, error) {
	panic("not supported")
}

func (s *InMemorySkipMapWindowStoreG[K, V]) ConfigureExactlyOnce(rem exactly_once_intr.ReadOnlyExactlyOnceManager, guarantee exactly_once_intr.GuaranteeMth, serdeFormat commtypes.SerdeFormat) error {
	panic("not supported")
}

func (s *InMemorySkipMapWindowStoreG[K, V]) ChangelogTopicName() string {
	panic("not supported")
}

func (s *InMemorySkipMapWindowStoreG[K, V]) removeExpiredSegments() {
	s.mux.RLock()
	minLiveTimeTmp := int64(s.observedStreamTime) - int64(s.retentionPeriod) + 1
	s.mux.RUnlock()
	minLiveTime := int64(0)

	if minLiveTimeTmp > 0 {
		minLiveTime = int64(minLiveTimeTmp)
	}

	if s.retainDuplicates {
		s.storeWithDup.Range(func(ts int64, _ *skipmap.FuncMap[VersionedKeyG[K], V]) bool {
			if ts < minLiveTime {
				s.storeWithDup.Delete(ts)
				return true
			} else {
				return false
			}
		})
	} else {
		s.storeNoDup.Range(func(ts int64, _ *skipmap.FuncMap[K, V]) bool {
			if ts < minLiveTime {
				s.storeNoDup.Delete(ts)
				return true
			} else {
				return false
			}
		})
	}
}