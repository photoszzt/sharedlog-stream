package store

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/exactly_once_intr"
	"sharedlog-stream/pkg/optional"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/txn_data"
	"time"
)

// If the entry is expired, it's
// not removed from the cache. It will try to insert again when it's evicted while
// the wrapped store might have already advanced. If the retension time is long, it
// would generate an entry in the changelog that is not needed.
type CachingWindowStoreG[K comparable, V any] struct {
	cache             *Cache[commtypes.KeyAndWindowStartTsG[K], V]
	wrappedStore      WindowStoreBackedByChangelogG[K, V]
	flushCallbackFunc func(ctx context.Context, msg commtypes.MessageG[commtypes.WindowedKeyG[K], commtypes.ChangeG[V]]) error
}

var _ = CoreWindowStoreG[int, int](&CachingWindowStoreG[int, int]{})
var _ = WindowStoreBackedByChangelogG[int, int](&CachingWindowStoreG[int, int]{})

func NewCachingWindowStoreG[K comparable, V any](ctx context.Context,
	windowSize int64,
	store WindowStoreBackedByChangelogG[K, V],
	sizeOfK func(commtypes.KeyAndWindowStartTsG[K]) int64,
	sizeOfV func(V) int64,
	maxCacheBytes int64,
) *CachingWindowStoreG[K, V] {
	c := &CachingWindowStoreG[K, V]{
		wrappedStore: store,
	}
	c.cache = NewCache(func(entries []LRUElement[commtypes.KeyAndWindowStartTsG[K], V]) error {
		for _, entry := range entries {
			newVal := entry.entry.value
			kTs := entry.key
			oldVal, ok, err := store.Get(ctx, kTs.Key, kTs.WindowStartTs)
			if err != nil {
				return err
			}
			if newVal.IsSome() || ok {
				var change commtypes.ChangeG[V]
				if ok {
					change = commtypes.ChangeG[V]{
						OldVal: optional.Some(oldVal),
						NewVal: newVal,
					}
				} else {
					change = commtypes.ChangeG[V]{
						OldVal: optional.None[V](),
						NewVal: newVal,
					}
				}
				err = store.Put(ctx, entry.key.Key, entry.entry.value, entry.key.WindowStartTs, entry.entry.tm)
				if err != nil {
					return err
				}
				t, err := commtypes.NewTimeWindow(kTs.WindowStartTs, kTs.WindowStartTs+windowSize)
				if err != nil {
					return err
				}
				k := commtypes.WindowedKeyG[K]{
					Key:    kTs.Key,
					Window: t,
				}
				err = c.flushCallbackFunc(ctx, commtypes.MessageG[commtypes.WindowedKeyG[K], commtypes.ChangeG[V]]{
					Key:           optional.Some(k),
					Value:         optional.Some(change),
					TimestampMs:   entry.entry.tm.RecordTsMs,
					StartProcTime: entry.entry.tm.StartProcTs,
				})
				if err != nil {
					return err
				}
			}
		}
		return nil
	}, sizeOfK, sizeOfV, maxCacheBytes)
	return c
}

func (c *CachingWindowStoreG[K, V]) Name() string { return c.wrappedStore.Name() }
func (c *CachingWindowStoreG[K, V]) Put(ctx context.Context, key K,
	value optional.Option[V], windowStartTimestamp int64, tm TimeMeta,
) error {
	return c.cache.PutMaybeEvict(commtypes.KeyAndWindowStartTsG[K]{Key: key, WindowStartTs: windowStartTimestamp},
		LRUEntry[V]{value: value, isDirty: true, tm: tm})
}
func (c *CachingWindowStoreG[K, V]) PutWithoutPushToChangelogG(ctx context.Context,
	key K, value optional.Option[V], windowStartTs int64,
) error {
	return c.wrappedStore.PutWithoutPushToChangelogG(ctx, key, value, windowStartTs)
}
func (c *CachingWindowStoreG[K, V]) PutWithoutPushToChangelog(ctx context.Context, key commtypes.KeyT, value commtypes.ValueT) error {
	keyTs := key.(commtypes.KeyAndWindowStartTsG[K])
	return c.wrappedStore.PutWithoutPushToChangelogG(ctx, keyTs.Key, optional.Some(value.(V)), keyTs.WindowStartTs)
}
func (c *CachingWindowStoreG[K, V]) Get(ctx context.Context, key K,
	windowStartTimestamp int64,
) (V, bool, error) {
	entry, found := c.cache.get(commtypes.KeyAndWindowStartTsG[K]{Key: key, WindowStartTs: windowStartTimestamp})
	if found {
		v, ok := entry.value.Take()
		return v, ok, nil
	} else {
		return c.wrappedStore.Get(ctx, key, windowStartTimestamp)
	}
}
func (c *CachingWindowStoreG[K, V]) Fetch(ctx context.Context, key K, timeFrom time.Time, timeTo time.Time,
	iterFunc func(int64 /* ts */, K, V) error,
) error {
	panic("not implemented")
}
func (c *CachingWindowStoreG[K, V]) FetchWithKeyRange(ctx context.Context, keyFrom K,
	keyTo K, timeFrom time.Time, timeTo time.Time,
	iterFunc func(int64, K, V) error,
) error {
	panic("not implemented")
}
func (c *CachingWindowStoreG[K, V]) FetchAll(ctx context.Context, timeFrom time.Time, timeTo time.Time,
	iterFunc func(int64, K, V) error,
) error {
	panic("not implemented")
}
func (c *CachingWindowStoreG[K, V]) IterAll(iterFunc func(int64, K, V) error) error {
	panic("not implemented")
}
func (c *CachingWindowStoreG[K, V]) Flush(ctx context.Context) (uint32, error) {
	c.cache.flush(nil)
	return c.wrappedStore.Flush(ctx)
}
func (c *CachingWindowStoreG[K, V]) TableType() TABLE_TYPE { return IN_MEM }
func (c *CachingWindowStoreG[K, V]) SetTrackParFunc(f exactly_once_intr.TrackProdSubStreamFunc) {
	c.wrappedStore.SetTrackParFunc(f)
}
func (s *CachingWindowStoreG[K, V]) ConsumeOneLogEntry(ctx context.Context, parNum uint8) (int, error) {
	return s.wrappedStore.ConsumeOneLogEntry(ctx, parNum)
}
func (s *CachingWindowStoreG[K, V]) ConfigureExactlyOnce(rem exactly_once_intr.ReadOnlyExactlyOnceManager,
	guarantee exactly_once_intr.GuaranteeMth,
) error {
	return s.wrappedStore.ConfigureExactlyOnce(rem, guarantee)
}

func (s *CachingWindowStoreG[K, V]) ChangelogTopicName() string {
	return s.wrappedStore.ChangelogTopicName()
}
func (s *CachingWindowStoreG[K, V]) GetInitialProdSeqNum() uint64 {
	return s.wrappedStore.GetInitialProdSeqNum()
}
func (s *CachingWindowStoreG[K, V]) ResetInitialProd()               { s.wrappedStore.ResetInitialProd() }
func (s *CachingWindowStoreG[K, V]) Stream() sharedlog_stream.Stream { return s.wrappedStore.Stream() }
func (s *CachingWindowStoreG[K, V]) SubstreamNum() uint8             { return s.wrappedStore.SubstreamNum() }
func (s *CachingWindowStoreG[K, V]) SetFlushCallback(f func(ctx context.Context, msg commtypes.MessageG[commtypes.WindowedKeyG[K], commtypes.ChangeG[V]]) error) {
	s.flushCallbackFunc = f
}
func (s *CachingWindowStoreG[K, V]) Snapshot(logOff uint64) {
	s.wrappedStore.Snapshot(logOff)
}
func (s *CachingWindowStoreG[K, V]) WaitForAllSnapshot() error {
	return s.wrappedStore.WaitForAllSnapshot()
}
func (s *CachingWindowStoreG[K, V]) SetWinSnapshotCallback(ctx context.Context, f WinSnapshotCallback[K, V]) {
	s.wrappedStore.SetWinSnapshotCallback(ctx, f)
}
func (s *CachingWindowStoreG[K, V]) RestoreFromSnapshot(ctx context.Context, snapshot [][]byte) error {
	return s.wrappedStore.RestoreFromSnapshot(ctx, snapshot)
}
func (s *CachingWindowStoreG[K, V]) SetKVSerde(serdeFormat commtypes.SerdeFormat,
	keySerde commtypes.SerdeG[commtypes.KeyAndWindowStartTsG[K]], valSerde commtypes.SerdeG[V],
) error {
	return nil
}

func (s *CachingWindowStoreG[K, V]) GetKVSerde() commtypes.SerdeG[commtypes.KeyValuePair[commtypes.KeyAndWindowStartTsG[K], V]] {
	return s.wrappedStore.GetKVSerde()
}

func (s *CachingWindowStoreG[K, V]) FindLastEpochMetaWithAuxData(ctx context.Context, parNum uint8) (auxData []byte, metaSeqNum uint64, err error) {
	return s.wrappedStore.FindLastEpochMetaWithAuxData(ctx, parNum)
}
func (s *CachingWindowStoreG[K, V]) BuildKeyMeta(kms map[string][]txn_data.KeyMaping) {
	s.wrappedStore.BuildKeyMeta(kms)
}
