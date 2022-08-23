package store

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/exactly_once_intr"
	"sharedlog-stream/pkg/sharedlog_stream"
	"time"

	"4d63.com/optional"
)

// If the entry is expired, it's
// not removed from the cache. It will try to insert again when it's evicted while
// the wrapped store might have already advanced. If the retension time is long, it
// would generate an entry in the changelog that is not needed.
type CachingWindowStoreG[K comparable, V any] struct {
	cache        *Cache[commtypes.KeyAndWindowStartTsG[K], V]
	wrappedStore WindowStoreBackedByChangelogG[K, V]
}

var _ = CoreWindowStoreG[int, int](&CachingWindowStoreG[int, int]{})
var _ = WindowStoreBackedByChangelogG[int, int](&CachingWindowStoreG[int, int]{})

func NewCachingWindowStoreG[K comparable, V any](ctx context.Context,
	store WindowStoreBackedByChangelogG[K, V],
	sizeOfK func(commtypes.KeyAndWindowStartTsG[K]) int64,
	sizeOfV func(V) int64,
	maxCacheBytes int64,
) *CachingWindowStoreG[K, V] {
	return &CachingWindowStoreG[K, V]{
		cache: NewCache(func(entries []LRUElement[commtypes.KeyAndWindowStartTsG[K], V]) error {
			for _, entry := range entries {
				err := store.Put(ctx, entry.key.Key, entry.entry.value, entry.key.WindowStartTs)
				if err != nil {
					return err
				}
			}
			return nil
		}, sizeOfK, sizeOfV, maxCacheBytes),
		wrappedStore: store,
	}
}

func (c *CachingWindowStoreG[K, V]) Name() string { return c.wrappedStore.Name() }
func (c *CachingWindowStoreG[K, V]) Put(ctx context.Context, key K,
	value optional.Optional[V], windowStartTimestamp int64,
) error {
	err := c.cache.PutMaybeEvict(commtypes.KeyAndWindowStartTsG[K]{Key: key, WindowStartTs: windowStartTimestamp},
		LRUEntry[V]{value: value, isDirty: true})
	if err != nil {
		return err
	}
	err = c.wrappedStore.PutWithoutPushToChangelogG(ctx, key, value, windowStartTimestamp)
	return err
}
func (c *CachingWindowStoreG[K, V]) PutWithoutPushToChangelogG(ctx context.Context,
	key K, value optional.Optional[V], windowStartTs int64,
) error {
	return c.wrappedStore.PutWithoutPushToChangelogG(ctx, key, value, windowStartTs)
}
func (c *CachingWindowStoreG[K, V]) PutWithoutPushToChangelog(ctx context.Context, key commtypes.KeyT, value commtypes.ValueT) error {
	keyTs := key.(commtypes.KeyAndWindowStartTsG[K])
	return c.wrappedStore.PutWithoutPushToChangelogG(ctx, keyTs.Key, optional.Of(value.(V)), keyTs.WindowStartTs)
}
func (c *CachingWindowStoreG[K, V]) Get(ctx context.Context, key K,
	windowStartTimestamp int64,
) (V, bool, error) {
	entry, found := c.cache.get(commtypes.KeyAndWindowStartTsG[K]{Key: key, WindowStartTs: windowStartTimestamp})
	if found {
		v, ok := entry.value.Get()
		return v, ok, nil
	} else {
		return c.wrappedStore.Get(ctx, key, windowStartTimestamp)
	}
}
func (c *CachingWindowStoreG[K, V]) Fetch(ctx context.Context, key K, timeFrom time.Time, timeTo time.Time,
	iterFunc func(int64 /* ts */, K, V) error,
) error {
	return c.wrappedStore.Fetch(ctx, key, timeFrom, timeTo, iterFunc)
}
func (c *CachingWindowStoreG[K, V]) FetchWithKeyRange(ctx context.Context, keyFrom K,
	keyTo K, timeFrom time.Time, timeTo time.Time,
	iterFunc func(int64, K, V) error,
) error {
	return c.wrappedStore.FetchWithKeyRange(ctx, keyFrom, keyTo, timeFrom, timeTo, iterFunc)
}
func (c *CachingWindowStoreG[K, V]) FetchAll(ctx context.Context, timeFrom time.Time, timeTo time.Time,
	iterFunc func(int64, K, V) error,
) error {
	return c.wrappedStore.FetchAll(ctx, timeFrom, timeTo, iterFunc)
}
func (c *CachingWindowStoreG[K, V]) IterAll(iterFunc func(int64, K, V) error) error {
	return c.wrappedStore.IterAll(iterFunc)
}
func (c *CachingWindowStoreG[K, V]) Flush(ctx context.Context) error {
	c.cache.flush(nil)
	return c.wrappedStore.Flush(ctx)
}
func (c *CachingWindowStoreG[K, V]) TableType() TABLE_TYPE { return IN_MEM }
func (c *CachingWindowStoreG[K, V]) SetTrackParFunc(f exactly_once_intr.TrackProdSubStreamFunc) {
	c.wrappedStore.SetTrackParFunc(f)
}
func (s *CachingWindowStoreG[K, V]) ConsumeChangelog(ctx context.Context, parNum uint8) (*commtypes.MsgAndSeqs, error) {
	return s.wrappedStore.ConsumeChangelog(ctx, parNum)
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
func (s *CachingWindowStoreG[K, V]) GetCurrentProdSeqNum() uint64 {
	return s.wrappedStore.GetCurrentProdSeqNum()
}
func (s *CachingWindowStoreG[K, V]) ResetInitialProd()               { s.wrappedStore.ResetInitialProd() }
func (s *CachingWindowStoreG[K, V]) Stream() sharedlog_stream.Stream { return s.wrappedStore.Stream() }
func (s *CachingWindowStoreG[K, V]) SubstreamNum() uint8             { return s.wrappedStore.SubstreamNum() }
