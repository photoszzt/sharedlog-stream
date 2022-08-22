package store

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/exactly_once_intr"
	"time"

	"4d63.com/optional"
)

type CachingWindowStoreG[K comparable, V any] struct {
	cache        *Cache[commtypes.KeyAndWindowStartTsG[K], V]
	wrappedStore CoreWindowStoreG[K, V]
}

func NewCachingWindowStoreG[K comparable, V any](ctx context.Context,
	store CoreWindowStoreG[K, V],
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
