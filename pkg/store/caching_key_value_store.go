package store

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/exactly_once_intr"

	"4d63.com/optional"
)

type CachingKeyValueStoreG[K comparable, V any] struct {
	cache        *Cache[K, V]
	wrappedStore CoreKeyValueStoreG[K, V]
	name         string
}

var _ CoreKeyValueStoreG[int, int] = (*CachingKeyValueStoreG[int, int])(nil)

func NewCachingKeyValueStoreG[K comparable, V any](ctx context.Context, name string,
	store CoreKeyValueStoreG[K, V],
	sizeOfK func(K) int64,
	sizeOfV func(V) int64,
	maxCacheBytes int64,
) *CachingKeyValueStoreG[K, V] {
	return &CachingKeyValueStoreG[K, V]{
		name: name,
		cache: NewCache(func(entries []LRUElement[K, V]) error {
			for _, entry := range entries {
				err := store.Put(ctx, entry.key, entry.entry.value)
				if err != nil {
					return err
				}
			}
			return nil
		}, sizeOfK, sizeOfV, maxCacheBytes),
		wrappedStore: store,
	}
}

func (c *CachingKeyValueStoreG[K, V]) Name() string { return c.name }
func (c *CachingKeyValueStoreG[K, V]) Get(ctx context.Context, key K) (V, bool, error) {
	entry, found := c.cache.get(key)
	if found {
		v, ok := entry.value.Get()
		return v, ok, nil
	} else {
		return c.wrappedStore.Get(ctx, key)
	}
}
func (c *CachingKeyValueStoreG[K, V]) Range(ctx context.Context, from optional.Optional[K], to optional.Optional[K], iterFunc func(K, V) error) error {
	panic("not implemented")
}
func (c *CachingKeyValueStoreG[K, V]) ApproximateNumEntries() (uint64, error) {
	panic("not implemented")
}
func (c *CachingKeyValueStoreG[K, V]) Put(ctx context.Context, key K, value optional.Optional[V]) error {
	return c.putInternal(key, value)
}

func (c *CachingKeyValueStoreG[K, V]) putInternal(key K, value optional.Optional[V]) error {
	return c.cache.PutMaybeEvict(key, LRUEntry[V]{value: value, isDirty: true})
}

func (c *CachingKeyValueStoreG[K, V]) PutIfAbsent(ctx context.Context, key K, value V) (optional.Optional[V], error) {
	opV, err := c.getInternal(ctx, key)
	if err != nil {
		return optional.Optional[V]{}, err
	}
	if !opV.IsPresent() {
		err = c.putInternal(key, optional.Of(value))
		if err != nil {
			return optional.Optional[V]{}, err
		}
	}
	return opV, nil
}

func (c *CachingKeyValueStoreG[K, V]) getInternal(ctx context.Context, key K) (optional.Optional[V], error) {
	entry, found := c.cache.get(key)
	if found {
		return entry.value, nil
	} else {
		v, found, err := c.wrappedStore.Get(ctx, key)
		if err != nil {
			return optional.Optional[V]{}, err
		}
		if !found {
			return optional.Optional[V]{}, nil
		}
		retV := optional.Of(v)
		return retV, nil
	}
}

func (c *CachingKeyValueStoreG[K, V]) PutAll(ctx context.Context, msgs []*commtypes.Message) error {
	for _, msg := range msgs {
		err := c.Put(ctx, msg.Key.(K), optional.Of(msg.Value.(V)))
		if err != nil {
			return err
		}
	}
	return nil
}
func (c *CachingKeyValueStoreG[K, V]) Delete(ctx context.Context, key K) error {
	return c.cache.put(key, LRUEntry[V]{value: optional.Empty[V]()})
}
func (c *CachingKeyValueStoreG[K, V]) TableType() TABLE_TYPE { return IN_MEM }
func (c *CachingKeyValueStoreG[K, V]) SetTrackParFunc(f exactly_once_intr.TrackProdSubStreamFunc) {
	c.wrappedStore.SetTrackParFunc(f)
}
func (c *CachingKeyValueStoreG[K, V]) PutWithoutPushToChangelog(ctx context.Context, key commtypes.KeyT, value commtypes.ValueT) error {
	return c.wrappedStore.PutWithoutPushToChangelog(ctx, key, value)
}
func (c *CachingKeyValueStoreG[K, V]) Flush(ctx context.Context) error {
	c.cache.flush(nil)
	return c.wrappedStore.Flush(ctx)
}
