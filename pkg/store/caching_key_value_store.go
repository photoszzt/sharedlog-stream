package store

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/exactly_once_intr"
	"sharedlog-stream/pkg/optional"
	"sharedlog-stream/pkg/sharedlog_stream"
)

type CachingKeyValueStoreG[K comparable, V any] struct {
	wrappedStore      KeyValueStoreBackedByChangelogG[K, V]
	cache             *Cache[K, V]
	flushCallbackFunc func(ctx context.Context, msg commtypes.MessageG[K, commtypes.ChangeG[V]]) error
	name              string
}

var _ CoreKeyValueStoreG[int, int] = (*CachingKeyValueStoreG[int, int])(nil)
var _ KeyValueStoreBackedByChangelogG[int, int] = (*CachingKeyValueStoreG[int, int])(nil)
var _ CachedStateStore[int, int] = (*CachingKeyValueStoreG[int, int])(nil)

func NewCachingKeyValueStoreG[K comparable, V any](ctx context.Context,
	store KeyValueStoreBackedByChangelogG[K, V],
	sizeOfK func(K) int64,
	sizeOfV func(V) int64,
	maxCacheBytes int64,
) *CachingKeyValueStoreG[K, V] {
	c := &CachingKeyValueStoreG[K, V]{
		name:         store.Name(),
		wrappedStore: store,
	}
	c.cache = NewCache(func(entries []LRUElement[K, V]) error {
		for _, entry := range entries {
			newVal := entry.entry.value
			oldVal, ok, err := store.Get(ctx, entry.key)
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
				err = c.wrappedStore.Put(ctx, entry.key, entry.entry.value, entry.entry.currentStreamTime)
				if err != nil {
					return err
				}
				err = c.flushCallbackFunc(ctx, commtypes.MessageG[K, commtypes.ChangeG[V]]{
					Key:       optional.Some(entry.key),
					Value:     optional.Some(change),
					Timestamp: entry.entry.currentStreamTime,
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

func (c *CachingKeyValueStoreG[K, V]) SetKVSerde(serdeFormat commtypes.SerdeFormat, keySerde commtypes.SerdeG[K], valSerde commtypes.SerdeG[V]) error {
	return c.wrappedStore.SetKVSerde(serdeFormat, keySerde, valSerde)
}
func (c *CachingKeyValueStoreG[K, V]) Name() string { return c.name }
func (c *CachingKeyValueStoreG[K, V]) Get(ctx context.Context, key K) (V, bool, error) {
	entry, found := c.cache.get(key)
	if found {
		v, ok := entry.value.Take()
		return v, ok, nil
	} else {
		return c.wrappedStore.Get(ctx, key)
	}
}
func (c *CachingKeyValueStoreG[K, V]) SetFlushCallback(flushCallbackFunc func(ctx context.Context, msg commtypes.MessageG[K, commtypes.ChangeG[V]]) error) {
	c.flushCallbackFunc = flushCallbackFunc
}
func (c *CachingKeyValueStoreG[K, V]) Range(ctx context.Context, from optional.Option[K], to optional.Option[K], iterFunc func(K, V) error) error {
	panic("not implemented")
}
func (c *CachingKeyValueStoreG[K, V]) ApproximateNumEntries() (uint64, error) {
	panic("not implemented")
}
func (c *CachingKeyValueStoreG[K, V]) Put(ctx context.Context, key K, value optional.Option[V], currentStreamTime int64) error {
	return c.putInternal(key, value, currentStreamTime)
}

func (c *CachingKeyValueStoreG[K, V]) putInternal(key K, value optional.Option[V], currentStreamTime int64) error {
	return c.cache.PutMaybeEvict(key, LRUEntry[V]{value: value, isDirty: true, currentStreamTime: currentStreamTime})
}

func (c *CachingKeyValueStoreG[K, V]) PutIfAbsent(ctx context.Context, key K, value V, currentStreamTime int64) (optional.Option[V], error) {
	opV, err := c.getInternal(ctx, key)
	if err != nil {
		return optional.Option[V]{}, err
	}
	if opV.IsNone() {
		err = c.putInternal(key, optional.Some(value), currentStreamTime)
		if err != nil {
			return optional.Option[V]{}, err
		}
	}
	return opV, nil
}

func (c *CachingKeyValueStoreG[K, V]) getInternal(ctx context.Context, key K) (optional.Option[V], error) {
	entry, found := c.cache.get(key)
	if found {
		return entry.value, nil
	} else {
		v, found, err := c.wrappedStore.Get(ctx, key)
		if err != nil {
			return optional.Option[V]{}, err
		}
		if !found {
			return optional.Option[V]{}, nil
		}
		retV := optional.Some(v)
		return retV, nil
	}
}

func (c *CachingKeyValueStoreG[K, V]) PutAll(ctx context.Context, msgs []*commtypes.Message) error {
	maxTs := int64(0)
	for _, msg := range msgs {
		if msg.Timestamp > maxTs {
			maxTs = msg.Timestamp
		}
		err := c.Put(ctx, msg.Key.(K), optional.Some(msg.Value.(V)), maxTs)
		if err != nil {
			return err
		}
	}
	return nil
}
func (c *CachingKeyValueStoreG[K, V]) Delete(ctx context.Context, key K) error {
	return c.cache.put(key, LRUEntry[V]{value: optional.None[V]()})
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
func (c *CachingKeyValueStoreG[K, V]) ChangelogIsSrc() bool {
	return c.wrappedStore.ChangelogIsSrc()
}
func (c *CachingKeyValueStoreG[K, V]) ChangelogTopicName() string {
	return c.wrappedStore.ChangelogTopicName()
}

func (c *CachingKeyValueStoreG[K, V]) ConsumeOneLogEntry(ctx context.Context, parNum uint8, cutoff uint64) (int, error) {
	return c.wrappedStore.ConsumeOneLogEntry(ctx, parNum, cutoff)
}
func (c *CachingKeyValueStoreG[K, V]) ConfigureExactlyOnce(rem exactly_once_intr.ReadOnlyExactlyOnceManager,
	guarantee exactly_once_intr.GuaranteeMth,
) error {
	return c.wrappedStore.ConfigureExactlyOnce(rem, guarantee)
}
func (c *CachingKeyValueStoreG[K, V]) Stream() sharedlog_stream.Stream {
	return c.wrappedStore.Stream()
}

func (c *CachingKeyValueStoreG[K, V]) GetInitialProdSeqNum() uint64 {
	return c.wrappedStore.GetInitialProdSeqNum()
}
func (c *CachingKeyValueStoreG[K, V]) GetCurrentProdSeqNum() uint64 {
	return c.wrappedStore.GetCurrentProdSeqNum()
}
func (c *CachingKeyValueStoreG[K, V]) ResetInitialProd() {
	c.wrappedStore.ResetInitialProd()
}

func (c *CachingKeyValueStoreG[K, V]) SubstreamNum() uint8 {
	return c.wrappedStore.SubstreamNum()
}

func (c *CachingKeyValueStoreG[K, V]) Snapshot() [][]byte {
	return c.wrappedStore.Snapshot()
}
