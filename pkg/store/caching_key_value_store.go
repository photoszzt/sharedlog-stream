package store

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/exactly_once_intr"
	"sharedlog-stream/pkg/optional"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/txn_data"
)

type CachingKeyValueStoreG[K comparable, V any] struct {
	wrappedStore      KeyValueStoreBackedByChangelogG[K, V]
	cache             *Cache[K, V]
	flushCallbackFunc func(ctx context.Context, msg commtypes.MessageG[K, commtypes.ChangeG[V]]) error
	name              string
}

var (
	_ CoreKeyValueStoreG[int, int]              = (*CachingKeyValueStoreG[int, int])(nil)
	_ KeyValueStoreBackedByChangelogG[int, int] = (*CachingKeyValueStoreG[int, int])(nil)
	_ CachedKeyValueStore[int, int]             = (*CachingKeyValueStoreG[int, int])(nil)
)

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
				err = c.wrappedStore.Put(ctx, entry.key, entry.entry.value, entry.entry.tm)
				if err != nil {
					return err
				}
				err = c.flushCallbackFunc(ctx, commtypes.MessageG[K, commtypes.ChangeG[V]]{
					Key:           optional.Some(entry.key),
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

func (c *CachingKeyValueStoreG[K, V]) SetKVSerde(serdeFormat commtypes.SerdeFormat, keySerde commtypes.SerdeG[K], valSerde commtypes.SerdeG[V]) error {
	return c.wrappedStore.SetKVSerde(serdeFormat, keySerde, valSerde)
}

func (c *CachingKeyValueStoreG[K, V]) GetKVSerde() commtypes.SerdeG[commtypes.KeyValuePair[K, V]] {
	return c.wrappedStore.GetKVSerde()
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

func (c *CachingKeyValueStoreG[K, V]) Put(ctx context.Context, key K, value optional.Option[V], tm TimeMeta) error {
	return c.putInternal(key, value, tm)
}

func (c *CachingKeyValueStoreG[K, V]) putInternal(key K, value optional.Option[V], tm TimeMeta) error {
	return c.cache.PutMaybeEvict(key, LRUEntry[V]{value: value, isDirty: true, tm: tm})
}

func (c *CachingKeyValueStoreG[K, V]) PutIfAbsent(ctx context.Context, key K, value V, tm TimeMeta) (optional.Option[V], error) {
	opV, err := c.getInternal(ctx, key)
	if err != nil {
		return optional.Option[V]{}, err
	}
	if opV.IsNone() {
		err = c.putInternal(key, optional.Some(value), tm)
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

/*
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
*/
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

func (c *CachingKeyValueStoreG[K, V]) Flush(ctx context.Context) (uint32, error) {
	c.cache.flush(nil)
	return c.wrappedStore.Flush(ctx)
}

func (c *CachingKeyValueStoreG[K, V]) ChangelogIsSrc() bool {
	return c.wrappedStore.ChangelogIsSrc()
}

func (c *CachingKeyValueStoreG[K, V]) ChangelogTopicName() string {
	return c.wrappedStore.ChangelogTopicName()
}

func (c *CachingKeyValueStoreG[K, V]) ConsumeOneLogEntry(ctx context.Context, parNum uint8) (int, error) {
	return c.wrappedStore.ConsumeOneLogEntry(ctx, parNum)
}

func (c *CachingKeyValueStoreG[K, V]) ConfigureExactlyOnce(
	rem exactly_once_intr.ReadOnlyExactlyOnceManager,
	guarantee exactly_once_intr.GuaranteeMth,
) {
	c.wrappedStore.ConfigureExactlyOnce(rem, guarantee)
}

func (c *CachingKeyValueStoreG[K, V]) Stream() sharedlog_stream.Stream {
	return c.wrappedStore.Stream()
}

func (c *CachingKeyValueStoreG[K, V]) GetInitialProdSeqNum() uint64 {
	return c.wrappedStore.GetInitialProdSeqNum()
}

func (c *CachingKeyValueStoreG[K, V]) ResetInitialProd() {
	c.wrappedStore.ResetInitialProd()
}

func (c *CachingKeyValueStoreG[K, V]) SetLastMarkerSeq(lastMarkerSeq uint64) {
	c.wrappedStore.SetLastMarkerSeq(lastMarkerSeq)
}

func (c *CachingKeyValueStoreG[K, V]) SubstreamNum() uint8 {
	return c.wrappedStore.SubstreamNum()
}

func (c *CachingKeyValueStoreG[K, V]) Snapshot(tpLogOff []commtypes.TpLogOff, unprocessed []commtypes.ChkptMetaData) {
	c.wrappedStore.Snapshot(tpLogOff, unprocessed)
}

func (c *CachingKeyValueStoreG[K, V]) WaitForAllSnapshot() error {
	return c.wrappedStore.WaitForAllSnapshot()
}

func (c *CachingKeyValueStoreG[K, V]) SetSnapshotCallback(ctx context.Context, f KVSnapshotCallback[K, V]) {
	c.wrappedStore.SetSnapshotCallback(ctx, f)
}

func (c *CachingKeyValueStoreG[K, V]) SetFlushCallbackFunc(f exactly_once_intr.FlushCallbackFunc) {
	c.wrappedStore.SetFlushCallbackFunc(f)
}

func (c *CachingKeyValueStoreG[K, V]) RestoreFromSnapshot(snapshot [][]byte) error {
	return c.wrappedStore.RestoreFromSnapshot(snapshot)
}

func (c *CachingKeyValueStoreG[K, V]) FindLastEpochMetaWithAuxData(ctx context.Context, parNum uint8) (auxData []byte, metaSeqNum uint64, err error) {
	return c.wrappedStore.FindLastEpochMetaWithAuxData(ctx, parNum)
}

func (c *CachingKeyValueStoreG[K, V]) BuildKeyMeta(ctx context.Context, kms map[string][]txn_data.KeyMaping) error {
	return c.wrappedStore.BuildKeyMeta(ctx, kms)
}

func (c *CachingKeyValueStoreG[K, V]) SetInstanceId(id uint8) {
	c.wrappedStore.SetInstanceId(id)
}

func (c *CachingKeyValueStoreG[K, V]) GetInstanceId() uint8 {
	return c.wrappedStore.GetInstanceId()
}
