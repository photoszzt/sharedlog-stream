package store_with_changelog

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/exactly_once_intr"
	"sharedlog-stream/pkg/optional"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stats"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/utils"
)

type KeyValueStoreWithChangelogG[K, V any] struct {
	kvstore          store.CoreKeyValueStoreG[K, V]
	msgSerde         commtypes.MessageGSerdeG[K, V]
	trackFunc        exactly_once_intr.TrackProdSubStreamFunc
	changelogManager *ChangelogManager[K, V]
	changelogProduce *stats.ConcurrentStatsCollector[int64]
	parNum           uint8
}

var _ = store.KeyValueStoreBackedByChangelogG[int, int](&KeyValueStoreWithChangelogG[int, int]{})

func NewKeyValueStoreWithChangelogG[K, V any](mp *MaterializeParam[K, V],
	store store.CoreKeyValueStoreG[K, V],
) (*KeyValueStoreWithChangelogG[K, V], error) {
	changelog, err := CreateChangelog(mp.changelogParam.Env,
		mp.storeName, mp.changelogParam.NumPartition, mp.serdeFormat)
	if err != nil {
		return nil, err
	}
	changelogManager, err := NewChangelogManager(changelog, mp.msgSerde, mp.changelogParam.TimeOut,
		mp.changelogParam.FlushDuration, mp.serdeFormat, mp.parNum)
	if err != nil {
		return nil, err
	}
	err = store.SetKVSerde(mp.serdeFormat, mp.msgSerde.GetKeySerdeG(), mp.msgSerde.GetValSerdeG())
	if err != nil {
		return nil, err
	}
	return &KeyValueStoreWithChangelogG[K, V]{
		kvstore:          store,
		trackFunc:        exactly_once_intr.DefaultTrackProdSubstreamFunc,
		msgSerde:         mp.msgSerde,
		changelogManager: changelogManager,
		parNum:           mp.ParNum(),
		changelogProduce: stats.NewConcurrentStatsCollector[int64](mp.storeName+"-clProd",
			stats.DEFAULT_COLLECT_DURATION),
	}, nil
}

func (st *KeyValueStoreWithChangelogG[K, V]) SetKVSerde(serdeFormat commtypes.SerdeFormat, keySerde commtypes.SerdeG[K], valSerde commtypes.SerdeG[V]) error {
	return nil
}

func (st *KeyValueStoreWithChangelogG[K, V]) Name() string {
	return st.kvstore.Name()
}

func (st *KeyValueStoreWithChangelogG[K, V]) Get(ctx context.Context, key K) (V, bool, error) {
	return st.kvstore.Get(ctx, key)
}

func (st *KeyValueStoreWithChangelogG[K, V]) Flush(ctx context.Context) error {
	return st.changelogManager.Flush(ctx)
}
func (st *KeyValueStoreWithChangelogG[K, V]) ConfigureExactlyOnce(rem exactly_once_intr.ReadOnlyExactlyOnceManager,
	guarantee exactly_once_intr.GuaranteeMth,
) error {
	return st.changelogManager.ConfigExactlyOnce(rem, guarantee)
}

func (st *KeyValueStoreWithChangelogG[K, V]) Put(ctx context.Context, key K, value optional.Option[V], currentStreamTime int64) error {
	msg := commtypes.MessageG[K, V]{
		Key:   optional.Some(key),
		Value: value,
	}
	pStart := stats.TimerBegin()
	msgSerOp, err := commtypes.MsgGToMsgSer(msg, st.msgSerde.GetKeySerdeG(), st.msgSerde.GetValSerdeG())
	if err != nil {
		return err
	}
	msgSer, ok := msgSerOp.Take()
	if ok {
		err := st.changelogManager.Produce(ctx, msgSer, st.parNum)
		elapsed := stats.Elapsed(pStart).Microseconds()
		st.changelogProduce.AddSample(elapsed)
		if err != nil {
			return err
		}
		err = st.trackFunc(ctx, msgSer.KeyEnc, st.changelogManager.TopicName(), st.parNum)
		if err != nil {
			return err
		}
		err = st.kvstore.Put(ctx, key, value, currentStreamTime)
		return err
	} else {
		return nil
	}
}

func (st *KeyValueStoreWithChangelogG[K, V]) PutWithoutPushToChangelog(ctx context.Context, key commtypes.KeyT, value commtypes.ValueT) error {
	return st.kvstore.PutWithoutPushToChangelog(ctx, key, value)
}

func (st *KeyValueStoreWithChangelogG[K, V]) PutIfAbsent(ctx context.Context, key K, value V, currentStreamTime int64) (optional.Option[V], error) {
	origVal, exists, err := st.kvstore.Get(ctx, key)
	if err != nil {
		return optional.None[V](), err
	}
	if !exists {
		err := st.Put(ctx, key, optional.Some(value), currentStreamTime)
		if err != nil {
			return optional.None[V](), err
		}
		return optional.None[V](), nil
	}
	return optional.Some(origVal), nil
}

func (st *KeyValueStoreWithChangelogG[K, V]) PutAll(ctx context.Context, entries []*commtypes.Message) error {
	maxTs := int64(0)
	for _, msg := range entries {
		if msg.Timestamp > maxTs {
			maxTs = msg.Timestamp
		}
		var err error
		if utils.IsNil(msg.Value) {
			err = st.Put(ctx, msg.Key.(K), optional.None[V](), maxTs)
		} else {
			err = st.Put(ctx, msg.Key.(K), optional.Some(msg.Value.(V)), maxTs)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (st *KeyValueStoreWithChangelogG[K, V]) Delete(ctx context.Context, key K) error {
	msg := commtypes.MessageG[K, V]{
		Key:   optional.Some(key),
		Value: optional.None[V](),
	}
	msgSerOp, err := commtypes.MsgGToMsgSer(msg, st.msgSerde.GetKeySerdeG(), st.msgSerde.GetValSerdeG())
	if err != nil {
		return err
	}
	msgSer, ok := msgSerOp.Take()
	if ok {
		err := st.changelogManager.Produce(ctx, msgSer, st.parNum)
		if err != nil {
			return err
		}
		return st.kvstore.Delete(ctx, key)
	} else {
		return nil
	}
}

func (st *KeyValueStoreWithChangelogG[K, V]) ApproximateNumEntries() (uint64, error) {
	return st.kvstore.ApproximateNumEntries()
}

func (st *KeyValueStoreWithChangelogG[K, V]) Range(ctx context.Context, from optional.Option[K], to optional.Option[K], iterFunc func(K, V) error) error {
	return st.kvstore.Range(ctx, from, to, iterFunc)
}

func (st *KeyValueStoreWithChangelogG[K, V]) TableType() store.TABLE_TYPE {
	return st.kvstore.TableType()
}

func (st *KeyValueStoreWithChangelogG[K, V]) SetTrackParFunc(trackParFunc exactly_once_intr.TrackProdSubStreamFunc) {
	st.trackFunc = trackParFunc
}
func (st *KeyValueStoreWithChangelogG[K, V]) ConsumeOneLogEntry(ctx context.Context, parNum uint8, cutoff uint64) (int, error) {
	msgSeq, err := st.changelogManager.Consume(ctx, parNum)
	if err != nil {
		return 0, err
	}
	count := 0
	if msgSeq.MsgArr != nil {
		for _, msg := range msgSeq.MsgArr {
			if msg.Key.IsNone() && msg.Value.IsNone() {
				continue
			}
			count += 1
			k := msg.Key.Unwrap()
			v := msg.Value.Unwrap()
			err = st.PutWithoutPushToChangelog(ctx, k, v)
			if err != nil {
				return 0, err
			}
		}
	} else {
		msg := msgSeq.Msg
		if msg.Key.IsNone() && msg.Value.IsNone() {
			return 0, nil
		}
		count += 1
		k := msg.Key.Unwrap()
		v := msg.Value.Unwrap()
		err = st.PutWithoutPushToChangelog(ctx, k, v)
		if err != nil {
			return 0, err
		}
	}
	return count, nil
}
func (st *KeyValueStoreWithChangelogG[K, V]) ChangelogTopicName() string {
	return st.changelogManager.TopicName()
}
func (st *KeyValueStoreWithChangelogG[K, V]) ChangelogIsSrc() bool {
	return false
}

func (st *KeyValueStoreWithChangelogG[K, V]) Stream() sharedlog_stream.Stream {
	return st.changelogManager.Stream()
}

func (st *KeyValueStoreWithChangelogG[K, V]) GetInitialProdSeqNum() uint64 {
	return st.changelogManager.producer.GetInitialProdSeqNum(st.parNum)
}
func (st *KeyValueStoreWithChangelogG[K, V]) GetCurrentProdSeqNum() uint64 {
	return st.changelogManager.producer.GetCurrentProdSeqNum(st.parNum)
}
func (st *KeyValueStoreWithChangelogG[K, V]) ResetInitialProd() {
	st.changelogManager.producer.ResetInitialProd()
}
func (st *KeyValueStoreWithChangelogG[K, V]) SubstreamNum() uint8 {
	return st.parNum
}
func (st *KeyValueStoreWithChangelogG[K, V]) SetFlushCallback(
	func(ctx context.Context, msg commtypes.MessageG[K, commtypes.ChangeG[V]]) error) {
}
func (st *KeyValueStoreWithChangelogG[K, V]) Snapshot() [][]byte {
	return st.kvstore.Snapshot()
}
func (st *KeyValueStoreWithChangelogG[K, V]) RestoreFromSnapshot(snapshot [][]byte) error {
	return st.kvstore.RestoreFromSnapshot(snapshot)
}

/*
func CreateInMemBTreeKVTableWithChangelogG[K, V any](mp *MaterializeParam[K, V], less store.LessFunc[K],
) (*KeyValueStoreWithChangelogG[K, V], error) {
	s := store.NewInMemoryBTreeKeyValueStoreG[K, V](mp.storeName, less)
	return NewKeyValueStoreWithChangelogG[K, V](mp, s)
}
*/

func CreateInMemorySkipmapKVTableWithChangelogG[K, V any](mp *MaterializeParam[K, V], less store.LessFunc[K],
) (*KeyValueStoreWithChangelogG[K, V], error) {
	s := store.NewInMemorySkipmapKeyValueStoreG[K, V](mp.storeName, less)
	return NewKeyValueStoreWithChangelogG[K, V](mp, s)
}

func ToInMemSkipmapKVTableWithChangelog[K, V any](mp *MaterializeParam[K, commtypes.ValueTimestampG[V]],
	less store.LessFunc[K],
) (*processor.MeteredProcessorG[K, V, K, commtypes.ChangeG[V]],
	*KeyValueStoreWithChangelogG[K, commtypes.ValueTimestampG[V]], error,
) {
	s := store.NewInMemorySkipmapKeyValueStoreG[K, commtypes.ValueTimestampG[V]](mp.storeName, less)
	storeWithlog, err := NewKeyValueStoreWithChangelogG[K, commtypes.ValueTimestampG[V]](mp, s)
	if err != nil {
		return nil, nil, err
	}
	toTableProc := processor.NewMeteredProcessorG[K, V, K, commtypes.ChangeG[V]](
		processor.NewTableSourceProcessorWithTableG[K, V](storeWithlog))
	return toTableProc, storeWithlog, nil
}
