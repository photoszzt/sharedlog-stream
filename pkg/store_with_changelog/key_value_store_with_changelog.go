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
)

type KeyValueStoreWithChangelog[K, V any] struct {
	kvstore          store.CoreKeyValueStore
	msgSerde         commtypes.MessageGSerdeG[K, V]
	trackFunc        exactly_once_intr.TrackProdSubStreamFunc
	changelogManager *ChangelogManager[K, V]
	changelogProduce *stats.ConcurrentStatsCollector[int64]
	use_bytes        bool
	parNum           uint8
}

var _ = store.KeyValueStoreBackedByChangelog(&KeyValueStoreWithChangelog[int, int]{})

func NewKeyValueStoreWithChangelog[K, V any](mp *MaterializeParam[K, V],
	store store.CoreKeyValueStore, use_bytes bool,
) (*KeyValueStoreWithChangelog[K, V], error) {
	changelog, err := CreateChangelog(mp.changelogParam.Env,
		mp.storeName+"_changelog", mp.changelogParam.NumPartition, mp.serdeFormat)
	if err != nil {
		return nil, err
	}
	changelogManager, err := NewChangelogManager(changelog, mp.msgSerde, mp.changelogParam.TimeOut,
		mp.changelogParam.FlushDuration, mp.serdeFormat, mp.parNum)
	if err != nil {
		return nil, err
	}
	return &KeyValueStoreWithChangelog[K, V]{
		kvstore:          store,
		trackFunc:        exactly_once_intr.DefaultTrackProdSubstreamFunc,
		use_bytes:        use_bytes,
		msgSerde:         mp.msgSerde,
		changelogManager: changelogManager,
		parNum:           mp.ParNum(),
		changelogProduce: stats.NewConcurrentStatsCollector[int64](mp.storeName+"-clProd",
			stats.DEFAULT_COLLECT_DURATION),
	}, nil
}

func (st *KeyValueStoreWithChangelog[K, V]) Name() string {
	return st.kvstore.Name()
}

func (st *KeyValueStoreWithChangelog[K, V]) Snapshot() [][]byte {
	panic("not implemented")
}

func (st *KeyValueStoreWithChangelog[K, V]) RestoreFromSnapshot(snapshot [][]byte) error {
	panic("not implemented")
}

func (st *KeyValueStoreWithChangelog[K, V]) Get(ctx context.Context, key commtypes.KeyT) (commtypes.ValueT, bool, error) {
	if st.use_bytes {
		keyBytes, err := st.msgSerde.EncodeKey(key.(K))
		if err != nil {
			return nil, false, err
		}
		valBytes, ok, err := st.kvstore.Get(ctx, keyBytes)
		if err != nil {
			return nil, ok, err
		}
		val, err := st.msgSerde.DecodeVal(valBytes.([]byte))
		return val, ok, err
	}
	return st.kvstore.Get(ctx, key)
}

func (st *KeyValueStoreWithChangelog[K, V]) Flush(ctx context.Context) error {
	return st.changelogManager.Flush(ctx)
}
func (st *KeyValueStoreWithChangelog[K, V]) ConfigureExactlyOnce(rem exactly_once_intr.ReadOnlyExactlyOnceManager,
	guarantee exactly_once_intr.GuaranteeMth,
) error {
	return st.changelogManager.ConfigExactlyOnce(rem, guarantee)
}

func (st *KeyValueStoreWithChangelog[K, V]) Put(ctx context.Context, key commtypes.KeyT, value commtypes.ValueT) error {
	msg := commtypes.MessageG[K, V]{
		Key:   optional.Some(key.(K)),
		Value: optional.Some(value.(V)),
	}
	pStart := stats.TimerBegin()
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
		elapsed := stats.Elapsed(pStart).Microseconds()
		st.changelogProduce.AddSample(elapsed)
		if err != nil {
			return err
		}
		err = st.trackFunc(ctx, msgSer.KeyEnc, st.changelogManager.TopicName(), st.parNum)
		if err != nil {
			return err
		}
		if st.use_bytes {
			keyBytes, err := st.msgSerde.EncodeKey(key.(K))
			if err != nil {
				return err
			}
			valBytes, err := st.msgSerde.EncodeVal(value.(V))
			if err != nil {
				return err
			}
			err = st.kvstore.Put(ctx, keyBytes, valBytes)
			return err
		} else {
			err = st.kvstore.Put(ctx, key, value)
			return err
		}
	} else {
		return nil
	}
}

func (st *KeyValueStoreWithChangelog[K, V]) PutWithoutPushToChangelog(ctx context.Context, key commtypes.KeyT, value commtypes.ValueT) error {
	return st.kvstore.PutWithoutPushToChangelog(ctx, key, value)
}

func (st *KeyValueStoreWithChangelog[K, V]) PutIfAbsent(ctx context.Context, key commtypes.KeyT, value commtypes.ValueT) (commtypes.ValueT, error) {
	origVal, exists, err := st.kvstore.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if !exists {
		err := st.Put(ctx, key, value)
		if err != nil {
			return nil, err
		}
		return nil, nil
	}
	return origVal, nil
}

func (st *KeyValueStoreWithChangelog[K, V]) PutAll(ctx context.Context, entries []*commtypes.Message) error {
	for _, msg := range entries {
		err := st.Put(ctx, msg.Key, msg.Value)
		if err != nil {
			return err
		}
	}
	return nil
}

func (st *KeyValueStoreWithChangelog[K, V]) Delete(ctx context.Context, key commtypes.KeyT) error {
	msg := commtypes.MessageG[K, V]{
		Key:   optional.Some(key.(K)),
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

func (st *KeyValueStoreWithChangelog[K, V]) ApproximateNumEntries() (uint64, error) {
	return st.kvstore.ApproximateNumEntries()
}

func (st *KeyValueStoreWithChangelog[K, V]) Range(ctx context.Context, from commtypes.KeyT, to commtypes.KeyT, iterFunc func(commtypes.KeyT, commtypes.ValueT) error) error {
	return st.kvstore.Range(ctx, from, to, iterFunc)
}

func (st *KeyValueStoreWithChangelog[K, V]) TableType() store.TABLE_TYPE {
	return st.kvstore.TableType()
}

func (st *KeyValueStoreWithChangelog[K, V]) SetTrackParFunc(trackParFunc exactly_once_intr.TrackProdSubStreamFunc) {
	st.trackFunc = trackParFunc
}
func (st *KeyValueStoreWithChangelog[K, V]) ConsumeOneLogEntry(ctx context.Context, parNum uint8, cutoff uint64) (int, error) {
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
func (st *KeyValueStoreWithChangelog[K, V]) ChangelogTopicName() string {
	return st.changelogManager.TopicName()
}
func (st *KeyValueStoreWithChangelog[K, V]) ChangelogIsSrc() bool {
	return false
}

func (st *KeyValueStoreWithChangelog[K, V]) Stream() sharedlog_stream.Stream {
	return st.changelogManager.Stream()
}

func (st *KeyValueStoreWithChangelog[K, V]) GetInitialProdSeqNum() uint64 {
	return st.changelogManager.producer.GetInitialProdSeqNum(st.parNum)
}
func (st *KeyValueStoreWithChangelog[K, V]) GetCurrentProdSeqNum() uint64 {
	return st.changelogManager.producer.GetCurrentProdSeqNum(st.parNum)
}
func (st *KeyValueStoreWithChangelog[K, V]) ResetInitialProd() {
	st.changelogManager.producer.ResetInitialProd()
}
func (st *KeyValueStoreWithChangelog[K, V]) SubstreamNum() uint8 {
	return st.parNum
}

func CreateInMemKVTableWithChangelog[K, V any](mp *MaterializeParam[K, V],
	compare store.KVStoreLessFunc,
) (*KeyValueStoreWithChangelog[K, V], error) {
	s := store.NewInMemoryKeyValueStore(mp.storeName, compare)
	return NewKeyValueStoreWithChangelog(mp, s, false)
}

/*
func CreateInMemBTreeKVTableWithChangelog[K, V any](mp *MaterializeParam[K, V],
) (*KeyValueStoreWithChangelog[K, V], error) {
	s := store.NewInMemoryBTreeKeyValueStore(mp.storeName)
	return NewKeyValueStoreWithChangelog(mp, s, false)
}
*/

func ToInMemKVTableWithChangelog[K, V any](mp *MaterializeParam[K, V],
	compare store.KVStoreLessFunc,
) (*processor.MeteredProcessor, *KeyValueStoreWithChangelog[K, V], error) {
	s := store.NewInMemoryKeyValueStore(mp.storeName, compare)
	storeWithlog, err := NewKeyValueStoreWithChangelog(mp, s, false)
	if err != nil {
		return nil, nil, err
	}
	toTableProc := processor.NewMeteredProcessor(processor.NewTableSourceProcessorWithTable(storeWithlog))
	return toTableProc, storeWithlog, nil
}
