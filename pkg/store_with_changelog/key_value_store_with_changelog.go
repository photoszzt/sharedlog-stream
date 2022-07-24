package store_with_changelog

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/exactly_once_intr"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stats"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/treemap"
)

type KeyValueStoreWithChangelog[K, V any] struct {
	kvstore          store.CoreKeyValueStore
	msgSerde         commtypes.MessageSerdeG[K, V]
	trackFunc        exactly_once_intr.TrackProdSubStreamFunc
	changelogManager *ChangelogManager[K, V]
	changelogProduce stats.ConcurrentInt64Collector
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
	changelogManager := NewChangelogManager(changelog, mp.msgSerde, mp.changelogParam.TimeOut,
		mp.changelogParam.FlushDuration)
	return &KeyValueStoreWithChangelog[K, V]{
		kvstore:          store,
		trackFunc:        exactly_once_intr.DefaultTrackProdSubstreamFunc,
		use_bytes:        use_bytes,
		msgSerde:         mp.msgSerde,
		changelogManager: changelogManager,
		parNum:           mp.ParNum(),
		changelogProduce: stats.NewConcurrentInt64Collector(mp.storeName+"-clProd",
			stats.DEFAULT_COLLECT_DURATION),
	}, nil
}

func (st *KeyValueStoreWithChangelog[K, V]) Name() string {
	return st.kvstore.Name()
}

func (st *KeyValueStoreWithChangelog[K, V]) Get(ctx context.Context, key commtypes.KeyT) (commtypes.ValueT, bool, error) {
	if st.use_bytes {
		keyBytes, err := st.msgSerde.GetKeySerdeG().Encode(key.(K))
		if err != nil {
			return nil, false, err
		}
		valBytes, ok, err := st.kvstore.Get(ctx, keyBytes)
		if err != nil {
			return nil, ok, err
		}
		val, err := st.msgSerde.GetValSerdeG().Decode(valBytes.([]byte))
		return val, ok, err
	}
	return st.kvstore.Get(ctx, key)
}

func (st *KeyValueStoreWithChangelog[K, V]) FlushChangelog(ctx context.Context) error {
	return st.changelogManager.Flush(ctx)
}
func (st *KeyValueStoreWithChangelog[K, V]) ConfigureExactlyOnce(rem exactly_once_intr.ReadOnlyExactlyOnceManager,
	guarantee exactly_once_intr.GuaranteeMth, serdeFormat commtypes.SerdeFormat,
) error {
	return st.changelogManager.ConfigExactlyOnce(rem, guarantee, serdeFormat)
}

func (st *KeyValueStoreWithChangelog[K, V]) Put(ctx context.Context, key commtypes.KeyT, value commtypes.ValueT) error {
	msg := commtypes.Message{
		Key:   key,
		Value: value,
	}
	pStart := stats.TimerBegin()
	err := st.changelogManager.Produce(ctx, msg, st.parNum, false)
	elapsed := stats.Elapsed(pStart).Microseconds()
	st.changelogProduce.AddSample(elapsed)
	if err != nil {
		return err
	}
	err = st.trackFunc(ctx, key.(K), commtypes.EncoderFunc(func(i interface{}) ([]byte, error) {
		if i == nil {
			return nil, nil
		}
		return st.msgSerde.GetKeySerdeG().Encode(i.(K))
	}), st.changelogManager.TopicName(), st.parNum)
	if err != nil {
		return err
	}
	if st.use_bytes {
		keyBytes, err := st.msgSerde.GetKeySerdeG().Encode(key.(K))
		if err != nil {
			return err
		}
		valBytes, err := st.msgSerde.GetValSerdeG().Encode(value.(V))
		if err != nil {
			return err
		}
		err = st.kvstore.Put(ctx, keyBytes, valBytes)
		return err
	} else {
		err = st.kvstore.Put(ctx, key, value)
		return err
	}
}

func (st *KeyValueStoreWithChangelog[K, V]) PutWithoutPushToChangelog(ctx context.Context, key commtypes.KeyT, value commtypes.ValueT) error {
	return st.kvstore.Put(ctx, key, value)
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
	msg := commtypes.Message{
		Key:   key,
		Value: nil,
	}
	err := st.changelogManager.Produce(ctx, msg, st.parNum, false)
	if err != nil {
		return err
	}
	return st.kvstore.Delete(ctx, key)
}

func (st *KeyValueStoreWithChangelog[K, V]) ApproximateNumEntries() (uint64, error) {
	return st.kvstore.ApproximateNumEntries()
}

func (st *KeyValueStoreWithChangelog[K, V]) Range(ctx context.Context, from commtypes.KeyT, to commtypes.KeyT, iterFunc func(commtypes.KeyT, commtypes.ValueT) error) error {
	return st.kvstore.Range(ctx, from, to, iterFunc)
}

func (st *KeyValueStoreWithChangelog[K, V]) ReverseRange(from commtypes.KeyT, to commtypes.KeyT, iterFunc func(commtypes.KeyT, commtypes.ValueT) error) error {
	return st.kvstore.ReverseRange(from, to, iterFunc)
}

func (st *KeyValueStoreWithChangelog[K, V]) PrefixScan(prefix interface{},
	prefixKeyEncoder commtypes.Encoder,
	iterFunc func(commtypes.KeyT, commtypes.ValueT) error,
) error {
	panic("not implemented")
}

func (st *KeyValueStoreWithChangelog[K, V]) TableType() store.TABLE_TYPE {
	return st.kvstore.TableType()
}

func (st *KeyValueStoreWithChangelog[K, V]) StartTransaction(ctx context.Context) error { return nil }
func (st *KeyValueStoreWithChangelog[K, V]) CommitTransaction(ctx context.Context,
	taskRepr string, transactionID uint64) error {
	return nil
}
func (st *KeyValueStoreWithChangelog[K, V]) AbortTransaction(ctx context.Context) error { return nil }
func (st *KeyValueStoreWithChangelog[K, V]) GetTransactionID(ctx context.Context, taskRepr string) (uint64, bool, error) {
	panic("not supported")
}

func (st *KeyValueStoreWithChangelog[K, V]) SetTrackParFunc(trackParFunc exactly_once_intr.TrackProdSubStreamFunc) {
	st.trackFunc = trackParFunc
}
func (st *KeyValueStoreWithChangelog[K, V]) ConsumeChangelog(ctx context.Context, parNum uint8) (*commtypes.MsgAndSeqs, error) {
	return st.changelogManager.Consume(ctx, parNum)
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

func CreateInMemKVTableWithChangelog[K, V any](mp *MaterializeParam[K, V],
	compare func(a, b treemap.Key) int,
) (*KeyValueStoreWithChangelog[K, V], error) {
	s := store.NewInMemoryKeyValueStore(mp.storeName, compare)
	return NewKeyValueStoreWithChangelog(mp, s, false)
}
