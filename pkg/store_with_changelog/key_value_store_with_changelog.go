package store_with_changelog

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/exactly_once_intr"
	"sharedlog-stream/pkg/stats"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/treemap"
)

type KeyValueStoreWithChangelog struct {
	kvstore          store.KeyValueStore
	msgSerde         commtypes.MessageSerde
	trackFunc        exactly_once_intr.TrackProdSubStreamFunc
	changelogManager *ChangelogManager
	changelogProduce stats.ConcurrentInt64Collector
	use_bytes        bool
	parNum           uint8
}

func NewKeyValueStoreWithChangelog(mp *MaterializeParam,
	store store.KeyValueStore, use_bytes bool,
) (*KeyValueStoreWithChangelog, error) {
	changelog, err := CreateChangelog(mp.changelogParam.Env,
		mp.storeName+"_changelog", mp.changelogParam.NumPartition, mp.serdeFormat)
	if err != nil {
		return nil, err
	}
	changelogManager := NewChangelogManager(changelog, mp.msgSerde, mp.changelogParam.TimeOut,
		mp.changelogParam.FlushDuration)
	return &KeyValueStoreWithChangelog{
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

func (st *KeyValueStoreWithChangelog) Name() string {
	return st.kvstore.Name()
}

func (st *KeyValueStoreWithChangelog) ChangelogManager() *ChangelogManager {
	return st.changelogManager
}

func (st *KeyValueStoreWithChangelog) Get(ctx context.Context, key commtypes.KeyT) (commtypes.ValueT, bool, error) {
	if st.use_bytes {
		keyBytes, err := st.msgSerde.GetKeySerde().Encode(key)
		if err != nil {
			return nil, false, err
		}
		valBytes, ok, err := st.kvstore.Get(ctx, keyBytes)
		if err != nil {
			return nil, ok, err
		}
		val, err := st.msgSerde.GetValSerde().Decode(valBytes.([]byte))
		return val, ok, err
	}
	return st.kvstore.Get(ctx, key)
}

func (st *KeyValueStoreWithChangelog) FlushChangelog(ctx context.Context) error {
	return st.changelogManager.Flush(ctx)
}

func (st *KeyValueStoreWithChangelog) Put(ctx context.Context, key commtypes.KeyT, value commtypes.ValueT) error {
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
	err = st.trackFunc(ctx, key, st.msgSerde.GetKeySerde(), st.changelogManager.TopicName(), st.parNum)
	if err != nil {
		return err
	}
	if st.use_bytes {
		keyBytes, err := st.msgSerde.GetKeySerde().Encode(key)
		if err != nil {
			return err
		}
		valBytes, err := st.msgSerde.GetValSerde().Encode(value)
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

func (st *KeyValueStoreWithChangelog) PutWithoutPushToChangelog(ctx context.Context, key commtypes.KeyT, value commtypes.ValueT) error {
	return st.kvstore.Put(ctx, key, value)
}

func (st *KeyValueStoreWithChangelog) PutIfAbsent(ctx context.Context, key commtypes.KeyT, value commtypes.ValueT) (commtypes.ValueT, error) {
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

func (st *KeyValueStoreWithChangelog) PutAll(ctx context.Context, entries []*commtypes.Message) error {
	for _, msg := range entries {
		err := st.Put(ctx, msg.Key, msg.Value)
		if err != nil {
			return err
		}
	}
	return nil
}

func (st *KeyValueStoreWithChangelog) Delete(ctx context.Context, key commtypes.KeyT) error {
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

func (st *KeyValueStoreWithChangelog) ApproximateNumEntries() (uint64, error) {
	return st.kvstore.ApproximateNumEntries()
}

func (st *KeyValueStoreWithChangelog) Range(ctx context.Context, from commtypes.KeyT, to commtypes.KeyT, iterFunc func(commtypes.KeyT, commtypes.ValueT) error) error {
	return st.kvstore.Range(ctx, from, to, iterFunc)
}

func (st *KeyValueStoreWithChangelog) ReverseRange(from commtypes.KeyT, to commtypes.KeyT, iterFunc func(commtypes.KeyT, commtypes.ValueT) error) error {
	return st.kvstore.ReverseRange(from, to, iterFunc)
}

func (st *KeyValueStoreWithChangelog) PrefixScan(prefix interface{},
	prefixKeyEncoder commtypes.Encoder,
	iterFunc func(commtypes.KeyT, commtypes.ValueT) error,
) error {
	panic("not implemented")
}

func (st *KeyValueStoreWithChangelog) TableType() store.TABLE_TYPE {
	return st.kvstore.TableType()
}

func (st *KeyValueStoreWithChangelog) StartTransaction(ctx context.Context) error { return nil }
func (st *KeyValueStoreWithChangelog) CommitTransaction(ctx context.Context,
	taskRepr string, transactionID uint64) error {
	return nil
}
func (st *KeyValueStoreWithChangelog) AbortTransaction(ctx context.Context) error { return nil }
func (st *KeyValueStoreWithChangelog) GetTransactionID(ctx context.Context, taskRepr string) (uint64, bool, error) {
	panic("not supported")
}

func (st *KeyValueStoreWithChangelog) SetTrackParFunc(trackParFunc exactly_once_intr.TrackProdSubStreamFunc) {
	st.trackFunc = trackParFunc
}

func CreateInMemKVTableWithChangelog(mp *MaterializeParam,
	compare func(a, b treemap.Key) int,
) (*KeyValueStoreWithChangelog, error) {
	s := store.NewInMemoryKeyValueStore(mp.storeName, compare)
	return NewKeyValueStoreWithChangelog(mp, s, false)
}
