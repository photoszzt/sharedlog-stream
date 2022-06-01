package store_with_changelog

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/transaction/tran_interface"
	"sharedlog-stream/pkg/treemap"
	"time"
)

type KeyValueStoreWithChangelog struct {
	kvstore   *store.InMemoryKeyValueStore
	mp        *MaterializeParam
	use_bytes bool
}

func NewKeyValueStoreWithChangelog(mp *MaterializeParam, store *store.InMemoryKeyValueStore, use_bytes bool) *KeyValueStoreWithChangelog {
	return &KeyValueStoreWithChangelog{
		kvstore:   store,
		mp:        mp,
		use_bytes: use_bytes,
	}
}

func (st *KeyValueStoreWithChangelog) Init(sctx store.StoreContext) {
	st.kvstore.Init(sctx)
	sctx.RegisterKeyValueStore(st)
}

func (st *KeyValueStoreWithChangelog) MaterializeParam() *MaterializeParam {
	return st.mp
}

func (st *KeyValueStoreWithChangelog) Name() string {
	return st.mp.storeName
}

func (st *KeyValueStoreWithChangelog) IsOpen() bool {
	return st.kvstore.IsOpen()
}

func (st *KeyValueStoreWithChangelog) Get(ctx context.Context, key commtypes.KeyT) (commtypes.ValueT, bool, error) {
	if st.use_bytes {
		keyBytes, err := st.mp.kvMsgSerdes.KeySerde.Encode(key)
		if err != nil {
			return nil, false, err
		}
		valBytes, ok, err := st.kvstore.Get(ctx, keyBytes)
		if err != nil {
			return nil, ok, err
		}
		val, err := st.mp.kvMsgSerdes.ValSerde.Decode(valBytes.([]byte))
		return val, ok, err
	}
	return st.kvstore.Get(ctx, key)
}

func (st *KeyValueStoreWithChangelog) FlushChangelog(ctx context.Context) error {
	return st.mp.ChangelogManager().Flush(ctx)
}

func (st *KeyValueStoreWithChangelog) Put(ctx context.Context, key commtypes.KeyT, value commtypes.ValueT) error {
	keyBytes, err := st.mp.kvMsgSerdes.KeySerde.Encode(key)
	if err != nil {
		return err
	}
	valBytes, err := st.mp.kvMsgSerdes.ValSerde.Encode(value)
	if err != nil {
		return err
	}
	encoded, err := st.mp.kvMsgSerdes.MsgSerde.Encode(keyBytes, valBytes)
	if err != nil {
		return err
	}
	changelogManager := st.mp.ChangelogManager()
	err = changelogManager.Push(ctx, encoded, st.mp.parNum)
	if err != nil {
		return err
	}
	err = st.mp.trackFunc(ctx, key, st.mp.kvMsgSerdes.KeySerde, changelogManager.TopicName(), st.mp.parNum)
	if err != nil {
		return err
	}
	if st.use_bytes {
		err = st.kvstore.Put(ctx, keyBytes, valBytes)
	} else {
		err = st.kvstore.Put(ctx, key, value)
	}
	return err
}

func (st *KeyValueStoreWithChangelog) PutIfAbsent(ctx context.Context, key commtypes.KeyT, value commtypes.ValueT) (commtypes.ValueT, error) {
	origVal, exists, err := st.kvstore.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if !exists {
		keyBytes, err := st.mp.kvMsgSerdes.KeySerde.Encode(key)
		if err != nil {
			return nil, err
		}
		valBytes, err := st.mp.kvMsgSerdes.ValSerde.Encode(value)
		if err != nil {
			return nil, err
		}
		encoded, err := st.mp.kvMsgSerdes.MsgSerde.Encode(keyBytes, valBytes)
		if err != nil {
			return nil, err
		}
		err = st.mp.ChangelogManager().Push(ctx, encoded, st.mp.parNum)
		if err != nil {
			return nil, err
		}
		err = st.kvstore.Put(ctx, key, value)
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
	keyBytes, err := st.mp.kvMsgSerdes.KeySerde.Encode(key)
	if err != nil {
		return err
	}
	encoded, err := st.mp.kvMsgSerdes.MsgSerde.Encode(keyBytes, nil)
	if err != nil {
		return err
	}
	err = st.mp.ChangelogManager().Push(ctx, encoded, st.mp.parNum)
	if err != nil {
		return err
	}
	return st.kvstore.Delete(ctx, key)
}

func (st *KeyValueStoreWithChangelog) ApproximateNumEntries(ctx context.Context) (uint64, error) {
	return st.kvstore.ApproximateNumEntries(ctx)
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

func (st *KeyValueStoreWithChangelog) SetTrackParFunc(trackParFunc tran_interface.TrackKeySubStreamFunc) {
	st.mp.SetTrackParFunc(trackParFunc)
}

func ToInMemKVTableWithChangelog(storeName string, mp *MaterializeParam,
	compare func(a, b treemap.Key) int, warmup time.Duration,
) (*processor.MeteredProcessor, store.KeyValueStore, error) {
	s := store.NewInMemoryKeyValueStore(storeName, compare)
	tabWithLog := NewKeyValueStoreWithChangelog(mp, s, false)
	toTableProc := processor.NewMeteredProcessor(processor.NewStoreToKVTableProcessor(tabWithLog), warmup)
	return toTableProc, tabWithLog, nil
}

func CreateInMemKVTableWithChangelog(mp *MaterializeParam,
	compare func(a, b treemap.Key) int, warmup time.Duration,
) *KeyValueStoreWithChangelog {
	s := store.NewInMemoryKeyValueStore(mp.storeName, compare)
	tabWithLog := NewKeyValueStoreWithChangelog(mp, s, false)
	return tabWithLog
}
