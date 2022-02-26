package store

import (
	"context"
	"sharedlog-stream/pkg/stream/processor/commtypes"
)

type KeyValueStoreWithChangelog struct {
	kvstore KeyValueStore
	mp      *MaterializeParam
}

func NewKeyValueStoreWithChangelog(mp *MaterializeParam, store KeyValueStore) *KeyValueStoreWithChangelog {
	return &KeyValueStoreWithChangelog{
		kvstore: store,
		mp:      mp,
	}
}

func (st *KeyValueStoreWithChangelog) Init(sctx StoreContext) {
	st.kvstore.Init(sctx)
	sctx.RegisterKeyValueStore(st)
}

func (st *KeyValueStoreWithChangelog) Name() string {
	return st.mp.StoreName
}

func (st *KeyValueStoreWithChangelog) IsOpen() bool {
	return st.kvstore.IsOpen()
}

func (st *KeyValueStoreWithChangelog) Get(ctx context.Context, key KeyT) (ValueT, bool, error) {
	return st.kvstore.Get(ctx, key)
}

func (st *KeyValueStoreWithChangelog) Put(ctx context.Context, key KeyT, value ValueT) error {
	keyBytes, err := st.mp.KeySerde.Encode(key)
	if err != nil {
		return err
	}
	valBytes, err := st.mp.ValueSerde.Encode(value)
	if err != nil {
		return err
	}
	encoded, err := st.mp.MsgSerde.Encode(keyBytes, valBytes)
	if err != nil {
		return err
	}
	_, err = st.mp.Changelog.Push(ctx, encoded, st.mp.ParNum, false)
	if err != nil {
		return err
	}
	err = st.kvstore.Put(ctx, key, value)
	return err
}

func (st *KeyValueStoreWithChangelog) PutIfAbsent(ctx context.Context, key KeyT, value ValueT) (ValueT, error) {
	origVal, exists, err := st.kvstore.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if !exists {
		keyBytes, err := st.mp.KeySerde.Encode(key)
		if err != nil {
			return nil, err
		}
		valBytes, err := st.mp.ValueSerde.Encode(value)
		if err != nil {
			return nil, err
		}
		encoded, err := st.mp.MsgSerde.Encode(keyBytes, valBytes)
		if err != nil {
			return nil, err
		}
		_, err = st.mp.Changelog.Push(ctx, encoded, st.mp.ParNum, false)
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

func (st *KeyValueStoreWithChangelog) Delete(ctx context.Context, key KeyT) error {
	keyBytes, err := st.mp.KeySerde.Encode(key)
	if err != nil {
		return err
	}
	encoded, err := st.mp.MsgSerde.Encode(keyBytes, nil)
	if err != nil {
		return err
	}
	_, err = st.mp.Changelog.Push(ctx, encoded, st.mp.ParNum, false)
	if err != nil {
		return err
	}
	return st.kvstore.Delete(ctx, key)
}

func (st *KeyValueStoreWithChangelog) ApproximateNumEntries(ctx context.Context) (uint64, error) {
	return st.kvstore.ApproximateNumEntries(ctx)
}

func (st *KeyValueStoreWithChangelog) Range(from KeyT, to KeyT, iterFunc func(KeyT, ValueT) error) error {
	return st.kvstore.Range(from, to, iterFunc)
}

func (st *KeyValueStoreWithChangelog) ReverseRange(from KeyT, to KeyT, iterFunc func(KeyT, ValueT) error) error {
	return st.kvstore.ReverseRange(from, to, iterFunc)
}

func (st *KeyValueStoreWithChangelog) PrefixScan(prefix interface{},
	prefixKeyEncoder commtypes.Encoder,
	iterFunc func(KeyT, ValueT) error,
) error {
	panic("not implemented")
}
