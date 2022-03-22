package store

import (
	"context"
	"sharedlog-stream/pkg/stream/processor/commtypes"
)

type KeyValueStoreWithChangelog struct {
	kvstore   KeyValueStore
	mp        *MaterializeParam
	use_bytes bool
}

func NewKeyValueStoreWithChangelog(mp *MaterializeParam, store KeyValueStore, use_bytes bool) *KeyValueStoreWithChangelog {
	return &KeyValueStoreWithChangelog{
		kvstore:   store,
		mp:        mp,
		use_bytes: use_bytes,
	}
}

func (st *KeyValueStoreWithChangelog) Init(sctx StoreContext) {
	st.kvstore.Init(sctx)
	sctx.RegisterKeyValueStore(st)
}

func (st *KeyValueStoreWithChangelog) MaterializeParam() *MaterializeParam {
	return st.mp
}

func (st *KeyValueStoreWithChangelog) Name() string {
	return st.mp.StoreName
}

func (st *KeyValueStoreWithChangelog) IsOpen() bool {
	return st.kvstore.IsOpen()
}

func (st *KeyValueStoreWithChangelog) Get(ctx context.Context, key commtypes.KeyT) (commtypes.ValueT, bool, error) {
	if st.use_bytes {
		keyBytes, err := st.mp.KeySerde.Encode(key)
		if err != nil {
			return nil, false, err
		}
		valBytes, ok, err := st.kvstore.Get(ctx, keyBytes)
		if err != nil {
			return nil, ok, err
		}
		val, err := st.mp.ValueSerde.Decode(valBytes.([]byte))
		return val, ok, err
	}
	return st.kvstore.Get(ctx, key)
}

func (st *KeyValueStoreWithChangelog) Put(ctx context.Context, key commtypes.KeyT, value commtypes.ValueT) error {
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

func (st *KeyValueStoreWithChangelog) Delete(ctx context.Context, key commtypes.KeyT) error {
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

func (st *KeyValueStoreWithChangelog) TableType() TABLE_TYPE {
	return st.kvstore.TableType()
}
