package store

import (
	"context"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/treemap"
)

type InMemoryKeyValueStoreWithChangelog struct {
	kvstore *InMemoryKeyValueStore
	mp      *MaterializeParam
}

func NewInMemoryKeyValueStoreWithChangelog(mp *MaterializeParam, compare func(a, b treemap.Key) int) *InMemoryKeyValueStoreWithChangelog {
	return &InMemoryKeyValueStoreWithChangelog{
		kvstore: NewInMemoryKeyValueStore(mp.StoreName, compare),
		mp:      mp,
	}
}

func (st *InMemoryKeyValueStoreWithChangelog) Init(sctx StoreContext) {
	st.kvstore.Init(sctx)
	sctx.RegisterKeyValueStore(st)
}

func (st *InMemoryKeyValueStoreWithChangelog) Name() string {
	return st.mp.StoreName
}

func (st *InMemoryKeyValueStoreWithChangelog) IsOpen() bool {
	return st.kvstore.IsOpen()
}

func (st *InMemoryKeyValueStoreWithChangelog) Get(key KeyT) (ValueT, bool, error) {
	return st.kvstore.Get(key)
}

func (st *InMemoryKeyValueStoreWithChangelog) Put(ctx context.Context, key KeyT, value ValueT) error {
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

func (st *InMemoryKeyValueStoreWithChangelog) PutIfAbsent(ctx context.Context, key KeyT, value ValueT) (ValueT, error) {
	origVal, exists, err := st.kvstore.Get(key)
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
		st.kvstore.store.Set(key, value)
		return nil, nil
	}
	return origVal, nil
}

func (st *InMemoryKeyValueStoreWithChangelog) PutAll(ctx context.Context, entries []*commtypes.Message) error {
	for _, msg := range entries {
		err := st.Put(ctx, msg.Key, msg.Value)
		if err != nil {
			return err
		}
	}
	return nil
}

func (st *InMemoryKeyValueStoreWithChangelog) Delete(ctx context.Context, key KeyT) error {
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

func (st *InMemoryKeyValueStoreWithChangelog) ApproximateNumEntries() uint64 {
	return st.kvstore.ApproximateNumEntries()
}

func (st *InMemoryKeyValueStoreWithChangelog) Range(from KeyT, to KeyT) KeyValueIterator {
	return st.kvstore.Range(from, to)
}

func (st *InMemoryKeyValueStoreWithChangelog) ReverseRange(from KeyT, to KeyT) KeyValueIterator {
	return st.kvstore.ReverseRange(from, to)
}

func (st *InMemoryKeyValueStoreWithChangelog) PrefixScan(prefix interface{}, prefixKeyEncoder commtypes.Encoder) KeyValueIterator {
	panic("not implemented")
}
