package store

import (
	"bytes"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/treemap"

	"github.com/rs/zerolog/log"
)

type InMemoryKeyValueStoreWithChangelog struct {
	kvstore *InMemoryKeyValueStore
	mp      *MaterializeParam
}

func NewInMemoryKeyValueStoreWithChangelog(mp *MaterializeParam) *InMemoryKeyValueStoreWithChangelog {

	return &InMemoryKeyValueStoreWithChangelog{
		kvstore: NewInMemoryKeyValueStore(mp.StoreName+"_bytes", func(a treemap.Key, b treemap.Key) int {
			valA := a.([]byte)
			valB := b.([]byte)
			return bytes.Compare(valA, valB)
		}),
		mp: mp,
	}
}

func (st *InMemoryKeyValueStoreWithChangelog) Init(sctx ProcessorContext) {
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
	keyBytes, err := st.mp.KeySerde.Encode(key)
	if err != nil {
		return nil, false, err
	}
	valBytes, ok, err := st.kvstore.Get(keyBytes)
	if err != nil {
		return nil, false, err
	}
	if ok {
		val, err := st.mp.ValueSerde.Decode(valBytes.([]byte))
		if err != nil {
			return nil, false, err
		}
		return val, ok, nil
	}
	return valBytes, ok, nil
}

func (st *InMemoryKeyValueStoreWithChangelog) Put(key KeyT, value ValueT) error {
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
	_, err = st.mp.Changelog.Push(encoded, st.mp.ParNum)
	if err != nil {
		return err
	}
	err = st.kvstore.Put(keyBytes, valBytes)
	return err
}

func (st *InMemoryKeyValueStoreWithChangelog) PutIfAbsent(key KeyT, value ValueT) (ValueT, error) {
	keyBytes, err := st.mp.KeySerde.Encode(key)
	if err != nil {
		return nil, err
	}
	origValBytes, exists, err := st.kvstore.Get(keyBytes)
	if err != nil {
		return nil, err
	}
	if !exists {
		valBytes, err := st.mp.ValueSerde.Encode(value)
		if err != nil {
			return nil, err
		}
		encoded, err := st.mp.MsgSerde.Encode(keyBytes, valBytes)
		if err != nil {
			return nil, err
		}
		_, err = st.mp.Changelog.Push(encoded, st.mp.ParNum)
		if err != nil {
			return nil, err
		}
		st.kvstore.store.Set(keyBytes, valBytes)
		return nil, nil
	}
	origVal, err := st.mp.ValueSerde.Decode(origValBytes.([]byte))
	if err != nil {
		return nil, err
	}
	return origVal, nil
}

func (st *InMemoryKeyValueStoreWithChangelog) PutAll(entries []*commtypes.Message) error {
	for _, msg := range entries {
		err := st.Put(msg.Key, msg.Value)
		if err != nil {
			return err
		}
	}
	return nil
}

func (st *InMemoryKeyValueStoreWithChangelog) Delete(key KeyT) error {
	keyBytes, err := st.mp.KeySerde.Encode(key)
	if err != nil {
		return err
	}
	encoded, err := st.mp.MsgSerde.Encode(keyBytes, nil)
	if err != nil {
		return err
	}
	_, err = st.mp.Changelog.Push(encoded, st.mp.ParNum)
	if err != nil {
		return err
	}
	return st.kvstore.Delete(keyBytes)
}

func (st *InMemoryKeyValueStoreWithChangelog) ApproximateNumEntries() uint64 {
	return st.kvstore.ApproximateNumEntries()
}

func (st *InMemoryKeyValueStoreWithChangelog) Range(from KeyT, to KeyT) KeyValueIterator {
	fromBytes, err := st.mp.KeySerde.Encode(from)
	if err != nil {
		log.Fatal().Err(err)
	}
	toBytes, err := st.mp.KeySerde.Encode(to)
	if err != nil {
		log.Fatal().Err(err)
	}
	return st.kvstore.Range(fromBytes, toBytes)
}

func (st *InMemoryKeyValueStoreWithChangelog) ReverseRange(from KeyT, to KeyT) KeyValueIterator {
	fromBytes, err := st.mp.KeySerde.Encode(from)
	if err != nil {
		log.Fatal().Err(err)
	}
	toBytes, err := st.mp.KeySerde.Encode(to)
	if err != nil {
		log.Fatal().Err(err)
	}
	return st.kvstore.ReverseRange(fromBytes, toBytes)
}

func (st *InMemoryKeyValueStoreWithChangelog) PrefixScan(prefix interface{}, prefixKeyEncoder commtypes.Encoder) KeyValueIterator {
	panic("not implemented")
}
