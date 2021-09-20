package processor

import (
	"bytes"
	"sharedlog-stream/pkg/stream/processor/treemap"

	"github.com/rs/zerolog/log"
)

type InMemoryKeyValueStoreWithChangelog struct {
	kvstore    *InMemoryKeyValueStore
	keySerde   Serde
	valueSerde Serde
	logStore   LogStore
	msgSerde   MsgSerde
}

func NewInMemoryKeyValueStoreWithChangelog(name string, keySerde Serde, valSerde Serde, logStore LogStore) *InMemoryKeyValueStoreWithChangelog {

	return &InMemoryKeyValueStoreWithChangelog{
		kvstore: NewInMemoryKeyValueStore(name, func(a treemap.Key, b treemap.Key) int {
			valA := a.([]byte)
			valB := b.([]byte)
			return bytes.Compare(valA, valB)
		}),
		keySerde:   keySerde,
		valueSerde: valSerde,
		logStore:   logStore,
		msgSerde:   MessageSerializedMsgpSerde{},
	}
}

func (st *InMemoryKeyValueStoreWithChangelog) Init(sctx ProcessorContext, root KeyValueStore) {
	st.kvstore.Init(sctx, root)
}

func (st *InMemoryKeyValueStoreWithChangelog) Name() string {
	return st.kvstore.Name()
}

func (st *InMemoryKeyValueStoreWithChangelog) IsOpen() bool {
	return st.kvstore.IsOpen()
}

func (st *InMemoryKeyValueStoreWithChangelog) Get(key KeyT) (ValueT, bool, error) {
	keyBytes, err := st.keySerde.Encode(key)
	if err != nil {
		return nil, false, err
	}
	valBytes, ok, err := st.kvstore.Get(keyBytes)
	if err != nil {
		return nil, false, err
	}
	if ok {
		val, err := st.valueSerde.Decode(valBytes.([]byte))
		if err != nil {
			return nil, false, err
		}
		return val, ok, nil
	}
	return valBytes, ok, nil
}

func (st *InMemoryKeyValueStoreWithChangelog) Put(key KeyT, value ValueT) error {
	keyBytes, err := st.keySerde.Encode(key)
	if err != nil {
		return err
	}
	valBytes, err := st.valueSerde.Encode(value)
	if err != nil {
		return err
	}
	encoded, err := st.msgSerde.Encode(keyBytes, valBytes)
	if err != nil {
		return err
	}
	_, err = st.logStore.Push(encoded)
	if err != nil {
		return err
	}
	err = st.kvstore.Put(keyBytes, valBytes)
	return err
}

func (st *InMemoryKeyValueStoreWithChangelog) PutIfAbsent(key KeyT, value ValueT) (ValueT, error) {
	keyBytes, err := st.keySerde.Encode(key)
	if err != nil {
		return nil, err
	}
	origValBytes, exists, err := st.kvstore.Get(keyBytes)
	if !exists {
		valBytes, err := st.valueSerde.Encode(value)
		if err != nil {
			return nil, err
		}
		encoded, err := st.msgSerde.Encode(keyBytes, valBytes)
		if err != nil {
			return nil, err
		}
		_, err = st.logStore.Push(encoded)
		if err != nil {
			return nil, err
		}
		st.kvstore.store.Set(keyBytes, valBytes)
		return nil, nil
	}
	origVal, err := st.valueSerde.Decode(origValBytes.([]byte))
	if err != nil {
		return nil, err
	}
	return origVal, nil
}

func (st *InMemoryKeyValueStoreWithChangelog) PutAll(entries []*Message) error {
	for _, msg := range entries {
		err := st.Put(msg.Key, msg.Value)
		if err != nil {
			return err
		}
	}
	return nil
}

func (st *InMemoryKeyValueStoreWithChangelog) Delete(key KeyT) error {
	keyBytes, err := st.keySerde.Encode(key)
	if err != nil {
		return err
	}
	encoded, err := st.msgSerde.Encode(keyBytes, nil)
	if err != nil {
		return err
	}
	_, err = st.logStore.Push(encoded)
	if err != nil {
		return err
	}
	return st.kvstore.Delete(keyBytes)
}

func (st *InMemoryKeyValueStoreWithChangelog) ApproximateNumEntries() uint64 {
	return st.kvstore.ApproximateNumEntries()
}

func (st *InMemoryKeyValueStoreWithChangelog) Range(from KeyT, to KeyT) KeyValueIterator {
	fromBytes, err := st.keySerde.Encode(from)
	if err != nil {
		log.Fatal().Err(err)
	}
	toBytes, err := st.keySerde.Encode(to)
	if err != nil {
		log.Fatal().Err(err)
	}
	return st.kvstore.Range(fromBytes, toBytes)
}

func (st *InMemoryKeyValueStoreWithChangelog) ReverseRange(from KeyT, to KeyT) KeyValueIterator {
	fromBytes, err := st.keySerde.Encode(from)
	if err != nil {
		log.Fatal().Err(err)
	}
	toBytes, err := st.keySerde.Encode(to)
	if err != nil {
		log.Fatal().Err(err)
	}
	return st.kvstore.ReverseRange(fromBytes, toBytes)
}

func (st *InMemoryKeyValueStoreWithChangelog) PrefixScan(prefix interface{}, prefixKeyEncoder Encoder) KeyValueIterator {
	panic("not implemented")
}
