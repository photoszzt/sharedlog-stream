package state

import (
	"bytes"

	"github.com/rs/zerolog/log"

	"cs.utexas.edu/zhitingz/sharedlog-stream/pkg/stream/processor"
	"golang.org/x/xerrors"
)

//go:generate gotemplate "cs.utexas.edu/zhitingz/sharedlog-stream/pkg/stream/state/treemap" "BytesTreeMap([]byte, []byte)"

// not thread safe
type InMemoryKeyValueStore struct {
	open  bool
	name  string
	store *BytesTreeMap
}

var _ = KeyValueStore(NewInMemoryKeyValueStore("a"))

func NewInMemoryKeyValueStore(name string) *InMemoryKeyValueStore {
	return &InMemoryKeyValueStore{
		name: name,
		store: NewBytesTreeMap(func(a []byte, b []byte) bool {
			return bytes.Compare(a, b) < 0
		}),
	}
}

func (st *InMemoryKeyValueStore) Init(pctx processor.StateStoreContext, root processor.StateStore) {

}

func (st *InMemoryKeyValueStore) Name() string {
	return st.name
}

func (st *InMemoryKeyValueStore) IsOpen() bool {
	return st.open
}

func (st *InMemoryKeyValueStore) Get(key KeyT) (ValueT, bool) {
	k := key.([]byte)
	return st.store.Get(k)
}

func (st *InMemoryKeyValueStore) Put(key KeyT, value ValueT) {
	k := key.([]byte)
	v := value.([]byte)
	if value == nil {
		st.store.Del(k)
	} else {
		st.store.Set(k, v)
	}
}

func (st *InMemoryKeyValueStore) PutIfAbsent(key KeyT, value ValueT) ValueT {
	k := key.([]byte)
	v := value.([]byte)
	originalVal, exists := st.store.Get(k)
	if !exists {
		st.store.Set(k, v)
	}
	return originalVal
}

func (st *InMemoryKeyValueStore) PutAll(entries []*processor.Message) {
	for _, msg := range entries {
		k := msg.Key.([]byte)
		v := msg.Value.([]byte)
		st.store.Set(k, v)
	}
}

func (st *InMemoryKeyValueStore) Delete(key KeyT) {
	k := key.([]byte)
	st.store.Del(k)
}

func (st *InMemoryKeyValueStore) ApproximateNumEntries() uint64 {
	return uint64(st.store.Len())
}

func (st *InMemoryKeyValueStore) Range(from KeyT, to KeyT) KeyValueIterator {
	return nil
}

func (st *InMemoryKeyValueStore) ReverseRange(from KeyT, to KeyT) KeyValueIterator {
	return nil
}

func (st *InMemoryKeyValueStore) PrefixScan(prefix interface{}, prefixKeyEncoder processor.Encoder) KeyValueIterator {
	return nil
}

type InMemoryKeyValueForwardIterator struct {
	fromIter ForwardIteratorBytesTreeMap
	toIter   *ForwardIteratorBytesTreeMap
}

var _ = KeyValueIterator(NewInMemoryKeyValueForwardIterator(nil, nil, nil))

func NewInMemoryKeyValueForwardIterator(store *BytesTreeMap, from []byte, to []byte) InMemoryKeyValueForwardIterator {
	if from == nil && to == nil {
		iter := store.Iterator()
		return InMemoryKeyValueForwardIterator{
			fromIter: iter,
			toIter:   nil,
		}
	} else if from == nil {
		iter := store.UpperBound(to)
		return InMemoryKeyValueForwardIterator{
			fromIter: iter,
			toIter:   nil,
		}
	} else if to == nil {
		iter := store.LowerBound(from)
		return InMemoryKeyValueForwardIterator{
			fromIter: iter,
			toIter:   nil,
		}
	} else {
		fromIter, toIter := store.Range(from, to)
		return InMemoryKeyValueForwardIterator{
			fromIter: fromIter,
			toIter:   &toIter,
		}
	}
}

func (fi InMemoryKeyValueForwardIterator) HasNext() bool {
	return fi.fromIter.Valid()
}

func (fi InMemoryKeyValueForwardIterator) Next() KeyValue {
	fi.fromIter.Next()
	return KeyValue{
		Key:   fi.fromIter.Key(),
		Value: fi.fromIter.Value(),
	}
}

func (fi InMemoryKeyValueForwardIterator) Close() {
}

func (fi InMemoryKeyValueForwardIterator) PeekNextKey() KeyT {
	log.Fatal().Err(xerrors.New("PeekNextKey is not supported"))
	return nil
}
