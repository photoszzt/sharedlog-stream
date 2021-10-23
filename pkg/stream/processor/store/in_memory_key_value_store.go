package store

import (
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/treemap"

	"github.com/rs/zerolog/log"

	"golang.org/x/xerrors"
)

type InMemoryKeyValueStore struct {
	open    bool
	name    string
	store   *treemap.TreeMap
	sctx    ProcessorContext
	compare func(a treemap.Key, b treemap.Value) int
}

var _ = KeyValueStore(NewInMemoryKeyValueStore("a", nil))

func NewInMemoryKeyValueStore(name string, compare func(a treemap.Key, b treemap.Key) int) *InMemoryKeyValueStore {
	return &InMemoryKeyValueStore{
		name: name,
		store: treemap.New(func(a treemap.Key, b treemap.Key) bool {
			return compare(a, b) < 0
		}),
	}
}

func (st *InMemoryKeyValueStore) Init(sctx ProcessorContext) {
	st.sctx = sctx
	st.open = true
	st.sctx.RegisterKeyValueStore(st)
}

func (st *InMemoryKeyValueStore) Name() string {
	return st.name
}

func (st *InMemoryKeyValueStore) IsOpen() bool {
	return st.open
}

func (st *InMemoryKeyValueStore) Get(key KeyT) (ValueT, bool, error) {
	val, ok := st.store.Get(key)
	return val, ok, nil
}

func (st *InMemoryKeyValueStore) Put(key KeyT, value ValueT) error {
	if value == nil {
		st.store.Del(key)
	} else {
		st.store.Set(key, value)
	}
	return nil
}

func (st *InMemoryKeyValueStore) PutIfAbsent(key KeyT, value ValueT) (ValueT, error) {
	originalVal, exists := st.store.Get(key)
	if !exists {
		st.store.Set(key, value)
	}
	return originalVal, nil
}

func (st *InMemoryKeyValueStore) PutAll(entries []*commtypes.Message) error {
	for _, msg := range entries {
		st.store.Set(msg.Key, msg.Value)
	}
	return nil
}

func (st *InMemoryKeyValueStore) Delete(key KeyT) error {
	st.store.Del(key)
	return nil
}

func (st *InMemoryKeyValueStore) ApproximateNumEntries() uint64 {
	return uint64(st.store.Len())
}

func (st *InMemoryKeyValueStore) Range(from KeyT, to KeyT) KeyValueIterator {
	if st.compare(from, to) > 0 {
		log.Warn().Msgf("Returning empty iterator for invalid key range from > to")
		return EMPTY_KEY_VALUE_ITER
	}
	return NewInMemoryKeyValueForwardIterator(st.store, from, to)

}

func (st *InMemoryKeyValueStore) ReverseRange(from KeyT, to KeyT) KeyValueIterator {
	if st.compare(from, to) < 0 {
		log.Warn().Msgf("Returning empty iterator for invalid key range from > to")
		return EMPTY_KEY_VALUE_ITER
	}
	return NewInMemoryKeyValueReverseIterator(st.store, from, to)
}

func (st *InMemoryKeyValueStore) PrefixScan(prefix interface{}, prefixKeyEncoder commtypes.Encoder) KeyValueIterator {
	panic("not implemented")
	/*
		from, err := prefixKeyEncoder.Encode(prefix)
		if err != nil {
			log.Fatal().Err(err)
		}
		to, err := byte_slice_increment(from)
		if err != nil {
			log.Fatal().Err(err)
		}
		return NewInMemoryKeyValueForwardIterator(st.store, from, to)
	*/
}

type InMemoryKeyValueForwardIterator struct {
	fromIter treemap.ForwardIterator
	toIter   *treemap.ForwardIterator
}

func NewInMemoryKeyValueForwardIterator(store *treemap.TreeMap, from KeyT, to KeyT) InMemoryKeyValueForwardIterator {
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

type InMemoryKeyValueReverseIterator struct {
	fromIter treemap.ReverseIterator
	toIter   *treemap.ReverseIterator
}

func NewInMemoryKeyValueReverseIterator(store *treemap.TreeMap, from KeyT, to KeyT) InMemoryKeyValueReverseIterator {
	if from == nil && to == nil {
		iter := store.Reverse()
		return InMemoryKeyValueReverseIterator{
			fromIter: iter,
			toIter:   nil,
		}
	} else if from == nil {
		iter := store.ReverseUpperBound(to)
		return InMemoryKeyValueReverseIterator{
			fromIter: iter,
			toIter:   nil,
		}
	} else if to == nil {
		iter := store.ReverseLowerBound(from)
		return InMemoryKeyValueReverseIterator{
			fromIter: iter,
			toIter:   nil,
		}
	} else {
		fromIter, toIter := store.ReverseRange(from, to)
		return InMemoryKeyValueReverseIterator{
			fromIter: fromIter,
			toIter:   &toIter,
		}
	}
}

func (fi InMemoryKeyValueReverseIterator) HasNext() bool {
	return fi.fromIter.Valid()
}

func (fi InMemoryKeyValueReverseIterator) Next() KeyValue {
	fi.fromIter.Next()
	return KeyValue{
		Key:   fi.fromIter.Key(),
		Value: fi.fromIter.Value(),
	}
}

func (fi InMemoryKeyValueReverseIterator) Close() {
}

func (fi InMemoryKeyValueReverseIterator) PeekNextKey() KeyT {
	log.Fatal().Err(xerrors.New("PeekNextKey is not supported"))
	return nil
}
