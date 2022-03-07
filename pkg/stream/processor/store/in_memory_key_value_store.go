package store

import (
	"context"
	"fmt"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/treemap"
)

type InMemoryKeyValueStore struct {
	sctx    StoreContext
	store   *treemap.TreeMap
	compare func(a treemap.Key, b treemap.Value) int
	name    string
	open    bool
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

func (st *InMemoryKeyValueStore) Init(sctx StoreContext) {
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

func (st *InMemoryKeyValueStore) Get(ctx context.Context, key KeyT) (ValueT, bool, error) {
	val, ok := st.store.Get(key)
	return val, ok, nil
}

func (st *InMemoryKeyValueStore) Put(ctx context.Context, key KeyT, value ValueT) error {
	if value == nil {
		st.store.Del(key)
	} else {
		st.store.Set(key, value)
	}
	return nil
}

func (st *InMemoryKeyValueStore) PutIfAbsent(ctx context.Context, key KeyT, value ValueT) (ValueT, error) {
	originalVal, exists := st.store.Get(key)
	if !exists {
		st.store.Set(key, value)
	}
	return originalVal, nil
}

func (st *InMemoryKeyValueStore) PutAll(ctx context.Context, entries []*commtypes.Message) error {
	for _, msg := range entries {
		st.store.Set(msg.Key, msg.Value)
	}
	return nil
}

func (st *InMemoryKeyValueStore) Delete(ctx context.Context, key KeyT) error {
	st.store.Del(key)
	return nil
}

func (st *InMemoryKeyValueStore) ApproximateNumEntries(ctx context.Context) (uint64, error) {
	return uint64(st.store.Len()), nil
}

func (st *InMemoryKeyValueStore) Range(ctx context.Context, from KeyT, to KeyT, iterFunc func(KeyT, ValueT) error) error {
	if st.compare(from, to) > 0 {
		return fmt.Errorf("from should be smaller or equal to to")
	}
	if from == nil && to == nil {
		it := st.store.Iterator()
		for ; it.Valid(); it.Next() {
			err := iterFunc(it.Key(), it.Value())
			if err != nil {
				return err
			}
		}
	} else if from == nil && to != nil {
		it := st.store.UpperBound(to)
		for ; it.Valid(); it.Next() {
			err := iterFunc(it.Key(), it.Value())
			if err != nil {
				return err
			}
		}
	} else if from != nil && to == nil {
		it := st.store.LowerBound(from)
		for ; it.Valid(); it.Next() {
			err := iterFunc(it.Key(), it.Value())
			if err != nil {
				return err
			}
		}
	} else {
		it, end := st.store.Range(from, to)
		for ; it != end; it.Next() {
			err := iterFunc(it.Key(), it.Value())
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (st *InMemoryKeyValueStore) ReverseRange(from KeyT, to KeyT, iterFunc func(KeyT, ValueT) error) error {
	if st.compare(from, to) < 0 {
		return fmt.Errorf("from > to")
	}
	if from == nil && to == nil {
		it := st.store.Reverse()
		for ; it.Valid(); it.Next() {
			err := iterFunc(it.Key(), it.Value())
			if err != nil {
				return err
			}
		}
	} else if from == nil && to != nil {
		it := st.store.ReverseUpperBound(to)
		for ; it.Valid(); it.Next() {
			err := iterFunc(it.Key(), it.Value())
			if err != nil {
				return err
			}
		}
	} else if from != nil && to == nil {
		it := st.store.ReverseLowerBound(from)
		for ; it.Valid(); it.Next() {
			err := iterFunc(it.Key(), it.Value())
			if err != nil {
				return err
			}
		}
	} else {
		it, end := st.store.ReverseRange(from, to)
		for ; it != end; it.Next() {
			err := iterFunc(it.Key(), it.Value())
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (st *InMemoryKeyValueStore) PrefixScan(prefix interface{}, prefixKeyEncoder commtypes.Encoder,
	iterFunc func(KeyT, ValueT) error,
) error {
	panic("not implemented")
}
