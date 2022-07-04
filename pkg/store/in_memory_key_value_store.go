package store

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/exactly_once_intr"
	"sharedlog-stream/pkg/treemap"
	"sharedlog-stream/pkg/utils"
	"strings"
)

type InMemoryKeyValueStore struct {
	sctx  StoreContext
	store *treemap.TreeMap
	name  string
	open  bool
}

var _ = KeyValueStore(NewInMemoryKeyValueStore("a", nil))

type KVStoreCompareFunc func(a treemap.Key, b treemap.Key) int

func Uint64KeyKVStoreCompare(a, b treemap.Key) int {
	ka := a.(uint64)
	kb := b.(uint64)
	if ka > kb {
		return 1
	} else if ka == kb {
		return 0
	} else {
		return -1
	}
}

func StringKeyKVStoreCompare(a, b treemap.Key) int {
	a1 := a.(string)
	b1 := b.(string)
	return strings.Compare(a1, b1)
}

func NewInMemoryKeyValueStore(name string, compare KVStoreCompareFunc) *InMemoryKeyValueStore {
	return &InMemoryKeyValueStore{
		name: name,
		store: treemap.New(func(a treemap.Key, b treemap.Key) bool {
			return compare(a, b) < 0
		}),
	}
}

func (st *InMemoryKeyValueStore) Name() string {
	return st.name
}

func (st *InMemoryKeyValueStore) Get(ctx context.Context, key commtypes.KeyT) (commtypes.ValueT, bool, error) {
	val, ok := st.store.Get(key)
	return val, ok, nil
}

func (st *InMemoryKeyValueStore) Put(ctx context.Context, key commtypes.KeyT, value commtypes.ValueT) error {
	if utils.IsNil(value) {
		st.store.Del(key)
	} else {
		if key != nil {
			st.store.Set(key, value)
		}
	}
	return nil
}

func (st *InMemoryKeyValueStore) PutWithoutPushToChangelog(ctx context.Context, key commtypes.KeyT, value commtypes.ValueT) error {
	return st.Put(ctx, key, value)
}

func (st *InMemoryKeyValueStore) PutIfAbsent(ctx context.Context, key commtypes.KeyT, value commtypes.ValueT) (commtypes.ValueT, error) {
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

func (st *InMemoryKeyValueStore) Delete(ctx context.Context, key commtypes.KeyT) error {
	st.store.Del(key)
	return nil
}

func (st *InMemoryKeyValueStore) ApproximateNumEntries(ctx context.Context) (uint64, error) {
	return uint64(st.store.Len()), nil
}

func (st *InMemoryKeyValueStore) Range(ctx context.Context, from commtypes.KeyT, to commtypes.KeyT, iterFunc func(commtypes.KeyT, commtypes.ValueT) error) error {
	if utils.IsNil(from) && utils.IsNil(to) {
		it := st.store.Iterator()
		for ; it.Valid(); it.Next() {
			err := iterFunc(it.Key(), it.Value())
			if err != nil {
				return err
			}
		}
	} else if utils.IsNil(from) && !utils.IsNil(to) {
		it := st.store.UpperBound(to)
		for ; it.Valid(); it.Next() {
			err := iterFunc(it.Key(), it.Value())
			if err != nil {
				return err
			}
		}
	} else if !utils.IsNil(from) && utils.IsNil(to) {
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

func (st *InMemoryKeyValueStore) ReverseRange(from commtypes.KeyT, to commtypes.KeyT, iterFunc func(commtypes.KeyT, commtypes.ValueT) error) error {
	if utils.IsNil(from) && utils.IsNil(to) {
		it := st.store.Reverse()
		for ; it.Valid(); it.Next() {
			err := iterFunc(it.Key(), it.Value())
			if err != nil {
				return err
			}
		}
	} else if utils.IsNil(from) && !utils.IsNil(to) {
		it := st.store.ReverseUpperBound(to)
		for ; it.Valid(); it.Next() {
			err := iterFunc(it.Key(), it.Value())
			if err != nil {
				return err
			}
		}
	} else if !utils.IsNil(from) && utils.IsNil(to) {
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
	iterFunc func(commtypes.KeyT, commtypes.ValueT) error,
) error {
	panic("not implemented")
}

func (st *InMemoryKeyValueStore) TableType() TABLE_TYPE {
	return IN_MEM
}

func (st *InMemoryKeyValueStore) StartTransaction(ctx context.Context) error {
	panic("not supported")
}

func (st *InMemoryKeyValueStore) CommitTransaction(ctx context.Context, taskRepr string, transactionID uint64) error {
	panic("not supported")
}

func (st *InMemoryKeyValueStore) AbortTransaction(ctx context.Context) error { panic("not supported") }
func (st *InMemoryKeyValueStore) GetTransactionID(ctx context.Context, taskRepr string) (uint64, bool, error) {
	panic("not supported")
}

func (st *InMemoryKeyValueStore) SetTrackParFunc(exactly_once_intr.TrackProdSubStreamFunc) {
}
