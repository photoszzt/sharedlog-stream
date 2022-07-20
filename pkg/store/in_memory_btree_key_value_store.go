package store

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/exactly_once_intr"
	"sharedlog-stream/pkg/utils"
	"sync"

	"github.com/google/btree"
)

type InMemoryBTreeKeyValueStore[KeyT, ValT any] struct {
	mux   sync.Mutex
	store *btree.BTreeG[kvPair[KeyT, ValT]]
	name  string
}

type kvPair[KeyT, ValT any] struct {
	key KeyT
	val ValT
}

/*
func (kv kvPair) Less(than btree.Item) bool {
	return kv.key.Less(than)
}

type StringItem string

func (a StringItem) Less(than btree.Item) bool {
	other, ok := than.(StringItem)
	if !ok {
		kvItem := than.(kvPair)
		other = kvItem.key.(StringItem)
	}
	return strings.Compare(string(a), string(other)) < 0
}

type Uint64Item uint64

func (a Uint64Item) Less(than btree.Item) bool {
	other, ok := than.(Uint64Item)
	if !ok {
		kvItem := than.(kvPair)
		other = kvItem.key.(Uint64Item)
	}
	return a < other
}
*/

var _ = KeyValueStore[int, int](&InMemoryBTreeKeyValueStore[int, int]{})

func NewInMemoryBTreeKeyValueStore[KeyT, ValT any](name string, less btree.LessFunc[KeyT]) *InMemoryBTreeKeyValueStore[KeyT, ValT] {
	return &InMemoryBTreeKeyValueStore[KeyT, ValT]{
		name: name,
		store: btree.NewG(2, func(a, b kvPair[KeyT, ValT]) bool {
			return less(a.key, b.key)
		}),
	}
}

func (st *InMemoryBTreeKeyValueStore[KeyT, ValT]) Name() string {
	return st.name
}

func (st *InMemoryBTreeKeyValueStore[KeyT, ValT]) Get(ctx context.Context, key KeyT) (ValT, bool, error) {
	st.mux.Lock()
	defer st.mux.Unlock()
	ret, exists := st.store.Get(kvPair[KeyT, ValT]{key: key})
	return ret.val, exists, nil
}

func (st *InMemoryBTreeKeyValueStore[KeyT, ValT]) Put(ctx context.Context, key KeyT, value ValT) error {
	st.mux.Lock()
	defer st.mux.Unlock()
	if utils.IsNil(value) {
		st.store.Delete(kvPair[KeyT, ValT]{key: key})
	} else {
		st.store.ReplaceOrInsert(kvPair[KeyT, ValT]{key: key, val: value})
	}
	return nil
}

func (st *InMemoryBTreeKeyValueStore[KeyT, ValT]) PutIfAbsent(ctx context.Context, key KeyT, value ValT) (ValT, error) {
	st.mux.Lock()
	defer st.mux.Unlock()
	originalVal, exists := st.store.Get(kvPair[KeyT, ValT]{key: key})
	if !exists {
		st.store.ReplaceOrInsert(kvPair[KeyT, ValT]{key: key, val: value})
	}
	return originalVal.val, nil
}

func (st *InMemoryBTreeKeyValueStore[KeyT, ValT]) PutWithoutPushToChangelog(ctx context.Context, key KeyT, value ValT) error {
	return st.Put(ctx, key, value)
}

func (st *InMemoryBTreeKeyValueStore[KeyT, ValT]) PutAll(ctx context.Context, kvs []*commtypes.Message[KeyT, ValT]) error {
	for _, kv := range kvs {
		st.Put(ctx, kv.Key, kv.Value)
	}
	return nil
}

func (st *InMemoryBTreeKeyValueStore[KeyT, ValT]) Delete(ctx context.Context, key KeyT) error {
	st.mux.Lock()
	defer st.mux.Unlock()
	st.store.Delete(kvPair[KeyT, ValT]{key: key})
	return nil
}

func (st *InMemoryBTreeKeyValueStore[KeyT, ValT]) ApproximateNumEntries() (uint64, error) {
	st.mux.Lock()
	defer st.mux.Unlock()
	return uint64(st.store.Len()), nil
}

func (st *InMemoryBTreeKeyValueStore[KeyT, ValT]) Range(ctx context.Context, from KeyT, to KeyT, iterFunc func(KeyT, ValT) error) error {
	st.mux.Lock()
	defer st.mux.Unlock()
	if utils.IsNil(from) && utils.IsNil(to) {
		st.store.Ascend(func(kv kvPair[KeyT, ValT]) bool {
			err := iterFunc(kv.key, kv.val)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[Error] Range: %v\n", err)
				return false
			}
			return true
		})
	} else if utils.IsNil(from) && !utils.IsNil(to) {
		st.store.AscendLessThan(kvPair[KeyT, ValT]{key: to}, func(kv kvPair[KeyT, ValT]) bool {
			err := iterFunc(kv.key, kv.val)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[Error] Range: %v\n", err)
				return false
			}
			return true
		})
	} else if !utils.IsNil(from) && utils.IsNil(to) {
		st.store.AscendGreaterOrEqual(kvPair[KeyT, ValT]{key: from}, func(kv kvPair[KeyT, ValT]) bool {
			err := iterFunc(kv.key, kv.val)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[Error] Range: %v\n", err)
				return false
			}
			return true
		})
	} else {
		st.store.AscendRange(kvPair[KeyT, ValT]{key: from}, kvPair[KeyT, ValT]{key: to}, func(kv kvPair[KeyT, ValT]) bool {
			err := iterFunc(kv.key, kv.val)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[Error] Range: %v\n", err)
				return false
			}
			return true
		})
	}
	return nil
}

func (st *InMemoryBTreeKeyValueStore[KeyT, ValT]) ReverseRange(from KeyT, to KeyT, iterFunc func(KeyT, ValT) error) error {
	panic("not implemented")
}

func (st *InMemoryBTreeKeyValueStore[KeyT, ValT]) TableType() TABLE_TYPE {
	return IN_MEM
}

func (st *InMemoryBTreeKeyValueStore[KeyT, ValT]) StartTransaction(ctx context.Context) error {
	panic("not supported")
}

func (st *InMemoryBTreeKeyValueStore[KeyT, ValT]) CommitTransaction(ctx context.Context, taskRepr string, transactionID uint64) error {
	panic("not supported")
}

func (st *InMemoryBTreeKeyValueStore[KeyT, ValT]) AbortTransaction(ctx context.Context) error {
	panic("not supported")
}
func (st *InMemoryBTreeKeyValueStore[KeyT, ValT]) GetTransactionID(ctx context.Context, taskRepr string) (uint64, bool, error) {
	panic("not supported")
}

func (st *InMemoryBTreeKeyValueStore[KeyT, ValT]) SetTrackParFunc(exactly_once_intr.TrackProdSubStreamFunc) {
}
