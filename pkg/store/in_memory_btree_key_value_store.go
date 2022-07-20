package store

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/exactly_once_intr"
	"sharedlog-stream/pkg/utils"
	"strings"
	"sync"

	"github.com/google/btree"
)

type InMemoryBTreeKeyValueStore struct {
	mux   sync.Mutex
	store *btree.BTree
	name  string
}

type kvPair struct {
	key btree.Item
	val commtypes.ValueT
}

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

var _ = KeyValueStore(&InMemoryBTreeKeyValueStore{})

func NewInMemoryBTreeKeyValueStore(name string) *InMemoryBTreeKeyValueStore {
	return &InMemoryBTreeKeyValueStore{
		name:  name,
		store: btree.New(2),
	}
}

func (st *InMemoryBTreeKeyValueStore) Name() string {
	return st.name
}

func (st *InMemoryBTreeKeyValueStore) Get(ctx context.Context, key commtypes.KeyT) (commtypes.ValueT, bool, error) {
	st.mux.Lock()
	defer st.mux.Unlock()
	ret := st.store.Get(key.(btree.Item))
	return ret, ret != nil, nil
}

func (st *InMemoryBTreeKeyValueStore) Put(ctx context.Context, key commtypes.KeyT, value commtypes.ValueT) error {
	st.mux.Lock()
	defer st.mux.Unlock()
	if utils.IsNil(value) {
		st.store.Delete(key.(btree.Item))
	} else {
		st.store.ReplaceOrInsert(kvPair{key: key.(btree.Item), val: value})
	}
	return nil
}

func (st *InMemoryBTreeKeyValueStore) PutIfAbsent(ctx context.Context, key commtypes.KeyT, value commtypes.ValueT) (commtypes.ValueT, error) {
	st.mux.Lock()
	defer st.mux.Unlock()
	originalVal := st.store.Get(key.(btree.Item))
	if originalVal == nil {
		st.store.ReplaceOrInsert(kvPair{key: key.(btree.Item), val: value})
	}
	return originalVal, nil
}

func (st *InMemoryBTreeKeyValueStore) PutWithoutPushToChangelog(ctx context.Context, key commtypes.KeyT, value commtypes.ValueT) error {
	return st.Put(ctx, key, value)
}

func (st *InMemoryBTreeKeyValueStore) PutAll(ctx context.Context, kvs []*commtypes.Message) error {
	for _, kv := range kvs {
		st.Put(ctx, kv.Key, kv.Value)
	}
	return nil
}

func (st *InMemoryBTreeKeyValueStore) Delete(ctx context.Context, key commtypes.KeyT) error {
	st.mux.Lock()
	defer st.mux.Unlock()
	st.store.Delete(key.(btree.Item))
	return nil
}

func (st *InMemoryBTreeKeyValueStore) ApproximateNumEntries() (uint64, error) {
	st.mux.Lock()
	defer st.mux.Unlock()
	return uint64(st.store.Len()), nil
}

func (st *InMemoryBTreeKeyValueStore) Range(ctx context.Context, from commtypes.KeyT, to commtypes.KeyT, iterFunc func(commtypes.KeyT, commtypes.ValueT) error) error {
	st.mux.Lock()
	defer st.mux.Unlock()
	if utils.IsNil(from) && utils.IsNil(to) {
		st.store.Ascend(func(item btree.Item) bool {
			kv := item.(kvPair)
			err := iterFunc(kv.key, kv.val)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[Error] Range: %v\n", err)
				return false
			}
			return true
		})
	} else if utils.IsNil(from) && !utils.IsNil(to) {
		st.store.AscendLessThan(to.(btree.Item), func(item btree.Item) bool {
			kv := item.(kvPair)
			err := iterFunc(kv.key, kv.val)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[Error] Range: %v\n", err)
				return false
			}
			return true
		})
	} else if !utils.IsNil(from) && utils.IsNil(to) {
		st.store.AscendGreaterOrEqual(from.(btree.Item), func(item btree.Item) bool {
			kv := item.(kvPair)
			err := iterFunc(kv.key, kv.val)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[Error] Range: %v\n", err)
				return false
			}
			return true
		})
	} else {
		st.store.AscendRange(from.(btree.Item), to.(btree.Item), func(item btree.Item) bool {
			kv := item.(kvPair)
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

func (st *InMemoryBTreeKeyValueStore) ReverseRange(from commtypes.KeyT, to commtypes.KeyT, iterFunc func(commtypes.KeyT, commtypes.ValueT) error) error {
	panic("not implemented")
}

func (st *InMemoryBTreeKeyValueStore) TableType() TABLE_TYPE {
	return IN_MEM
}

func (st *InMemoryBTreeKeyValueStore) SetTrackParFunc(exactly_once_intr.TrackProdSubStreamFunc) {
}
