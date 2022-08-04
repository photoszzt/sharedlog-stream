package store

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/exactly_once_intr"
	"sharedlog-stream/pkg/utils"
	"sync"

	"4d63.com/optional"
	"github.com/google/btree"
)

type kvPairG[K, V any] struct {
	key K
	val V
}

type InMemoryBTreeKeyValueStoreG[K, V any] struct {
	mux   sync.Mutex
	store *btree.BTreeG[kvPairG[K, V]]
	name  string
}

var _ = CoreKeyValueStoreG[int, int](&InMemoryBTreeKeyValueStoreG[int, int]{})

type LessFunc[K any] func(k1, k2 K) bool

func NewInMemoryBTreeKeyValueStoreG[K, V any](name string, lessFunc LessFunc[K]) *InMemoryBTreeKeyValueStoreG[K, V] {
	return &InMemoryBTreeKeyValueStoreG[K, V]{
		name: name,
		store: btree.NewG(2, btree.LessFunc[kvPairG[K, V]](
			func(a, b kvPairG[K, V]) bool {
				return lessFunc(a.key, b.key)
			})),
	}
}

func (st *InMemoryBTreeKeyValueStoreG[K, V]) Name() string {
	return st.name
}

func (st *InMemoryBTreeKeyValueStoreG[K, V]) Get(ctx context.Context, key K) (V, bool, error) {
	st.mux.Lock()
	defer st.mux.Unlock()
	ret, exists := st.store.Get(kvPairG[K, V]{key: key})
	return ret.val, exists, nil
}

func (st *InMemoryBTreeKeyValueStoreG[K, V]) Put(ctx context.Context, key K, value V) error {
	st.mux.Lock()
	defer st.mux.Unlock()
	if utils.IsNil(value) {
		st.store.Delete(kvPairG[K, V]{key: key})
	} else {
		st.store.ReplaceOrInsert(kvPairG[K, V]{key: key, val: value})
	}
	return nil
}

func (st *InMemoryBTreeKeyValueStoreG[K, V]) PutIfAbsent(ctx context.Context, key K, value V) (optional.Optional[V], error) {
	st.mux.Lock()
	defer st.mux.Unlock()
	originalKV, exists := st.store.Get(kvPairG[K, V]{key: key})
	if !exists {
		st.store.ReplaceOrInsert(kvPairG[K, V]{key: key, val: value})
		return optional.Empty[V](), nil
	}
	return optional.Of(originalKV.val), nil
}

func (st *InMemoryBTreeKeyValueStoreG[K, V]) PutWithoutPushToChangelog(ctx context.Context, key K, value V) error {
	return st.Put(ctx, key, value)
}

func (st *InMemoryBTreeKeyValueStoreG[K, V]) PutAll(ctx context.Context, kvs []*commtypes.Message) error {
	for _, kv := range kvs {
		err := st.Put(ctx, kv.Key.(K), kv.Value.(V))
		if err != nil {
			return err
		}
	}
	return nil
}

func (st *InMemoryBTreeKeyValueStoreG[K, V]) Delete(ctx context.Context, key K) error {
	st.mux.Lock()
	defer st.mux.Unlock()
	st.store.Delete(kvPairG[K, V]{key: key})
	return nil
}

func (st *InMemoryBTreeKeyValueStoreG[K, V]) ApproximateNumEntries() (uint64, error) {
	st.mux.Lock()
	defer st.mux.Unlock()
	return uint64(st.store.Len()), nil
}

func (st *InMemoryBTreeKeyValueStoreG[K, V]) Range(ctx context.Context, from K, to K, iterFunc func(K, V) error) error {
	st.mux.Lock()
	defer st.mux.Unlock()
	if utils.IsNil(from) && utils.IsNil(to) {
		st.store.Ascend(btree.ItemIteratorG[kvPairG[K, V]](func(kv kvPairG[K, V]) bool {
			err := iterFunc(kv.key, kv.val)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[Error] Range: %v\n", err)
				return false
			}
			return true
		}))
	} else if utils.IsNil(from) && !utils.IsNil(to) {
		st.store.AscendLessThan(kvPairG[K, V]{key: to},
			btree.ItemIteratorG[kvPairG[K, V]](func(kv kvPairG[K, V]) bool {
				err := iterFunc(kv.key, kv.val)
				if err != nil {
					fmt.Fprintf(os.Stderr, "[Error] Range: %v\n", err)
					return false
				}
				return true
			}))
	} else if !utils.IsNil(from) && utils.IsNil(to) {
		st.store.AscendGreaterOrEqual(kvPairG[K, V]{key: from},
			btree.ItemIteratorG[kvPairG[K, V]](func(kv kvPairG[K, V]) bool {
				err := iterFunc(kv.key, kv.val)
				if err != nil {
					fmt.Fprintf(os.Stderr, "[Error] Range: %v\n", err)
					return false
				}
				return true
			}))
	} else {
		st.store.AscendRange(kvPairG[K, V]{key: from}, kvPairG[K, V]{key: to},
			btree.ItemIteratorG[kvPairG[K, V]](func(kv kvPairG[K, V]) bool {
				err := iterFunc(kv.key, kv.val)
				if err != nil {
					fmt.Fprintf(os.Stderr, "[Error] Range: %v\n", err)
					return false
				}
				return true
			}))
	}
	return nil
}

func (st *InMemoryBTreeKeyValueStoreG[K, V]) TableType() TABLE_TYPE {
	return IN_MEM
}

func (st *InMemoryBTreeKeyValueStoreG[K, V]) SetTrackParFunc(exactly_once_intr.TrackProdSubStreamFunc) {
}

func (st *InMemoryBTreeKeyValueStoreG[K, V]) FlushChangelog(ctx context.Context) error {
	return nil
}

func (st *InMemoryBTreeKeyValueStoreG[K, V]) ConsumeChangelog(ctx context.Context, parNum uint8) (*commtypes.MsgAndSeqs, error) {
	return nil, nil
}
