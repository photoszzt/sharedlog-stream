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

func (st *InMemoryBTreeKeyValueStoreG[K, V]) Put(ctx context.Context, key K, value optional.Optional[V]) error {
	st.mux.Lock()
	defer st.mux.Unlock()
	v, ok := value.Get()
	if !ok {
		st.store.Delete(kvPairG[K, V]{key: key})
	} else {
		st.store.ReplaceOrInsert(kvPairG[K, V]{key: key, val: v})
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

func (st *InMemoryBTreeKeyValueStoreG[K, V]) PutWithoutPushToChangelog(ctx context.Context, key commtypes.KeyT, value commtypes.ValueT) error {
	if utils.IsNil(value) {
		return st.Put(ctx, key.(K), optional.Empty[V]())
	} else {
		return st.Put(ctx, key.(K), optional.Of(value.(V)))
	}
}

func (st *InMemoryBTreeKeyValueStoreG[K, V]) PutAll(ctx context.Context, kvs []*commtypes.Message) error {
	for _, kv := range kvs {
		var err error
		if utils.IsNil(kv.Value) {
			err = st.Put(ctx, kv.Key.(K), optional.Empty[V]())
		} else {
			err = st.Put(ctx, kv.Key.(K), optional.Of(kv.Value.(V)))
		}
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

func (st *InMemoryBTreeKeyValueStoreG[K, V]) Range(ctx context.Context,
	from optional.Optional[K], to optional.Optional[K],
	iterFunc func(K, V) error,
) error {
	st.mux.Lock()
	defer st.mux.Unlock()
	f, okF := from.Get()
	t, okT := to.Get()
	if !okF && !okT {
		st.store.Ascend(btree.ItemIteratorG[kvPairG[K, V]](func(kv kvPairG[K, V]) bool {
			err := iterFunc(kv.key, kv.val)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[Error] Range: %v\n", err)
				return false
			}
			return true
		}))
	} else if !okF && okT {
		st.store.AscendLessThan(kvPairG[K, V]{key: t},
			btree.ItemIteratorG[kvPairG[K, V]](func(kv kvPairG[K, V]) bool {
				err := iterFunc(kv.key, kv.val)
				if err != nil {
					fmt.Fprintf(os.Stderr, "[Error] Range: %v\n", err)
					return false
				}
				return true
			}))
	} else if okF && !okT {
		st.store.AscendGreaterOrEqual(kvPairG[K, V]{key: f},
			btree.ItemIteratorG[kvPairG[K, V]](func(kv kvPairG[K, V]) bool {
				err := iterFunc(kv.key, kv.val)
				if err != nil {
					fmt.Fprintf(os.Stderr, "[Error] Range: %v\n", err)
					return false
				}
				return true
			}))
	} else {
		st.store.AscendRange(kvPairG[K, V]{key: f}, kvPairG[K, V]{key: t},
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

func (st *InMemoryBTreeKeyValueStoreG[K, V]) Flush(ctx context.Context) error {
	return nil
}

func (st *InMemoryBTreeKeyValueStoreG[K, V]) ConsumeChangelog(ctx context.Context, parNum uint8) (*commtypes.MsgAndSeqs, error) {
	return nil, nil
}
