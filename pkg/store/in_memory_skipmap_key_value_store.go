package store

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/exactly_once_intr"
	"sharedlog-stream/pkg/optional"
	"sharedlog-stream/pkg/utils"

	"github.com/zhangyunhao116/skipmap"
)

type InMemorySkipmapKeyValueStoreG[K, V any] struct {
	store *skipmap.FuncMap[K, V]
	name  string
}

var _ = CoreKeyValueStoreG[int, int](&InMemorySkipmapKeyValueStoreG[int, int]{})

func NewInMemorySkipmapKeyValueStoreG[K, V any](name string, lessFunc LessFunc[K]) *InMemorySkipmapKeyValueStoreG[K, V] {
	return &InMemorySkipmapKeyValueStoreG[K, V]{
		name:  name,
		store: skipmap.NewFunc[K, V](lessFunc),
	}
}

var _ = CoreKeyValueStoreG[int, int](&InMemorySkipmapKeyValueStoreG[int, int]{})

func (st *InMemorySkipmapKeyValueStoreG[K, V]) Name() string {
	return st.name
}

func (st *InMemorySkipmapKeyValueStoreG[K, V]) Get(ctx context.Context, key K) (V, bool, error) {
	ret, exists := st.store.Load(key)
	return ret, exists, nil
}

func (st *InMemorySkipmapKeyValueStoreG[K, V]) Put(ctx context.Context, key K, value optional.Option[V]) error {
	v, ok := value.Take()
	if !ok {
		st.store.Delete(key)
	} else {
		st.store.Store(key, v)
	}
	return nil
}

func (st *InMemorySkipmapKeyValueStoreG[K, V]) PutIfAbsent(ctx context.Context, key K, value V) (optional.Option[V], error) {
	val, loaded := st.store.LoadOrStore(key, value)
	if loaded {
		return optional.Some(val), nil
	} else {
		return optional.None[V](), nil
	}
}

func (st *InMemorySkipmapKeyValueStoreG[K, V]) PutWithoutPushToChangelog(ctx context.Context, key commtypes.KeyT, value commtypes.ValueT) error {
	if utils.IsNil(value) {
		return st.Put(ctx, key.(K), optional.None[V]())
	} else {
		return st.Put(ctx, key.(K), optional.Some(value.(V)))
	}
}

func (st *InMemorySkipmapKeyValueStoreG[K, V]) PutAll(ctx context.Context, kvs []*commtypes.Message) error {
	for _, kv := range kvs {
		var err error
		if utils.IsNil(kv.Value) {
			err = st.Put(ctx, kv.Key.(K), optional.None[V]())
		} else {
			err = st.Put(ctx, kv.Key.(K), optional.Some(kv.Value.(V)))
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (st *InMemorySkipmapKeyValueStoreG[K, V]) Delete(ctx context.Context, key K) error {
	st.store.Delete(key)
	return nil
}

func (st *InMemorySkipmapKeyValueStoreG[K, V]) ApproximateNumEntries() (uint64, error) {
	return uint64(st.store.Len()), nil
}

func (st *InMemorySkipmapKeyValueStoreG[K, V]) Range(ctx context.Context,
	from optional.Option[K], to optional.Option[K],
	iterFunc func(K, V) error,
) error {
	f, okF := from.Take()
	t, okT := to.Take()
	_ = f
	_ = t
	if !okF && !okT {
		st.store.Range(func(key K, value V) bool {
			err := iterFunc(key, value)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[Error] Range: %v\n", err)
				return false
			}
			return true
		})
	} else if !okF && okT {
		panic("not implemented")
	} else if okF && !okT {
		panic("not implemented")
	} else {
		panic("not implemented")
	}
	return nil
}

func (st *InMemorySkipmapKeyValueStoreG[K, V]) TableType() TABLE_TYPE {
	return IN_MEM
}

func (st *InMemorySkipmapKeyValueStoreG[K, V]) SetTrackParFunc(exactly_once_intr.TrackProdSubStreamFunc) {
}

func (st *InMemorySkipmapKeyValueStoreG[K, V]) Flush(ctx context.Context) error {
	return nil
}

func (st *InMemorySkipmapKeyValueStoreG[K, V]) ConsumeChangelog(ctx context.Context, parNum uint8) (*commtypes.MsgAndSeqs, error) {
	return nil, nil
}
