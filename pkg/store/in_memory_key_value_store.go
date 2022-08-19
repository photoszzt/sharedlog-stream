package store

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/data_structure/treemap"
	"sharedlog-stream/pkg/exactly_once_intr"
	"sharedlog-stream/pkg/utils"
	"strings"
	"sync"
)

type InMemoryKeyValueStore struct {
	mux   sync.RWMutex
	store *treemap.TreeMap
	name  string
}

var _ = CoreKeyValueStore(NewInMemoryKeyValueStore("a", nil))

type KVStoreLessFunc func(a treemap.Key, b treemap.Key) bool

func Uint64Less(a, b treemap.Key) bool {
	ka := a.(uint64)
	kb := b.(uint64)
	return ka < kb
}

func IntLess(a, b treemap.Key) bool {
	ka := a.(int)
	kb := b.(int)
	return ka < kb
}

func StringLess(a, b treemap.Key) bool {
	a1 := a.(string)
	b1 := b.(string)
	return strings.Compare(a1, b1) < 0
}

func NewInMemoryKeyValueStore(name string, compare KVStoreLessFunc) *InMemoryKeyValueStore {
	return &InMemoryKeyValueStore{
		name:  name,
		store: treemap.New(compare),
	}
}

func (st *InMemoryKeyValueStore) Name() string {
	return st.name
}

func (st *InMemoryKeyValueStore) Get(ctx context.Context, key commtypes.KeyT) (commtypes.ValueT, bool, error) {
	st.mux.RLock()
	defer st.mux.RUnlock()
	val, ok := st.store.Get(key)
	return val, ok, nil
}

func (st *InMemoryKeyValueStore) Put(ctx context.Context, key commtypes.KeyT, value commtypes.ValueT) error {
	st.mux.Lock()
	defer st.mux.Unlock()
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
	st.mux.Lock()
	defer st.mux.Unlock()
	originalVal, exists := st.store.Get(key)
	if !exists {
		st.store.Set(key, value)
	}
	return originalVal, nil
}

func (st *InMemoryKeyValueStore) PutAll(ctx context.Context, entries []*commtypes.Message) error {
	st.mux.Lock()
	defer st.mux.Unlock()
	for _, msg := range entries {
		st.store.Set(msg.Key, msg.Value)
	}
	return nil
}

func (st *InMemoryKeyValueStore) Delete(ctx context.Context, key commtypes.KeyT) error {
	st.mux.Lock()
	defer st.mux.Unlock()
	st.store.Del(key)
	return nil
}

func (st *InMemoryKeyValueStore) ApproximateNumEntries() (uint64, error) {
	return uint64(st.store.Len()), nil
}

func (st *InMemoryKeyValueStore) Range(ctx context.Context, from commtypes.KeyT, to commtypes.KeyT, iterFunc func(commtypes.KeyT, commtypes.ValueT) error) error {
	st.mux.Lock()
	defer st.mux.Unlock()
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
	st.mux.Lock()
	defer st.mux.Unlock()
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

func (st *InMemoryKeyValueStore) TableType() TABLE_TYPE {
	return IN_MEM
}

func (st *InMemoryKeyValueStore) SetTrackParFunc(exactly_once_intr.TrackProdSubStreamFunc) {
}

func (st *InMemoryKeyValueStore) FlushChangelog(context.Context) error {
	return nil
}

func (s *InMemoryKeyValueStore) ConsumeChangelog(ctx context.Context, parNum uint8) (*commtypes.MsgAndSeqs, error) {
	panic("not supported")
}

func (s *InMemoryKeyValueStore) ChangelogTopicName() string {
	panic("not supported")
}

func (s *InMemoryKeyValueStore) ChangelogIsSrc() bool {
	panic("not supported")
}
func (s *InMemoryKeyValueStore) ConfigureExactlyOnce(rem exactly_once_intr.ReadOnlyExactlyOnceManager, guarantee exactly_once_intr.GuaranteeMth, serdeFormat commtypes.SerdeFormat) error {
	panic("not supported")
}
