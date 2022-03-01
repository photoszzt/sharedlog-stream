package store

import (
	"context"
	"math"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sync"
	"time"
)

type RedisWindowStore struct {
	bytesStore       SegmentedBytesStore
	retainDuplicates bool
	windowSize       int64

	seqNumMu sync.Mutex
	seqNum   uint32

	windowKeySchema *WindowKeySchema
	keySerde        commtypes.Serde
	valSerde        commtypes.Serde
}

var _ = WindowStore(&RedisWindowStore{})

func NewRedisWindowStore(
	bytesStore SegmentedBytesStore,
	retainDuplicates bool,
	windowSize int64,
	keySerde commtypes.Serde,
	valSerde commtypes.Serde,
) *RedisWindowStore {
	return &RedisWindowStore{
		bytesStore:       bytesStore,
		retainDuplicates: retainDuplicates,
		windowSize:       windowSize,
		seqNum:           0,
		windowKeySchema:  &WindowKeySchema{},
	}
}

func (rws *RedisWindowStore) updateSeqnumForDups() {
	if rws.retainDuplicates {
		rws.seqNumMu.Lock()
		defer rws.seqNumMu.Unlock()
		if rws.seqNum == math.MaxUint32 {
			rws.seqNum = 0
		}
		rws.seqNum += 1
	}
}

func (rws *RedisWindowStore) IsOpen() bool { return true }
func (rws *RedisWindowStore) Name() string { return rws.bytesStore.Name() }

func (rws *RedisWindowStore) Init(ctx StoreContext) {}

func (rws *RedisWindowStore) Put(ctx context.Context, key KeyT, value ValueT, windowStartTimestamp int64) error {
	if !(value == nil && rws.retainDuplicates) {
		rws.updateSeqnumForDups()
		var err error
		kBytes, ok := key.([]byte)
		if !ok {
			kBytes, err = rws.keySerde.Encode(key)
			if err != nil {
				return err
			}
		}
		rws.seqNumMu.Lock()
		defer rws.seqNumMu.Unlock()
		k := rws.windowKeySchema.ToStoreKeyBinary(kBytes, windowStartTimestamp, rws.seqNum)
		vBytes, ok := value.([]byte)
		rws.bytesStore.Put(ctx, k, vBytes)
	}
	return nil
}

func (rws *RedisWindowStore) Get(ctx context.Context, key KeyT, windowStartTimestamp int64) (ValueT, bool, error) {
	var err error
	kBytes, ok := key.([]byte)
	if !ok {
		kBytes, err = rws.keySerde.Encode(key)
		if err != nil {
			return nil, false, err
		}
	}
	rws.seqNumMu.Lock()
	defer rws.seqNumMu.Unlock()
	k := rws.windowKeySchema.ToStoreKeyBinary(kBytes, windowStartTimestamp, rws.seqNum)
	return rws.bytesStore.Get(ctx, k)
}

func (rws *RedisWindowStore) Fetch(key KeyT, timeFrom time.Time, timeTo time.Time,
	iterFunc func(int64 /* ts */, KeyT, ValueT) error,
) error {
	return nil
}

func (rws *RedisWindowStore) BackwardFetch(key KeyT, timeFrom time.Time, timeTo time.Time,
	iterFunc func(int64, KeyT, ValueT) error,
) error {
	panic("not implemented")
}

func (rws *RedisWindowStore) FetchWithKeyRange(keyFrom KeyT, keyTo KeyT, timeFrom time.Time, timeTo time.Time,
	iterFunc func(int64, KeyT, ValueT) error,
) error {
	return nil
}

func (rws *RedisWindowStore) BackwardFetchWithKeyRange(keyFrom KeyT, keyTo KeyT, timeFrom time.Time, timeTo time.Time, iterFunc func(int64, KeyT, ValueT) error) error {
	panic("not implemented")
}

func (rws *RedisWindowStore) FetchAll(timeFrom time.Time, timeTo time.Time,
	iterFunc func(int64, KeyT, ValueT) error,
) error {
	return nil
}

func (rws *RedisWindowStore) BackwardFetchAll(timeFrom time.Time, timeTo time.Time,
	iterFunc func(int64, KeyT, ValueT) error,
) error {
	return nil
}

func (rws *RedisWindowStore) IterAll(iterFunc func(int64, KeyT, ValueT) error) error {
	return nil
}
