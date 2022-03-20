package store

import (
	"context"
	"math"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sync"
	"time"
)

type SegmentedWindowStore struct {
	seqNumMu         sync.Mutex
	bytesStore       SegmentedBytesStore
	valSerde         commtypes.Serde
	keySerde         commtypes.Serde
	windowKeySchema  *WindowKeySchema
	windowSize       int64
	seqNum           uint32
	retainDuplicates bool
}

var _ = WindowStore(&SegmentedWindowStore{})

func NewSegmentedWindowStore(
	bytesStore SegmentedBytesStore,
	retainDuplicates bool,
	windowSize int64,
	keySerde commtypes.Serde,
	valSerde commtypes.Serde,
) *SegmentedWindowStore {
	return &SegmentedWindowStore{
		bytesStore:       bytesStore,
		retainDuplicates: retainDuplicates,
		windowSize:       windowSize,
		seqNum:           0,
		windowKeySchema:  &WindowKeySchema{},
		keySerde:         keySerde,
		valSerde:         valSerde,
	}
}

func (rws *SegmentedWindowStore) updateSeqnumForDups() {
	if rws.retainDuplicates {
		rws.seqNumMu.Lock()
		defer rws.seqNumMu.Unlock()
		if rws.seqNum == math.MaxUint32 {
			rws.seqNum = 0
		}
		rws.seqNum += 1
	}
}

func (rws *SegmentedWindowStore) IsOpen() bool { return true }
func (rws *SegmentedWindowStore) Name() string { return rws.bytesStore.Name() }

func (rws *SegmentedWindowStore) Init(ctx StoreContext) {}

func (rws *SegmentedWindowStore) Put(ctx context.Context, key commtypes.KeyT, value commtypes.ValueT, windowStartTimestamp int64) error {
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
		if !ok {
			vBytes, err = rws.valSerde.Encode(value)
			if err != nil {
				return err
			}
		}
		rws.bytesStore.Put(ctx, k, vBytes)
	}
	return nil
}

func (rws *SegmentedWindowStore) Get(ctx context.Context, key commtypes.KeyT, windowStartTimestamp int64) (commtypes.ValueT, bool, error) {
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
	valBytes, ok, err := rws.bytesStore.Get(ctx, k)
	if err != nil {
		return nil, false, err
	}
	val, err := rws.valSerde.Decode(valBytes)
	if err != nil {
		return nil, false, err
	}
	return val, ok, err
}

func (rws *SegmentedWindowStore) Fetch(ctx context.Context, key commtypes.KeyT, timeFrom time.Time, timeTo time.Time,
	iterFunc func(int64 /* ts */, commtypes.KeyT, commtypes.ValueT) error,
) error {
	tsFrom := timeFrom.UnixMilli()
	tsTo := timeTo.UnixMilli()
	var err error
	kBytes, ok := key.([]byte)
	if !ok {
		kBytes, err = rws.keySerde.Encode(key)
		if err != nil {
			return err
		}
	}
	rws.bytesStore.Fetch(ctx, kBytes, tsFrom, tsTo, func(ts int64, kBytes, vBytes []byte) error {
		k, err := rws.keySerde.Decode(kBytes)
		if err != nil {
			return err
		}
		v, err := rws.valSerde.Decode(vBytes)
		if err != nil {
			return err
		}
		return iterFunc(ts, k, v)
	})
	return nil
}

func (rws *SegmentedWindowStore) BackwardFetch(key commtypes.KeyT, timeFrom time.Time, timeTo time.Time,
	iterFunc func(int64, commtypes.KeyT, commtypes.ValueT) error,
) error {
	panic("not implemented")
}

func (rws *SegmentedWindowStore) FetchWithKeyRange(ctx context.Context, keyFrom commtypes.KeyT, keyTo commtypes.KeyT, timeFrom time.Time, timeTo time.Time,
	iterFunc func(int64, commtypes.KeyT, commtypes.ValueT) error,
) error {
	tsFrom := timeFrom.UnixMilli()
	tsTo := timeTo.UnixMilli()
	var err error
	kFromBytes, ok := keyFrom.([]byte)
	if !ok {
		kFromBytes, err = rws.keySerde.Encode(keyFrom)
		if err != nil {
			return err
		}
	}
	kToBytes, ok := keyTo.([]byte)
	if !ok {
		kToBytes, err = rws.keySerde.Encode(keyTo)
		if err != nil {
			return err
		}
	}
	return rws.bytesStore.FetchWithKeyRange(ctx, kFromBytes, kToBytes, tsFrom, tsTo,
		func(ts int64, kBytes, vBytes []byte) error {
			k, err := rws.keySerde.Decode(kBytes)
			if err != nil {
				return err
			}
			v, err := rws.valSerde.Decode(vBytes)
			if err != nil {
				return err
			}
			return iterFunc(ts, k, v)
		})
}

func (rws *SegmentedWindowStore) BackwardFetchWithKeyRange(keyFrom commtypes.KeyT, keyTo commtypes.KeyT, timeFrom time.Time, timeTo time.Time, iterFunc func(int64, commtypes.KeyT, commtypes.ValueT) error) error {
	panic("not implemented")
}

func (rws *SegmentedWindowStore) FetchAll(ctx context.Context, timeFrom time.Time, timeTo time.Time,
	iterFunc func(int64, commtypes.KeyT, commtypes.ValueT) error,
) error {
	tsFrom := timeFrom.UnixMilli()
	tsTo := timeTo.UnixMilli()
	return rws.bytesStore.FetchAll(ctx, tsFrom, tsTo, func(ts int64, k, v []byte) error {
		key, err := rws.keySerde.Decode(k)
		if err != nil {
			return err
		}
		val, err := rws.valSerde.Decode(v)
		if err != nil {
			return err
		}
		return iterFunc(ts, key, val)
	})
}

func (rws *SegmentedWindowStore) BackwardFetchAll(timeFrom time.Time, timeTo time.Time,
	iterFunc func(int64, commtypes.KeyT, commtypes.ValueT) error,
) error {
	panic("not implemented")
}

func (rws *SegmentedWindowStore) IterAll(iterFunc func(int64, commtypes.KeyT, commtypes.ValueT) error) error {
	panic("not implemented")
}
