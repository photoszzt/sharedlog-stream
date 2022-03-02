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

func (rws *SegmentedWindowStore) Put(ctx context.Context, key KeyT, value ValueT, windowStartTimestamp int64) error {
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

func (rws *SegmentedWindowStore) Get(ctx context.Context, key KeyT, windowStartTimestamp int64) (ValueT, bool, error) {
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

func (rws *SegmentedWindowStore) Fetch(key KeyT, timeFrom time.Time, timeTo time.Time,
	iterFunc func(int64 /* ts */, KeyT, ValueT) error,
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
	rws.bytesStore.Fetch(kBytes, tsFrom, tsTo, iterFunc)
	return nil
}

func (rws *SegmentedWindowStore) BackwardFetch(key KeyT, timeFrom time.Time, timeTo time.Time,
	iterFunc func(int64, KeyT, ValueT) error,
) error {
	panic("not implemented")
}

func (rws *SegmentedWindowStore) FetchWithKeyRange(keyFrom KeyT, keyTo KeyT, timeFrom time.Time, timeTo time.Time,
	iterFunc func(int64, KeyT, ValueT) error,
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
	return rws.bytesStore.FetchWithKeyRange(kFromBytes, kToBytes, tsFrom, tsTo, iterFunc)
}

func (rws *SegmentedWindowStore) BackwardFetchWithKeyRange(keyFrom KeyT, keyTo KeyT, timeFrom time.Time, timeTo time.Time, iterFunc func(int64, KeyT, ValueT) error) error {
	panic("not implemented")
}

func (rws *SegmentedWindowStore) FetchAll(timeFrom time.Time, timeTo time.Time,
	iterFunc func(int64, KeyT, ValueT) error,
) error {
	panic("not implemented")
}

func (rws *SegmentedWindowStore) BackwardFetchAll(timeFrom time.Time, timeTo time.Time,
	iterFunc func(int64, KeyT, ValueT) error,
) error {
	panic("not implemented")
}

func (rws *SegmentedWindowStore) IterAll(iterFunc func(int64, KeyT, ValueT) error) error {
	panic("not implemented")
}
