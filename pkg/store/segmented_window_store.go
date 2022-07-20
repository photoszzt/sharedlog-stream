package store

import (
	"context"
	"math"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/exactly_once_intr"
	"sharedlog-stream/pkg/utils/syncutils"
	"time"

	"4d63.com/optional"
)

type SegmentedWindowStore[K, V any] struct {
	seqNumMu         syncutils.Mutex
	bytesStore       SegmentedBytesStore
	valSerde         commtypes.Serde[V]
	keySerde         commtypes.Serde[K]
	windowKeySchema  *WindowKeySchema
	windowSize       int64
	seqNum           uint32
	retainDuplicates bool
}

var _ = WindowStore[int, string](&SegmentedWindowStore[int, string]{})

func NewSegmentedWindowStore[K, V any](
	bytesStore SegmentedBytesStore,
	retainDuplicates bool,
	windowSize int64,
	keySerde commtypes.Serde[K],
	valSerde commtypes.Serde[V],
) *SegmentedWindowStore[K, V] {
	return &SegmentedWindowStore[K, V]{
		bytesStore:       bytesStore,
		retainDuplicates: retainDuplicates,
		windowSize:       windowSize,
		seqNum:           0,
		windowKeySchema:  &WindowKeySchema{},
		keySerde:         keySerde,
		valSerde:         valSerde,
	}
}

func (rws *SegmentedWindowStore[K, V]) updateSeqnumForDups() {
	if rws.retainDuplicates {
		rws.seqNumMu.Lock()
		defer rws.seqNumMu.Unlock()
		if rws.seqNum == math.MaxUint32 {
			rws.seqNum = 0
		}
		rws.seqNum += 1
	}
}

func (rws *SegmentedWindowStore[K, V]) IsOpen() bool { return true }
func (rws *SegmentedWindowStore[K, V]) Name() string { return rws.bytesStore.Name() }

func (rws *SegmentedWindowStore[K, V]) Put(ctx context.Context, key K, value optional.Optional[V], windowStartTimestamp int64) error {
	if value.IsPresent() || !rws.retainDuplicates {
		rws.updateSeqnumForDups()
		kBytes, err := rws.keySerde.Encode(key)
		if err != nil {
			return err
		}
		rws.seqNumMu.Lock()
		defer rws.seqNumMu.Unlock()
		k, err := rws.windowKeySchema.ToStoreKeyBinary(kBytes, windowStartTimestamp, rws.seqNum)
		if err != nil {
			return err
		}
		var vBytes []byte
		if val, ok := value.Get(); ok {
			vBytes, err = rws.valSerde.Encode(val)
			if err != nil {
				return err
			}
		}
		return rws.bytesStore.Put(ctx, k, vBytes)
	}
	return nil
}

func (rws *SegmentedWindowStore[K, V]) PutWithoutPushToChangelog(ctx context.Context, key K, value optional.Optional[V], windowStartTimestamp int64) error {
	return rws.Put(ctx, key, value, windowStartTimestamp)
}

func (rws *SegmentedWindowStore[K, V]) Get(ctx context.Context, key K, windowStartTimestamp int64) (V, bool, error) {
	var err error
	var v V
	kBytes, err := rws.keySerde.Encode(key)
	if err != nil {
		return v, false, err
	}
	rws.seqNumMu.Lock()
	defer rws.seqNumMu.Unlock()
	k, err := rws.windowKeySchema.ToStoreKeyBinary(kBytes, windowStartTimestamp, rws.seqNum)
	if err != nil {
		return v, false, err
	}
	valBytes, ok, err := rws.bytesStore.Get(ctx, k)
	if err != nil {
		return v, false, err
	}
	if ok {
		val, err := rws.valSerde.Decode(valBytes)
		if err != nil {
			return v, false, err
		}
		return val, ok, err
	}
	return v, false, nil
}

func (rws *SegmentedWindowStore[K, V]) Fetch(ctx context.Context, key K, timeFrom time.Time, timeTo time.Time,
	iterFunc func(int64 /* ts */, K, V) error,
) error {
	tsFrom := timeFrom.UnixMilli()
	tsTo := timeTo.UnixMilli()
	kBytes, err := rws.keySerde.Encode(key)
	if err != nil {
		return err
	}
	return rws.bytesStore.Fetch(ctx, kBytes, tsFrom, tsTo, func(ts int64, kBytes, vBytes []byte) error {
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

func (rws *SegmentedWindowStore[K, V]) BackwardFetch(key K, timeFrom time.Time, timeTo time.Time,
	iterFunc func(int64, K, V) error,
) error {
	panic("not implemented")
}

func (rws *SegmentedWindowStore[K, V]) FetchWithKeyRange(ctx context.Context, keyFrom K, keyTo K, timeFrom time.Time, timeTo time.Time,
	iterFunc func(int64, K, V) error,
) error {
	tsFrom := timeFrom.UnixMilli()
	tsTo := timeTo.UnixMilli()
	kFromBytes, err := rws.keySerde.Encode(keyFrom)
	if err != nil {
		return err
	}
	kToBytes, err := rws.keySerde.Encode(keyTo)
	if err != nil {
		return err
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

func (rws *SegmentedWindowStore[K, V]) BackwardFetchWithKeyRange(keyFrom K, keyTo K, timeFrom time.Time, timeTo time.Time, iterFunc func(int64, K, V) error) error {
	panic("not implemented")
}

func (rws *SegmentedWindowStore[K, V]) FetchAll(ctx context.Context, timeFrom time.Time, timeTo time.Time,
	iterFunc func(int64, K, V) error,
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

func (rws *SegmentedWindowStore[K, V]) BackwardFetchAll(timeFrom time.Time, timeTo time.Time,
	iterFunc func(int64, K, V) error,
) error {
	panic("not implemented")
}

func (rws *SegmentedWindowStore[K, V]) IterAll(iterFunc func(int64, K, V) error) error {
	panic("not implemented")
}

func (rws *SegmentedWindowStore[K, V]) DropDatabase(ctx context.Context) error {
	return rws.bytesStore.DropDatabase(ctx)
}

func (rws *SegmentedWindowStore[K, V]) TableType() TABLE_TYPE {
	return rws.bytesStore.TableType()
}

func (rws *SegmentedWindowStore[K, V]) StartTransaction(ctx context.Context) error {
	return rws.bytesStore.StartTransaction(ctx)
}
func (rws *SegmentedWindowStore[K, V]) CommitTransaction(ctx context.Context, taskRepr string,
	transactionID uint64,
) error {
	return rws.bytesStore.CommitTransaction(ctx, taskRepr, transactionID)
}
func (rws *SegmentedWindowStore[K, V]) AbortTransaction(ctx context.Context) error {
	return rws.bytesStore.AbortTransaction(ctx)
}

func (rws *SegmentedWindowStore[K, V]) GetTransactionID(ctx context.Context, taskRepr string) (uint64, bool, error) {
	return rws.bytesStore.GetTransactionID(ctx, taskRepr)
}

func (rws *SegmentedWindowStore[K, V]) SetTrackParFunc(trackParFunc exactly_once_intr.TrackProdSubStreamFunc[K]) {
}
