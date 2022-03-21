package store

import (
	"context"
	"fmt"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"time"
)

type InMemoryWindowStoreWithChangelog struct {
	windowStore      *InMemoryWindowStore
	mp               *MaterializeParam
	keyWindowTsSerde commtypes.Serde
}

var _ = WindowStore(&InMemoryWindowStoreWithChangelog{})

func NewInMemoryWindowStoreWithChangelog(retensionPeriod int64, windowSize int64, retainDuplicates bool, mp *MaterializeParam) (*InMemoryWindowStoreWithChangelog, error) {
	var ktsSerde commtypes.Serde
	if mp.SerdeFormat == commtypes.JSON {
		ktsSerde = commtypes.KeyAndWindowStartTsJSONSerde{}
	} else if mp.SerdeFormat == commtypes.MSGP {
		ktsSerde = commtypes.KeyAndWindowStartTsMsgpSerde{}
	} else {
		return nil, fmt.Errorf("serde format should be either json or msgp; but %v is given", mp.SerdeFormat)
	}
	return &InMemoryWindowStoreWithChangelog{
		windowStore: NewInMemoryWindowStore(mp.StoreName,
			retensionPeriod, windowSize, retainDuplicates, mp.Comparable),
		mp:               mp,
		keyWindowTsSerde: ktsSerde,
	}, nil
}

func (st *InMemoryWindowStoreWithChangelog) Init(ctx StoreContext) {
	st.windowStore.Init(ctx)
	ctx.RegisterWindowStore(st)
}

func (st *InMemoryWindowStoreWithChangelog) IsOpen() bool {
	return true
}

func (st *InMemoryWindowStoreWithChangelog) MaterializeParam() *MaterializeParam {
	return st.mp
}

func (st *InMemoryWindowStoreWithChangelog) KeyWindowTsSerde() commtypes.Serde {
	return st.keyWindowTsSerde
}

func (st *InMemoryWindowStoreWithChangelog) Name() string {
	return st.windowStore.name
}

func (st *InMemoryWindowStoreWithChangelog) Put(ctx context.Context, key commtypes.KeyT, value commtypes.ValueT, windowStartTimestamp int64) error {
	keyBytes, err := st.mp.KeySerde.Encode(key)
	if err != nil {
		return err
	}
	valBytes, err := st.mp.ValueSerde.Encode(value)
	if err != nil {
		return err
	}

	keyAndTs := &commtypes.KeyAndWindowStartTs{
		Key:           keyBytes,
		WindowStartTs: windowStartTimestamp,
	}
	ktsBytes, err := st.keyWindowTsSerde.Encode(keyAndTs)
	if err != nil {
		return err
	}
	encoded, err := st.mp.MsgSerde.Encode(ktsBytes, valBytes)
	if err != nil {
		return err
	}
	_, err = st.mp.Changelog.Push(ctx, encoded, st.mp.ParNum, false)
	if err != nil {
		return err
	}
	err = st.windowStore.Put(ctx, key, value, windowStartTimestamp)
	return err
}

func (st *InMemoryWindowStoreWithChangelog) Get(ctx context.Context, key commtypes.KeyT, windowStartTimestamp int64) (commtypes.ValueT, bool, error) {
	val, ok, err := st.windowStore.Get(ctx, key, windowStartTimestamp)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, err
	}
	return val, ok, nil
}

func (st *InMemoryWindowStoreWithChangelog) Fetch(
	ctx context.Context,
	key commtypes.KeyT,
	timeFrom time.Time,
	timeTo time.Time,
	iterFunc func(int64, commtypes.KeyT, commtypes.ValueT) error,
) error {
	return st.windowStore.Fetch(ctx, key, timeFrom, timeTo, iterFunc)
}

func (st *InMemoryWindowStoreWithChangelog) IterAll(iterFunc func(int64, commtypes.KeyT, commtypes.ValueT) error) error {
	return st.windowStore.IterAll(iterFunc)
}

func (st *InMemoryWindowStoreWithChangelog) BackwardFetch(
	key commtypes.KeyT,
	timeFrom time.Time,
	timeTo time.Time,
	iterFunc func(int64, commtypes.KeyT, commtypes.ValueT) error,
) error {
	return st.windowStore.BackwardFetch(key, timeFrom, timeTo, iterFunc)
}

func (st *InMemoryWindowStoreWithChangelog) FetchWithKeyRange(
	ctx context.Context,
	keyFrom commtypes.KeyT,
	keyTo commtypes.KeyT,
	timeFrom time.Time,
	timeTo time.Time,
	iterFunc func(int64, commtypes.KeyT, commtypes.ValueT) error,
) error {
	return st.windowStore.FetchWithKeyRange(ctx, keyFrom, keyTo, timeFrom, timeTo, iterFunc)
}

func (st *InMemoryWindowStoreWithChangelog) BackwardFetchWithKeyRange(
	keyFrom commtypes.KeyT,
	keyTo commtypes.KeyT,
	timeFrom time.Time,
	timeTo time.Time,
	iterFunc func(int64, commtypes.KeyT, commtypes.ValueT) error,
) error {
	return st.windowStore.BackwardFetchWithKeyRange(keyFrom, keyTo, timeFrom, timeTo, iterFunc)
}

func (st *InMemoryWindowStoreWithChangelog) FetchAll(
	ctx context.Context,
	timeFrom time.Time,
	timeTo time.Time,
	iterFunc func(int64, commtypes.KeyT, commtypes.ValueT) error,
) error {
	return st.windowStore.FetchAll(ctx, timeFrom, timeTo, iterFunc)
}

func (st *InMemoryWindowStoreWithChangelog) BackwardFetchAll(
	timeFrom time.Time,
	timeTo time.Time,
	iterFunc func(int64, commtypes.KeyT, commtypes.ValueT) error,
) error {
	return st.windowStore.BackwardFetchAll(timeFrom, timeTo, iterFunc)
}

func (s *InMemoryWindowStoreWithChangelog) DropDatabase(ctx context.Context) {
	panic("not implemented")
}
