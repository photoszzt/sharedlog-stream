package store_with_changelog

import (
	"context"
	"fmt"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/concurrent_skiplist"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/transaction/tran_interface"
	"time"
)

type InMemoryWindowStoreWithChangelog struct {
	windowStore      *store.InMemoryWindowStore
	mp               *MaterializeParam
	keyWindowTsSerde commtypes.Serde
}

var _ = store.WindowStore(&InMemoryWindowStoreWithChangelog{})

func NewInMemoryWindowStoreWithChangelog(retensionPeriod int64,
	windowSize int64,
	retainDuplicates bool,
	comparable concurrent_skiplist.Comparable,
	mp *MaterializeParam,
) (*InMemoryWindowStoreWithChangelog, error) {
	var ktsSerde commtypes.Serde
	if mp.serdeFormat == commtypes.JSON {
		ktsSerde = commtypes.KeyAndWindowStartTsJSONSerde{}
	} else if mp.serdeFormat == commtypes.MSGP {
		ktsSerde = commtypes.KeyAndWindowStartTsMsgpSerde{}
	} else {
		return nil, fmt.Errorf("serde format should be either json or msgp; but %v is given", mp.SerdeFormat())
	}
	return &InMemoryWindowStoreWithChangelog{
		windowStore: store.NewInMemoryWindowStore(mp.storeName,
			retensionPeriod, windowSize, retainDuplicates, comparable),
		mp:               mp,
		keyWindowTsSerde: ktsSerde,
	}, nil
}

func (st *InMemoryWindowStoreWithChangelog) Init(ctx store.StoreContext) {
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
	return st.windowStore.Name()
}

func (st *InMemoryWindowStoreWithChangelog) FlushChangelog(ctx context.Context) error {
	return st.mp.ChangelogManager().Flush(ctx)
}

func (st *InMemoryWindowStoreWithChangelog) Put(ctx context.Context, key commtypes.KeyT, value commtypes.ValueT, windowStartTimestamp int64) error {
	keyBytes, err := st.mp.kvMsgSerdes.KeySerde.Encode(key)
	if err != nil {
		return err
	}
	valBytes, err := st.mp.kvMsgSerdes.ValSerde.Encode(value)
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
	encoded, err := st.mp.kvMsgSerdes.MsgSerde.Encode(ktsBytes, valBytes)
	if err != nil {
		return err
	}
	changelogManager := st.mp.ChangelogManager()
	err = changelogManager.Push(ctx, encoded, st.mp.parNum)
	if err != nil {
		return err
	}
	err = st.mp.trackFunc(ctx, key, st.mp.kvMsgSerdes.KeySerde, changelogManager.TopicName(), st.mp.parNum)
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

func (s *InMemoryWindowStoreWithChangelog) DropDatabase(ctx context.Context) error {
	panic("not implemented")
}

func (s *InMemoryWindowStoreWithChangelog) TableType() store.TABLE_TYPE {
	return store.IN_MEM
}

func (s *InMemoryWindowStoreWithChangelog) StartTransaction(ctx context.Context) error {
	panic("not supported")
}
func (s *InMemoryWindowStoreWithChangelog) CommitTransaction(ctx context.Context, taskRepr string, transactionID uint64) error {
	panic("not supported")
}
func (s *InMemoryWindowStoreWithChangelog) AbortTransaction(ctx context.Context) error {
	panic("not supported")
}
func (s *InMemoryWindowStoreWithChangelog) GetTransactionID(ctx context.Context, taskRepr string) (uint64, bool, error) {
	panic("not supported")
}
func (s *InMemoryWindowStoreWithChangelog) SetTrackParFunc(trackParFunc tran_interface.TrackKeySubStreamFunc) {
	s.mp.SetTrackParFunc(trackParFunc)
}

func ToInMemWindowTableWithChangelog(
	storeName string,
	mp *MaterializeParam,
	joinWindow *processor.JoinWindows,
	comparable concurrent_skiplist.Comparable,
	warmup time.Duration,
) (*processor.MeteredProcessor, store.WindowStore, error) {
	tabWithLog, err := NewInMemoryWindowStoreWithChangelog(
		joinWindow.MaxSize()+joinWindow.GracePeriodMs(),
		joinWindow.MaxSize(), true, comparable, mp)
	if err != nil {
		return nil, nil, err
	}
	toTableProc := processor.NewMeteredProcessor(processor.NewStoreToWindowTableProcessor(tabWithLog), warmup)
	return toTableProc, tabWithLog, nil
}
