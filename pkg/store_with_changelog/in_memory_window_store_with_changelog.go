package store_with_changelog

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/concurrent_skiplist"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/transaction/tran_interface"
	"time"
)

type InMemoryWindowStoreWithChangelog struct {
	windowStore *store.InMemoryWindowStore
	mp          *MaterializeParam
}

var _ = store.WindowStore(&InMemoryWindowStoreWithChangelog{})

func NewInMemoryWindowStoreWithChangelog(retensionPeriod int64,
	windowSize int64,
	retainDuplicates bool,
	comparable concurrent_skiplist.Comparable,
	mp *MaterializeParam,
) (*InMemoryWindowStoreWithChangelog, error) {
	return &InMemoryWindowStoreWithChangelog{
		windowStore: store.NewInMemoryWindowStore(mp.storeName,
			retensionPeriod, windowSize, retainDuplicates, comparable),
		mp: mp,
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

func (st *InMemoryWindowStoreWithChangelog) Name() string {
	return st.windowStore.Name()
}

func (st *InMemoryWindowStoreWithChangelog) FlushChangelog(ctx context.Context) error {
	return st.mp.ChangelogManager().Flush(ctx)
}

func (st *InMemoryWindowStoreWithChangelog) Put(ctx context.Context,
	key commtypes.KeyT, value commtypes.ValueT, windowStartTimestamp int64,
) error {
	keyTs := &commtypes.KeyAndWindowStartTs{
		Key:           key,
		WindowStartTs: windowStartTimestamp,
	}
	msg := commtypes.Message{
		Key:   keyTs,
		Value: value,
	}
	changelogManager := st.mp.ChangelogManager()
	err := changelogManager.Produce(ctx, msg, st.mp.parNum, false)
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

func (st *InMemoryWindowStoreWithChangelog) PutWithoutPushToChangelog(ctx context.Context,
	key commtypes.KeyT, value commtypes.ValueT, windowStartTimestamp int64,
) error {
	return st.windowStore.Put(ctx, key, value, windowStartTimestamp)
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
func (s *InMemoryWindowStoreWithChangelog) SetTrackParFunc(trackParFunc tran_interface.TrackProdSubStreamFunc) {
	s.mp.SetTrackParFunc(trackParFunc)
}

func ToInMemWindowTableWithChangelog(
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
