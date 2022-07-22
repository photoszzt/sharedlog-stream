package store_with_changelog

import (
	"context"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/concurrent_skiplist"
	"sharedlog-stream/pkg/exactly_once_intr"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/stats"
	"sharedlog-stream/pkg/store"
	"time"
)

type InMemoryWindowStoreWithChangelog struct {
	msgSerde         commtypes.MessageSerde
	originKeySerde   commtypes.Serde
	windowStore      *store.InMemoryWindowStore
	trackFunc        exactly_once_intr.TrackProdSubStreamFunc
	changelogManager *ChangelogManager
	// changeLogProduce stats.ConcurrentInt64Collector
	// storePutLatency stats.ConcurrentInt64Collector
	trackFuncLat stats.ConcurrentInt64Collector
	parNum       uint8
}

var _ = store.WindowStore(&InMemoryWindowStoreWithChangelog{})

func NewInMemoryWindowStoreWithChangelog(
	winDefs processor.EnumerableWindowDefinition,
	retainDuplicates bool,
	comparable concurrent_skiplist.Comparable,
	mp *MaterializeParam,
) (*InMemoryWindowStoreWithChangelog, error) {
	changelogManager, msgSerde, err := createChangelogManagerAndUpdateMsgSerde(mp)
	if err != nil {
		return nil, err
	}
	return &InMemoryWindowStoreWithChangelog{
		windowStore: store.NewInMemoryWindowStore(mp.storeName,
			winDefs.MaxSize()+winDefs.GracePeriodMs(), winDefs.MaxSize(), retainDuplicates, comparable),
		msgSerde:         msgSerde,
		originKeySerde:   mp.msgSerde.GetKeySerde(),
		parNum:           mp.ParNum(),
		trackFunc:        exactly_once_intr.DefaultTrackProdSubstreamFunc,
		changelogManager: changelogManager,
		// changeLogProduce: stats.NewConcurrentInt64Collector(mp.storeName+"-clProd", stats.DEFAULT_COLLECT_DURATION),
		// storePutLatency: stats.NewConcurrentInt64Collector(mp.storeName+"-storePutLatency", stats.DEFAULT_COLLECT_DURATION),
		trackFuncLat: stats.NewConcurrentInt64Collector(mp.storeName+"-trackFuncLat", stats.DEFAULT_COLLECT_DURATION),
	}, nil
}

func createChangelogManagerAndUpdateMsgSerde(mp *MaterializeParam) (*ChangelogManager, commtypes.MessageSerde, error) {
	changelog, err := CreateChangelog(mp.changelogParam.Env,
		mp.storeName, mp.changelogParam.NumPartition,
		mp.serdeFormat)
	if err != nil {
		return nil, nil, err
	}
	var keyAndWindowStartTsSerde commtypes.Serde
	if mp.serdeFormat == commtypes.JSON {
		keyAndWindowStartTsSerde = commtypes.KeyAndWindowStartTsJSONSerde{
			KeyJSONSerde: mp.msgSerde.GetKeySerde(),
		}
	} else if mp.serdeFormat == commtypes.MSGP {
		keyAndWindowStartTsSerde = commtypes.KeyAndWindowStartTsMsgpSerde{
			KeyMsgpSerde: mp.msgSerde.GetKeySerde(),
		}
	} else {
		return nil, nil, common_errors.ErrUnrecognizedSerdeFormat
	}
	msgSerde, err := commtypes.GetMsgSerde(mp.serdeFormat, keyAndWindowStartTsSerde, mp.msgSerde.GetValSerde())
	if err != nil {
		return nil, nil, err
	}
	changelogManager := NewChangelogManager(changelog, msgSerde, mp.changelogParam.TimeOut, mp.changelogParam.FlushDuration)
	return changelogManager, msgSerde, nil
}

func NewInMemoryWindowStoreWithChangelogForTest(
	retentionPeriod int64, windowSize int64,
	retainDuplicates bool,
	comparable concurrent_skiplist.Comparable,
	mp *MaterializeParam,
) (*InMemoryWindowStoreWithChangelog, error) {
	changelogManager, msgSerde, err := createChangelogManagerAndUpdateMsgSerde(mp)
	if err != nil {
		return nil, err
	}
	return &InMemoryWindowStoreWithChangelog{
		windowStore: store.NewInMemoryWindowStore(mp.storeName,
			retentionPeriod, windowSize, retainDuplicates, comparable),
		changelogManager: changelogManager,
		msgSerde:         msgSerde,
		parNum:           mp.ParNum(),
		trackFunc:        exactly_once_intr.DefaultTrackProdSubstreamFunc,
	}, nil
}

func (st *InMemoryWindowStoreWithChangelog) Name() string {
	return st.windowStore.Name()
}

func (st *InMemoryWindowStoreWithChangelog) ChangelogManager() *ChangelogManager {
	return st.changelogManager
}

func (st *InMemoryWindowStoreWithChangelog) FlushChangelog(ctx context.Context) error {
	return st.changelogManager.Flush(ctx)
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
	// pStart := stats.TimerBegin()
	err := st.changelogManager.Produce(ctx, msg, st.parNum, false)
	// elapsed := stats.Elapsed(pStart).Microseconds()
	// st.changeLogProduce.AddSample(elapsed)
	if err != nil {
		return err
	}
	tStart := stats.TimerBegin()
	err = st.trackFunc(ctx, key, st.originKeySerde, st.changelogManager.TopicName(), st.parNum)
	if err != nil {
		return err
	}
	tElapsed := stats.Elapsed(tStart).Microseconds()
	st.trackFuncLat.AddSample(tElapsed)

	// putStart := stats.TimerBegin()
	err = st.windowStore.Put(ctx, key, value, windowStartTimestamp)
	// elapsed := stats.Elapsed(putStart).Microseconds()
	// st.storePutLatency.AddSample(elapsed)
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
func (s *InMemoryWindowStoreWithChangelog) SetTrackParFunc(trackParFunc exactly_once_intr.TrackProdSubStreamFunc) {
	s.trackFunc = trackParFunc
}

func ToInMemWindowTableWithChangelog(
	mp *MaterializeParam,
	joinWindow *processor.JoinWindows,
	comparable concurrent_skiplist.Comparable,
) (*processor.MeteredProcessor, *InMemoryWindowStoreWithChangelog, error) {
	tabWithLog, err := NewInMemoryWindowStoreWithChangelog(
		joinWindow, true, comparable, mp)
	if err != nil {
		return nil, nil, err
	}
	toTableProc := processor.NewMeteredProcessor(processor.NewStoreToWindowTableProcessor(tabWithLog))
	return toTableProc, tabWithLog, nil
}
