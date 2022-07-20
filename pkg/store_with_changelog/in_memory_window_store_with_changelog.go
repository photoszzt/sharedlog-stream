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

type InMemoryWindowStoreWithChangelog[K, V any] struct {
	msgSerde         commtypes.MessageSerde[commtypes.KeyAndWindowStartTs[K], V]
	originKeySerde   commtypes.Serde[K]
	windowStore      *store.InMemoryWindowStore
	trackFunc        exactly_once_intr.TrackProdSubStreamFunc[K]
	changelogManager *ChangelogManager
	changeLogProduce stats.ConcurrentInt64Collector
	parNum           uint8
}

var _ = store.WindowStore(&InMemoryWindowStoreWithChangelog[int, int]{})

func NewInMemoryWindowStoreWithChangelog[K, V any](
	winDefs processor.EnumerableWindowDefinition,
	retainDuplicates bool,
	comparable concurrent_skiplist.Comparable,
	mp *MaterializeParam,
) (*InMemoryWindowStoreWithChangelog[K, V], error) {
	changelogManager, msgSerde, err := createChangelogManagerAndUpdateMsgSerde[K, V](mp)
	if err != nil {
		return nil, err
	}
	return &InMemoryWindowStoreWithChangelog[K, V]{
		windowStore: store.NewInMemoryWindowStore(mp.storeName,
			winDefs.MaxSize()+winDefs.GracePeriodMs(), winDefs.MaxSize(), retainDuplicates, comparable),
		msgSerde:         msgSerde,
		originKeySerde:   mp.msgSerde.GetKeySerde(),
		parNum:           mp.ParNum(),
		trackFunc:        exactly_once_intr.DefaultTrackProdSubstreamFunc[K],
		changelogManager: changelogManager,
		changeLogProduce: stats.NewConcurrentInt64Collector(mp.storeName+"-clProd", stats.DEFAULT_COLLECT_DURATION),
	}, nil
}

func createChangelogManagerAndUpdateMsgSerde[K, V any](mp *MaterializeParam) (*ChangelogManager, commtypes.MessageSerde[commtypes.KeyAndWindowStartTs[K], V], error) {
	changelog, err := CreateChangelog(mp.changelogParam.Env,
		mp.storeName, mp.changelogParam.NumPartition,
		mp.serdeFormat)
	if err != nil {
		return nil, nil, err
	}
	var keyAndWindowStartTsSerde commtypes.Serde[commtypes.KeyAndWindowStartTs[K]]
	if mp.serdeFormat == commtypes.JSON {
		keyAndWindowStartTsSerde = commtypes.KeyAndWindowStartTsJSONSerde[K]{
			KeyJSONSerde: mp.msgSerde.GetKeySerde(),
		}
	} else if mp.serdeFormat == commtypes.MSGP {
		keyAndWindowStartTsSerde = commtypes.KeyAndWindowStartTsMsgpSerde[K]{
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

func NewInMemoryWindowStoreWithChangelogForTest[K, V any](
	retentionPeriod int64, windowSize int64,
	retainDuplicates bool,
	comparable concurrent_skiplist.Comparable,
	mp *MaterializeParam,
) (*InMemoryWindowStoreWithChangelog[K, V], error) {
	changelogManager, msgSerde, err := createChangelogManagerAndUpdateMsgSerde[K, V](mp)
	if err != nil {
		return nil, err
	}
	return &InMemoryWindowStoreWithChangelog[K, V]{
		windowStore: store.NewInMemoryWindowStore(mp.storeName,
			retentionPeriod, windowSize, retainDuplicates, comparable),
		changelogManager: changelogManager,
		msgSerde:         msgSerde,
		parNum:           mp.ParNum(),
		trackFunc:        exactly_once_intr.DefaultTrackProdSubstreamFunc[K],
	}, nil
}

func (st *InMemoryWindowStoreWithChangelog[K, V]) Name() string {
	return st.windowStore.Name()
}

func (st *InMemoryWindowStoreWithChangelog[K, V]) ChangelogManager() *ChangelogManager {
	return st.changelogManager
}

func (st *InMemoryWindowStoreWithChangelog[K, V]) FlushChangelog(ctx context.Context) error {
	return st.changelogManager.Flush(ctx)
}

func (st *InMemoryWindowStoreWithChangelog[K, V]) Put(ctx context.Context,
	key K, value V, windowStartTimestamp int64,
) error {
	keyTs := &commtypes.KeyAndWindowStartTs[K]{
		Key:           key,
		WindowStartTs: windowStartTimestamp,
	}
	msg := commtypes.Message{
		Key:   keyTs,
		Value: value,
	}
	pStart := stats.TimerBegin()
	err := st.changelogManager.Produce(ctx, msg, st.parNum, false)
	elapsed := stats.Elapsed(pStart).Microseconds()
	st.changeLogProduce.AddSample(elapsed)
	if err != nil {
		return err
	}
	err = st.trackFunc(ctx, key, st.originKeySerde, st.changelogManager.TopicName(), st.parNum)
	if err != nil {
		return err
	}
	err = st.windowStore.Put(ctx, key, value, windowStartTimestamp)
	return err
}

func (st *InMemoryWindowStoreWithChangelog[K, V]) PutWithoutPushToChangelog(ctx context.Context,
	key K, value V, windowStartTimestamp int64,
) error {
	return st.windowStore.Put(ctx, key, value, windowStartTimestamp)
}

func (st *InMemoryWindowStoreWithChangelog[K, V]) Get(ctx context.Context, key K, windowStartTimestamp int64) (V, bool, error) {
	val, ok, err := st.windowStore.Get(ctx, key, windowStartTimestamp)
	if err != nil {
		return val.(V), false, err
	}
	if !ok {
		return val.(V), false, err
	}
	return val.(V), ok, nil
}

func (st *InMemoryWindowStoreWithChangelog[K, V]) Fetch(
	ctx context.Context,
	key K,
	timeFrom time.Time,
	timeTo time.Time,
	iterFunc func(int64, K, V) error,
) error {
	return st.windowStore.Fetch(ctx, key, timeFrom, timeTo, iterFunc)
}

func (st *InMemoryWindowStoreWithChangelog[K, V]) IterAll(iterFunc func(int64, K, V) error) error {
	return st.windowStore.IterAll(iterFunc)
}

func (st *InMemoryWindowStoreWithChangelog[K, V]) BackwardFetch(
	key K,
	timeFrom time.Time,
	timeTo time.Time,
	iterFunc func(int64, K, V) error,
) error {
	return st.windowStore.BackwardFetch(key, timeFrom, timeTo, iterFunc)
}

func (st *InMemoryWindowStoreWithChangelog[K, V]) FetchWithKeyRange(
	ctx context.Context,
	keyFrom K,
	keyTo K,
	timeFrom time.Time,
	timeTo time.Time,
	iterFunc func(int64, K, V) error,
) error {
	return st.windowStore.FetchWithKeyRange(ctx, keyFrom, keyTo, timeFrom, timeTo, iterFunc)
}

func (st *InMemoryWindowStoreWithChangelog[K, V]) BackwardFetchWithKeyRange(
	keyFrom K,
	keyTo K,
	timeFrom time.Time,
	timeTo time.Time,
	iterFunc func(int64, K, V) error,
) error {
	return st.windowStore.BackwardFetchWithKeyRange(keyFrom, keyTo, timeFrom, timeTo, iterFunc)
}

func (st *InMemoryWindowStoreWithChangelog[K, V]) FetchAll(
	ctx context.Context,
	timeFrom time.Time,
	timeTo time.Time,
	iterFunc func(int64, K, V) error,
) error {
	return st.windowStore.FetchAll(ctx, timeFrom, timeTo, iterFunc)
}

func (st *InMemoryWindowStoreWithChangelog[K, V]) BackwardFetchAll(
	timeFrom time.Time,
	timeTo time.Time,
	iterFunc func(int64, K, V) error,
) error {
	return st.windowStore.BackwardFetchAll(timeFrom, timeTo, iterFunc)
}

func (s *InMemoryWindowStoreWithChangelog[K, V]) DropDatabase(ctx context.Context) error {
	panic("not implemented")
}

func (s *InMemoryWindowStoreWithChangelog[K, V]) TableType() store.TABLE_TYPE {
	return store.IN_MEM
}

func (s *InMemoryWindowStoreWithChangelog[K, V]) StartTransaction(ctx context.Context) error {
	panic("not supported")
}
func (s *InMemoryWindowStoreWithChangelog[K, V]) CommitTransaction(ctx context.Context, taskRepr string, transactionID uint64) error {
	panic("not supported")
}
func (s *InMemoryWindowStoreWithChangelog[K, V]) AbortTransaction(ctx context.Context) error {
	panic("not supported")
}
func (s *InMemoryWindowStoreWithChangelog[K, V]) GetTransactionID(ctx context.Context, taskRepr string) (uint64, bool, error) {
	panic("not supported")
}
func (s *InMemoryWindowStoreWithChangelog[K, V]) SetTrackParFunc(trackParFunc exactly_once_intr.TrackProdSubStreamFunc) {
	s.trackFunc = trackParFunc
}

func ToInMemWindowTableWithChangelog[K, V any](
	mp *MaterializeParam,
	joinWindow *processor.JoinWindows,
	comparable concurrent_skiplist.Comparable,
) (*processor.MeteredProcessor, *InMemoryWindowStoreWithChangelog[K, V], error) {
	tabWithLog, err := NewInMemoryWindowStoreWithChangelog[K, V](
		joinWindow, true, comparable, mp)
	if err != nil {
		return nil, nil, err
	}
	toTableProc := processor.NewMeteredProcessor(processor.NewStoreToWindowTableProcessor(tabWithLog))
	return toTableProc, tabWithLog, nil
}
