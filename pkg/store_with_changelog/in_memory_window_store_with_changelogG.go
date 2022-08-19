package store_with_changelog

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/exactly_once_intr"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stats"
	"sharedlog-stream/pkg/store"
	"time"

	"4d63.com/optional"
)

type InMemoryWindowStoreWithChangelogG[K, V any] struct {
	msgSerde         commtypes.MessageSerdeG[commtypes.KeyAndWindowStartTsG[K], V]
	originKeySerde   commtypes.SerdeG[K]
	windowStore      store.CoreWindowStoreG[K, V]
	trackFunc        exactly_once_intr.TrackProdSubStreamFunc
	changelogManager *ChangelogManager[commtypes.KeyAndWindowStartTsG[K], V]
	// changeLogProduce stats.ConcurrentInt64Collector
	// storePutLatency stats.ConcurrentInt64Collector
	trackFuncLat *stats.ConcurrentStatsCollector[int64]
	parNum       uint8
}

var _ = store.WindowStoreBackedByChangelogG[int, string](&InMemoryWindowStoreWithChangelogG[int, string]{})

func NewInMemoryWindowStoreWithChangelogG[K, V any](
	windowStore store.CoreWindowStoreG[K, V],
	mp *MaterializeParam[K, V],
) (*InMemoryWindowStoreWithChangelogG[K, V], error) {
	changelogManager, msgSerde, err := createChangelogManagerAndUpdateMsgSerde(mp)
	if err != nil {
		return nil, err
	}
	return &InMemoryWindowStoreWithChangelogG[K, V]{
		windowStore:      windowStore,
		msgSerde:         msgSerde,
		originKeySerde:   mp.msgSerde.GetKeySerdeG(),
		parNum:           mp.ParNum(),
		trackFunc:        exactly_once_intr.DefaultTrackProdSubstreamFunc,
		changelogManager: changelogManager,
		// changeLogProduce: stats.NewConcurrentInt64Collector(mp.storeName+"-clProd", stats.DEFAULT_COLLECT_DURATION),
		// storePutLatency: stats.NewConcurrentInt64Collector(mp.storeName+"-storePutLatency", stats.DEFAULT_COLLECT_DURATION),
		trackFuncLat: stats.NewConcurrentStatsCollector[int64](mp.storeName+"-trackFuncLat", stats.DEFAULT_COLLECT_DURATION),
	}, nil
}

func (st *InMemoryWindowStoreWithChangelogG[K, V]) Name() string {
	return st.windowStore.Name()
}

func (st *InMemoryWindowStoreWithChangelogG[K, V]) ChangelogManager() *ChangelogManager[commtypes.KeyAndWindowStartTsG[K], V] {
	return st.changelogManager
}

func (st *InMemoryWindowStoreWithChangelogG[K, V]) FlushChangelog(ctx context.Context) error {
	return st.changelogManager.Flush(ctx)
}

func (st *InMemoryWindowStoreWithChangelogG[K, V]) Put(ctx context.Context,
	key K, value optional.Optional[V], windowStartTimestamp int64,
) error {
	keyTs := commtypes.KeyAndWindowStartTsG[K]{
		Key:           key,
		WindowStartTs: windowStartTimestamp,
	}
	var msg commtypes.Message
	v, ok := value.Get()
	if ok {
		msg = commtypes.Message{
			Key:   keyTs,
			Value: v,
		}
	} else {
		msg = commtypes.Message{
			Key:   keyTs,
			Value: nil,
		}
	}
	// pStart := stats.TimerBegin()
	err := st.changelogManager.Produce(ctx, msg, st.parNum, false)
	// elapsed := stats.Elapsed(pStart).Microseconds()
	// st.changeLogProduce.AddSample(elapsed)
	if err != nil {
		return err
	}
	tStart := stats.TimerBegin()
	err = st.trackFunc(ctx, key, commtypes.EncoderFunc(func(i interface{}) ([]byte, error) {
		if i == nil {
			return nil, nil
		}
		return st.originKeySerde.Encode(i.(K))
	}), st.changelogManager.TopicName(), st.parNum)
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

func (st *InMemoryWindowStoreWithChangelogG[K, V]) PutWithoutPushToChangelog(ctx context.Context,
	key commtypes.KeyT, value commtypes.ValueT,
) error {
	keyTs := key.(commtypes.KeyAndWindowStartTsG[K])
	return st.windowStore.Put(ctx, keyTs.Key, optional.Of(value.(V)), keyTs.WindowStartTs)
}

func (st *InMemoryWindowStoreWithChangelogG[K, V]) Get(ctx context.Context, key K, windowStartTimestamp int64) (V, bool, error) {
	return st.windowStore.Get(ctx, key, windowStartTimestamp)
}

func (st *InMemoryWindowStoreWithChangelogG[K, V]) Fetch(
	ctx context.Context,
	key K,
	timeFrom time.Time,
	timeTo time.Time,
	iterFunc func(int64, K, V) error,
) error {
	return st.windowStore.Fetch(ctx, key, timeFrom, timeTo, iterFunc)
}

func (st *InMemoryWindowStoreWithChangelogG[K, V]) IterAll(iterFunc func(int64, K, V) error) error {
	return st.windowStore.IterAll(iterFunc)
}

func (st *InMemoryWindowStoreWithChangelogG[K, V]) FetchWithKeyRange(
	ctx context.Context,
	keyFrom K,
	keyTo K,
	timeFrom time.Time,
	timeTo time.Time,
	iterFunc func(int64, K, V) error,
) error {
	return st.windowStore.FetchWithKeyRange(ctx, keyFrom, keyTo, timeFrom, timeTo, iterFunc)
}

func (st *InMemoryWindowStoreWithChangelogG[K, V]) FetchAll(
	ctx context.Context,
	timeFrom time.Time,
	timeTo time.Time,
	iterFunc func(int64, K, V) error,
) error {
	return st.windowStore.FetchAll(ctx, timeFrom, timeTo, iterFunc)
}

func (s *InMemoryWindowStoreWithChangelogG[K, V]) TableType() store.TABLE_TYPE {
	return store.IN_MEM
}

func (s *InMemoryWindowStoreWithChangelogG[K, V]) SetTrackParFunc(trackParFunc exactly_once_intr.TrackProdSubStreamFunc) {
	s.trackFunc = trackParFunc
}
func (s *InMemoryWindowStoreWithChangelogG[K, V]) ConsumeChangelog(ctx context.Context, parNum uint8) (*commtypes.MsgAndSeqs, error) {
	return s.changelogManager.Consume(ctx, parNum)
}
func (s *InMemoryWindowStoreWithChangelogG[K, V]) ConfigureExactlyOnce(rem exactly_once_intr.ReadOnlyExactlyOnceManager,
	guarantee exactly_once_intr.GuaranteeMth) error {
	return s.changelogManager.ConfigExactlyOnce(rem, guarantee)
}
func (s *InMemoryWindowStoreWithChangelogG[K, V]) ChangelogTopicName() string {
	return s.changelogManager.TopicName()
}
func (s *InMemoryWindowStoreWithChangelogG[K, V]) Stream() sharedlog_stream.Stream {
	return s.changelogManager.Stream()
}

func (s *InMemoryWindowStoreWithChangelogG[K, V]) GetInitialProdSeqNum() uint64 {
	return s.changelogManager.producer.GetInitialProdSeqNum(s.parNum)
}
func (s *InMemoryWindowStoreWithChangelogG[K, V]) GetCurrentProdSeqNum() uint64 {
	return s.changelogManager.producer.GetCurrentProdSeqNum(s.parNum)
}
func (s *InMemoryWindowStoreWithChangelogG[K, V]) ResetInitialProd() {
	s.changelogManager.producer.ResetInitialProd()
}
func (s *InMemoryWindowStoreWithChangelogG[K, V]) SubstreamNum() uint8 {
	return s.parNum
}

func ToInMemSkipMapWindowTableWithChangelogG[K, V any](
	joinWindow *processor.JoinWindows,
	retainDuplicates bool,
	comparable store.CompareFuncG[K],
	mp *MaterializeParam[K, V],
) (*processor.MeteredProcessor, *InMemoryWindowStoreWithChangelogG[K, V], error) {
	winTab := store.NewInMemorySkipMapWindowStore[K, V](mp.storeName,
		joinWindow.MaxSize()+joinWindow.GracePeriodMs(), joinWindow.MaxSize(), retainDuplicates, comparable)
	tabWithLog, err := NewInMemoryWindowStoreWithChangelogG[K, V](winTab, mp)
	if err != nil {
		return nil, nil, err
	}
	toTableProc := processor.NewMeteredProcessor(processor.NewStoreToWindowTableProcessorG[K, V](tabWithLog))
	return toTableProc, tabWithLog, nil
}

func CreateInMemSkipMapWindowTableWithChangelogG[K, V any](
	joinWindow *processor.JoinWindows,
	retainDuplicates bool,
	comparable store.CompareFuncG[K],
	mp *MaterializeParam[K, V],
) (*InMemoryWindowStoreWithChangelogG[K, V], error) {
	winTab := store.NewInMemorySkipMapWindowStore[K, V](mp.storeName,
		joinWindow.MaxSize()+joinWindow.GracePeriodMs(), joinWindow.MaxSize(), retainDuplicates, comparable)
	return NewInMemoryWindowStoreWithChangelogG[K, V](winTab, mp)
}
