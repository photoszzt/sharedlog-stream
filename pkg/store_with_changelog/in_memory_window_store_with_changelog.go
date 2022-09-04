package store_with_changelog

import (
	"context"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/exactly_once_intr"
	"sharedlog-stream/pkg/optional"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stats"
	"sharedlog-stream/pkg/store"
	"time"

	"github.com/rs/zerolog/log"
)

type InMemoryWindowStoreWithChangelog[K, V any] struct {
	msgSerde         commtypes.MessageGSerdeG[commtypes.KeyAndWindowStartTsG[K], V]
	originKeySerde   commtypes.SerdeG[K]
	windowStore      store.CoreWindowStore
	trackFunc        exactly_once_intr.TrackProdSubStreamFunc
	changelogManager *ChangelogManager[commtypes.KeyAndWindowStartTsG[K], V]
	// changeLogProduce stats.ConcurrentInt64Collector
	// storePutLatency stats.ConcurrentInt64Collector
	trackFuncLat *stats.ConcurrentStatsCollector[int64]
	parNum       uint8
}

var _ = store.WindowStoreBackedByChangelog(&InMemoryWindowStoreWithChangelog[int, string]{})

func NewInMemoryWindowStoreWithChangelog[K, V any](
	winStore store.CoreWindowStore,
	mp *MaterializeParam[K, V],
) (*InMemoryWindowStoreWithChangelog[K, V], error) {
	changelogManager, msgSerde, err := createChangelogManagerAndUpdateMsgSerde(mp)
	if err != nil {
		return nil, err
	}
	return &InMemoryWindowStoreWithChangelog[K, V]{
		windowStore:      winStore,
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

func CreateInMemWindowStoreWithChangelog[K, V any](
	winDefs commtypes.EnumerableWindowDefinition,
	retainDuplicates bool,
	comparable store.CompareFunc,
	mp *MaterializeParam[K, V],
) (*InMemoryWindowStoreWithChangelog[K, V], error) {
	winStore := store.NewInMemoryWindowStore(mp.storeName,
		winDefs.MaxSize()+winDefs.GracePeriodMs(), winDefs.MaxSize(), retainDuplicates, comparable)
	return NewInMemoryWindowStoreWithChangelog(winStore, mp)
}

func createChangelogManagerAndUpdateMsgSerde[K, V any](mp *MaterializeParam[K, V]) (
	*ChangelogManager[commtypes.KeyAndWindowStartTsG[K], V],
	commtypes.MessageGSerdeG[commtypes.KeyAndWindowStartTsG[K], V], error,
) {
	changelog, err := CreateChangelog(mp.changelogParam.Env,
		mp.storeName, mp.changelogParam.NumPartition,
		mp.serdeFormat)
	if err != nil {
		return nil, nil, err
	}
	var keyAndWindowStartTsSerde commtypes.SerdeG[commtypes.KeyAndWindowStartTsG[K]]
	if mp.serdeFormat == commtypes.JSON {
		keyAndWindowStartTsSerde = commtypes.KeyAndWindowStartTsJSONSerdeG[K]{
			KeyJSONSerde: mp.msgSerde.GetKeySerdeG(),
		}
	} else if mp.serdeFormat == commtypes.MSGP {
		keyAndWindowStartTsSerde = commtypes.KeyAndWindowStartTsMsgpSerdeG[K]{
			KeyMsgpSerde: mp.msgSerde.GetKeySerdeG(),
		}
	} else {
		return nil, nil, common_errors.ErrUnrecognizedSerdeFormat
	}
	msgSerde, err := commtypes.GetMsgGSerdeG(mp.serdeFormat, keyAndWindowStartTsSerde, mp.msgSerde.GetValSerdeG())
	if err != nil {
		return nil, nil, err
	}
	changelogManager, err := NewChangelogManager(changelog, msgSerde, mp.changelogParam.TimeOut,
		mp.changelogParam.FlushDuration, mp.serdeFormat, mp.parNum)
	if err != nil {
		return nil, nil, err
	}
	return changelogManager, msgSerde, nil
}

func NewInMemoryWindowStoreWithChangelogForTest[K, V any](
	retentionPeriod int64, windowSize int64,
	retainDuplicates bool,
	comparable store.CompareFunc,
	mp *MaterializeParam[K, V],
) (*InMemoryWindowStoreWithChangelog[K, V], error) {
	changelogManager, msgSerde, err := createChangelogManagerAndUpdateMsgSerde(mp)
	if err != nil {
		return nil, err
	}
	return &InMemoryWindowStoreWithChangelog[K, V]{
		windowStore: store.NewInMemoryWindowStore(mp.storeName,
			retentionPeriod, windowSize, retainDuplicates, comparable),
		changelogManager: changelogManager,
		msgSerde:         msgSerde,
		originKeySerde:   mp.msgSerde.GetKeySerdeG(),
		parNum:           mp.ParNum(),
		trackFunc:        exactly_once_intr.DefaultTrackProdSubstreamFunc,
	}, nil
}

func (st *InMemoryWindowStoreWithChangelog[K, V]) Name() string {
	return st.windowStore.Name()
}

func (st *InMemoryWindowStoreWithChangelog[K, V]) Snapshot() [][]byte {
	panic("not implemented")
}

func (st *InMemoryWindowStoreWithChangelog[K, V]) ChangelogManager() *ChangelogManager[commtypes.KeyAndWindowStartTsG[K], V] {
	return st.changelogManager
}

func (st *InMemoryWindowStoreWithChangelog[K, V]) Flush(ctx context.Context) error {
	return st.changelogManager.Flush(ctx)
}

func (st *InMemoryWindowStoreWithChangelog[K, V]) Put(ctx context.Context,
	key commtypes.KeyT, value commtypes.ValueT, windowStartTimestamp int64,
) error {
	keyTs := commtypes.KeyAndWindowStartTsG[K]{
		Key:           key.(K),
		WindowStartTs: windowStartTimestamp,
	}
	msg := commtypes.MessageG[commtypes.KeyAndWindowStartTsG[K], V]{
		Key:   optional.Some(keyTs),
		Value: optional.Some(value.(V)),
	}
	msgSerOp, err := commtypes.MsgGToMsgSer(msg, st.msgSerde.GetKeySerdeG(), st.msgSerde.GetValSerdeG())
	if err != nil {
		return err
	}
	msgSer, ok := msgSerOp.Take()
	if ok {
		// pStart := stats.TimerBegin()
		err = st.changelogManager.Produce(ctx, msgSer, st.parNum)
		// elapsed := stats.Elapsed(pStart).Microseconds()
		// st.changeLogProduce.AddSample(elapsed)
		if err != nil {
			return err
		}
		tStart := stats.TimerBegin()
		kBytes, err := st.originKeySerde.Encode(key.(K))
		if err != nil {
			return err
		}
		err = st.trackFunc(ctx, kBytes, st.changelogManager.TopicName(), st.parNum)
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
	} else {
		log.Warn().Msgf("get empty key and value")
		return nil
	}
}

func (st *InMemoryWindowStoreWithChangelog[K, V]) PutWithoutPushToChangelog(ctx context.Context,
	key commtypes.KeyT, value commtypes.ValueT,
) error {
	keyTs := key.(commtypes.KeyAndWindowStartTsG[K])
	return st.windowStore.Put(ctx, keyTs.Key, value, keyTs.WindowStartTs)
}

func (st *InMemoryWindowStoreWithChangelog[K, V]) Get(ctx context.Context, key commtypes.KeyT, windowStartTimestamp int64) (commtypes.ValueT, bool, error) {
	val, ok, err := st.windowStore.Get(ctx, key, windowStartTimestamp)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, err
	}
	return val, ok, nil
}

func (st *InMemoryWindowStoreWithChangelog[K, V]) Fetch(
	ctx context.Context,
	key commtypes.KeyT,
	timeFrom time.Time,
	timeTo time.Time,
	iterFunc func(int64, commtypes.KeyT, commtypes.ValueT) error,
) error {
	return st.windowStore.Fetch(ctx, key, timeFrom, timeTo, iterFunc)
}

func (st *InMemoryWindowStoreWithChangelog[K, V]) IterAll(iterFunc func(int64, commtypes.KeyT, commtypes.ValueT) error) error {
	return st.windowStore.IterAll(iterFunc)
}

func (st *InMemoryWindowStoreWithChangelog[K, V]) FetchWithKeyRange(
	ctx context.Context,
	keyFrom commtypes.KeyT,
	keyTo commtypes.KeyT,
	timeFrom time.Time,
	timeTo time.Time,
	iterFunc func(int64, commtypes.KeyT, commtypes.ValueT) error,
) error {
	return st.windowStore.FetchWithKeyRange(ctx, keyFrom, keyTo, timeFrom, timeTo, iterFunc)
}

func (st *InMemoryWindowStoreWithChangelog[K, V]) FetchAll(
	ctx context.Context,
	timeFrom time.Time,
	timeTo time.Time,
	iterFunc func(int64, commtypes.KeyT, commtypes.ValueT) error,
) error {
	return st.windowStore.FetchAll(ctx, timeFrom, timeTo, iterFunc)
}

func (s *InMemoryWindowStoreWithChangelog[K, V]) TableType() store.TABLE_TYPE {
	return store.IN_MEM
}

func (s *InMemoryWindowStoreWithChangelog[K, V]) SetTrackParFunc(trackParFunc exactly_once_intr.TrackProdSubStreamFunc) {
	s.trackFunc = trackParFunc
}
func (s *InMemoryWindowStoreWithChangelog[K, V]) ConsumeOneLogEntry(ctx context.Context, parNum uint8) (int, error) {
	msgSeq, err := s.changelogManager.Consume(ctx, parNum)
	if err != nil {
		return 0, err
	}
	count := 0
	if msgSeq.MsgArr != nil {
		for _, msg := range msgSeq.MsgArr {
			if msg.Key.IsNone() && msg.Value.IsNone() {
				continue
			}
			count += 1
			k := msg.Key.Unwrap()
			err := s.PutWithoutPushToChangelog(ctx, k.Key, msg.Value)
			if err != nil {
				return 0, err
			}
		}
	} else {
		msg := msgSeq.Msg
		if msg.Key.IsNone() && msg.Value.IsNone() {
			return 0, nil
		}
		count += 1
		k := msg.Key.Unwrap()
		err := s.PutWithoutPushToChangelog(ctx, k.Key, msg.Value)
		if err != nil {
			return 0, err
		}
	}
	return count, nil
}
func (s *InMemoryWindowStoreWithChangelog[K, V]) ConfigureExactlyOnce(rem exactly_once_intr.ReadOnlyExactlyOnceManager,
	guarantee exactly_once_intr.GuaranteeMth) error {
	return s.changelogManager.ConfigExactlyOnce(rem, guarantee)
}
func (s *InMemoryWindowStoreWithChangelog[K, V]) ChangelogTopicName() string {
	return s.changelogManager.TopicName()
}
func (s *InMemoryWindowStoreWithChangelog[K, V]) Stream() sharedlog_stream.Stream {
	return s.changelogManager.Stream()
}

func (s *InMemoryWindowStoreWithChangelog[K, V]) GetInitialProdSeqNum() uint64 {
	return s.changelogManager.producer.GetInitialProdSeqNum(s.parNum)
}
func (s *InMemoryWindowStoreWithChangelog[K, V]) GetCurrentProdSeqNum() uint64 {
	return s.changelogManager.producer.GetCurrentProdSeqNum(s.parNum)
}
func (s *InMemoryWindowStoreWithChangelog[K, V]) ResetInitialProd() {
	s.changelogManager.producer.ResetInitialProd()
}
func (s *InMemoryWindowStoreWithChangelog[K, V]) SubstreamNum() uint8 {
	return s.parNum
}

func ToInMemWindowTableWithChangelog[K, V any](
	joinWindow *commtypes.JoinWindows,
	retainDuplicates bool,
	comparable store.CompareFunc,
	mp *MaterializeParam[K, V],
) (*processor.MeteredProcessor, *InMemoryWindowStoreWithChangelog[K, V], error) {
	tabWithLog, err := CreateInMemWindowStoreWithChangelog(
		joinWindow, retainDuplicates, comparable, mp)
	if err != nil {
		return nil, nil, err
	}
	toTableProc := processor.NewMeteredProcessor(processor.NewStoreToWindowTableProcessor(tabWithLog))
	return toTableProc, tabWithLog, nil
}
