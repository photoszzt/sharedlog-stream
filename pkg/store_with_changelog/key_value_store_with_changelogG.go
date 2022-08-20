package store_with_changelog

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/exactly_once_intr"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stats"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/utils"

	"4d63.com/optional"
)

type KeyValueStoreWithChangelogG[K, V any] struct {
	kvstore          store.CoreKeyValueStoreG[K, V]
	msgSerde         commtypes.MessageSerdeG[K, V]
	trackFunc        exactly_once_intr.TrackProdSubStreamFunc
	changelogManager *ChangelogManager[K, V]
	changelogProduce *stats.ConcurrentStatsCollector[int64]
	parNum           uint8
}

var _ = store.KeyValueStoreBackedByChangelogG[int, int](&KeyValueStoreWithChangelogG[int, int]{})

func NewKeyValueStoreWithChangelogG[K, V any](mp *MaterializeParam[K, V],
	store store.CoreKeyValueStoreG[K, V],
) (*KeyValueStoreWithChangelogG[K, V], error) {
	changelog, err := CreateChangelog(mp.changelogParam.Env,
		mp.storeName, mp.changelogParam.NumPartition, mp.serdeFormat)
	if err != nil {
		return nil, err
	}
	changelogManager, err := NewChangelogManager(changelog, mp.msgSerde, mp.changelogParam.TimeOut,
		mp.changelogParam.FlushDuration, mp.serdeFormat, mp.parNum)
	if err != nil {
		return nil, err
	}
	return &KeyValueStoreWithChangelogG[K, V]{
		kvstore:          store,
		trackFunc:        exactly_once_intr.DefaultTrackProdSubstreamFunc,
		msgSerde:         mp.msgSerde,
		changelogManager: changelogManager,
		parNum:           mp.ParNum(),
		changelogProduce: stats.NewConcurrentStatsCollector[int64](mp.storeName+"-clProd",
			stats.DEFAULT_COLLECT_DURATION),
	}, nil
}

func (st *KeyValueStoreWithChangelogG[K, V]) Name() string {
	return st.kvstore.Name()
}

func (st *KeyValueStoreWithChangelogG[K, V]) Get(ctx context.Context, key K) (V, bool, error) {
	return st.kvstore.Get(ctx, key)
}

func (st *KeyValueStoreWithChangelogG[K, V]) FlushChangelog(ctx context.Context) error {
	return st.changelogManager.Flush(ctx)
}
func (st *KeyValueStoreWithChangelogG[K, V]) ConfigureExactlyOnce(rem exactly_once_intr.ReadOnlyExactlyOnceManager,
	guarantee exactly_once_intr.GuaranteeMth,
) error {
	return st.changelogManager.ConfigExactlyOnce(rem, guarantee)
}

func (st *KeyValueStoreWithChangelogG[K, V]) Put(ctx context.Context, key K, value optional.Optional[V]) error {
	var msg commtypes.Message
	v, ok := value.Get()
	if ok {
		msg = commtypes.Message{
			Key:   key,
			Value: v,
		}
	} else {
		msg = commtypes.Message{
			Key:   key,
			Value: nil,
		}
	}
	pStart := stats.TimerBegin()
	err := st.changelogManager.Produce(ctx, msg, st.parNum, false)
	elapsed := stats.Elapsed(pStart).Microseconds()
	st.changelogProduce.AddSample(elapsed)
	if err != nil {
		return err
	}
	kBytes, err := st.msgSerde.EncodeKey(msg.Key.(K))
	if err != nil {
		return err
	}
	err = st.trackFunc(ctx, kBytes, st.changelogManager.TopicName(), st.parNum)
	if err != nil {
		return err
	}
	err = st.kvstore.Put(ctx, key, value)
	return err
}

func (st *KeyValueStoreWithChangelogG[K, V]) PutWithoutPushToChangelog(ctx context.Context, key commtypes.KeyT, value commtypes.ValueT) error {
	return st.kvstore.PutWithoutPushToChangelog(ctx, key, value)
}

func (st *KeyValueStoreWithChangelogG[K, V]) PutIfAbsent(ctx context.Context, key K, value V) (optional.Optional[V], error) {
	origVal, exists, err := st.kvstore.Get(ctx, key)
	if err != nil {
		return optional.Empty[V](), err
	}
	if !exists {
		err := st.Put(ctx, key, optional.Of(value))
		if err != nil {
			return optional.Empty[V](), err
		}
		return optional.Empty[V](), nil
	}
	return optional.Of(origVal), nil
}

func (st *KeyValueStoreWithChangelogG[K, V]) PutAll(ctx context.Context, entries []*commtypes.Message) error {
	for _, msg := range entries {
		var err error
		if utils.IsNil(msg.Value) {
			err = st.Put(ctx, msg.Key.(K), optional.Empty[V]())
		} else {
			err = st.Put(ctx, msg.Key.(K), optional.Of(msg.Value.(V)))
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (st *KeyValueStoreWithChangelogG[K, V]) Delete(ctx context.Context, key K) error {
	msg := commtypes.Message{
		Key:   key,
		Value: nil,
	}
	err := st.changelogManager.Produce(ctx, msg, st.parNum, false)
	if err != nil {
		return err
	}
	return st.kvstore.Delete(ctx, key)
}

func (st *KeyValueStoreWithChangelogG[K, V]) ApproximateNumEntries() (uint64, error) {
	return st.kvstore.ApproximateNumEntries()
}

func (st *KeyValueStoreWithChangelogG[K, V]) Range(ctx context.Context, from optional.Optional[K], to optional.Optional[K], iterFunc func(K, V) error) error {
	return st.kvstore.Range(ctx, from, to, iterFunc)
}

func (st *KeyValueStoreWithChangelogG[K, V]) TableType() store.TABLE_TYPE {
	return st.kvstore.TableType()
}

func (st *KeyValueStoreWithChangelogG[K, V]) SetTrackParFunc(trackParFunc exactly_once_intr.TrackProdSubStreamFunc) {
	st.trackFunc = trackParFunc
}
func (st *KeyValueStoreWithChangelogG[K, V]) ConsumeChangelog(ctx context.Context, parNum uint8) (*commtypes.MsgAndSeqs, error) {
	return st.changelogManager.Consume(ctx, parNum)
}
func (st *KeyValueStoreWithChangelogG[K, V]) ChangelogTopicName() string {
	return st.changelogManager.TopicName()
}
func (st *KeyValueStoreWithChangelogG[K, V]) ChangelogIsSrc() bool {
	return false
}

func (st *KeyValueStoreWithChangelogG[K, V]) Stream() sharedlog_stream.Stream {
	return st.changelogManager.Stream()
}

func (st *KeyValueStoreWithChangelogG[K, V]) GetInitialProdSeqNum() uint64 {
	return st.changelogManager.producer.GetInitialProdSeqNum(st.parNum)
}
func (st *KeyValueStoreWithChangelogG[K, V]) GetCurrentProdSeqNum() uint64 {
	return st.changelogManager.producer.GetCurrentProdSeqNum(st.parNum)
}
func (st *KeyValueStoreWithChangelogG[K, V]) ResetInitialProd() {
	st.changelogManager.producer.ResetInitialProd()
}
func (st *KeyValueStoreWithChangelogG[K, V]) SubstreamNum() uint8 {
	return st.parNum
}

func CreateInMemBTreeKVTableWithChangelogG[K, V any](mp *MaterializeParam[K, V], less store.LessFunc[K],
) (*KeyValueStoreWithChangelogG[K, V], error) {
	s := store.NewInMemoryBTreeKeyValueStoreG[K, V](mp.storeName, less)
	return NewKeyValueStoreWithChangelogG[K, V](mp, s)
}

func CreateInMemorySkipmapKVTableWithChangelogG[K, V any](mp *MaterializeParam[K, V], less store.LessFunc[K],
) (*KeyValueStoreWithChangelogG[K, V], error) {
	s := store.NewInMemorySkipmapKeyValueStoreG[K, V](mp.storeName, less)
	return NewKeyValueStoreWithChangelogG[K, V](mp, s)
}

func ToInMemSkipmapKVTableWithChangelog[K, V any](mp *MaterializeParam[K, *commtypes.ValueTimestamp],
	less store.LessFunc[K],
) (*processor.MeteredProcessor, *KeyValueStoreWithChangelogG[K, *commtypes.ValueTimestamp], error) {
	s := store.NewInMemorySkipmapKeyValueStoreG[K, *commtypes.ValueTimestamp](mp.storeName, less)
	storeWithlog, err := NewKeyValueStoreWithChangelogG[K, *commtypes.ValueTimestamp](mp, s)
	if err != nil {
		return nil, nil, err
	}
	toTableProc := processor.NewMeteredProcessor(processor.NewTableSourceProcessorWithTableG[K, V](storeWithlog))
	return toTableProc, storeWithlog, nil
}
