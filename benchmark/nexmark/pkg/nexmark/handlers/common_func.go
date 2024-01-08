package handlers

import (
	"context"
	"fmt"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/ntypes"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/exactly_once_intr"
	"sharedlog-stream/pkg/optional"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/producer_consumer"
	"sharedlog-stream/pkg/snapshot_store"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/store_with_changelog"
	"sharedlog-stream/pkg/stream_task"
	"time"

	"cs.utexas.edu/zjia/faas/types"
)

func only_bid(key optional.Option[string], value optional.Option[*ntypes.Event]) (bool, error) {
	v := value.Unwrap()
	return v.Etype == ntypes.BID, nil
}

func compareStartEndTime(a, b ntypes.StartEndTime) bool {
	return ntypes.CompareStartEndTime(a, b) < 0
}

type KVStoreStreamArgsParam[K comparable, V any] struct {
	StoreName string
	FuncName  string
	MsgSerde  commtypes.MessageGSerdeG[K, commtypes.ValueTimestampG[V]]
	Compare   store.LessFunc[K]
	SizeofK   func(K) int64
	SizeofV   func(V) int64
	UseCache  bool
}

type WinStoreStreamArgsParam[K, V any] struct {
	StoreName  string
	FuncName   string
	MsgSerde   commtypes.MessageGSerdeG[K, commtypes.ValueTimestampG[V]]
	SizeOfK    func(K) int64
	SizeOfV    func(V) int64
	CmpFunc    store.CompareFuncG[K]
	JoinWindow commtypes.EnumerableWindowDefinition
	UseCache   bool
}

func getWinStoreAndStreamArgs[K comparable, V any](
	ctx context.Context,
	env types.Environment,
	sp *common.QueryInput,
	p *WinStoreStreamArgsParam[K, V],
	ectx *processor.BaseExecutionContext,
) (cachedStore store.CachedWindowStateStore[K, commtypes.ValueTimestampG[V]],
	builder stream_task.BuildStreamTaskArgs,
	f stream_task.SetupSnapshotCallbackFunc,
	err error,
) {
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	gua := exactly_once_intr.GuaranteeMth(sp.GuaranteeMth)
	transactionalID := fmt.Sprintf("%s-%s-%d-%s", p.FuncName, sp.InputTopicNames[0],
		sp.ParNum, sp.OutputTopicNames[0])
	builder = benchutil.UpdateStreamTaskArgs(sp,
		stream_task.NewStreamTaskArgsBuilder(env, ectx, transactionalID))
	if gua == exactly_once_intr.ALIGN_CHKPT {
		cachedStore = store.NewInMemorySkipMapWindowStore[K, commtypes.ValueTimestampG[V]](p.StoreName,
			p.JoinWindow.MaxSize()+p.JoinWindow.GracePeriodMs(), p.JoinWindow.MaxSize(), false, p.CmpFunc)
		builder = builder.WindowStoreOps([]store.WindowStoreOp{cachedStore})
		f = stream_task.SetupSnapshotCallbackFunc(func(ctx context.Context, env types.Environment,
			serdeFormat commtypes.SerdeFormat,
			rs *snapshot_store.RedisSnapshotStore,
		) error {
			payloadSerde, err := commtypes.GetPayloadArrSerdeG(serdeFormat)
			if err != nil {
				return err
			}
			stream_task.SetWinStoreChkpt[K, commtypes.ValueTimestampG[V]](ctx, rs, cachedStore, payloadSerde)
			return nil
		})
	} else {
		countMp, err := store_with_changelog.NewMaterializeParamBuilder[K, commtypes.ValueTimestampG[V]]().
			MessageSerde(p.MsgSerde).
			StoreName(p.StoreName).ParNum(sp.ParNum).
			SerdeFormat(serdeFormat).
			ChangelogManagerParam(commtypes.CreateChangelogManagerParam{
				Env:           env,
				NumPartition:  sp.NumChangelogPartition,
				TimeOut:       common.SrcConsumeTimeout,
				FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
			}).BufMaxSize(sp.BufMaxSize).Build()
		if err != nil {
			return nil, nil, nil, err
		}
		countWindowStore, err := store_with_changelog.CreateInMemSkipMapWindowTableWithChangelogG(
			p.JoinWindow, false, p.CmpFunc, countMp)
		if err != nil {
			return nil, nil, nil, err
		}
		var aggStore store.CachedWindowStoreBackedByChangelogG[K, commtypes.ValueTimestampG[V]]
		if p.UseCache {
			sizeOfVTs := commtypes.ValueTimestampGSize[V]{
				ValSizeFunc: p.SizeOfV,
			}
			sizeOfKeyTs := commtypes.KeyAndWindowStartTsGSize[K]{
				KeySizeFunc: p.SizeOfK,
			}
			cacheStore := store.NewCachingWindowStoreG[K, commtypes.ValueTimestampG[V]](
				ctx, p.JoinWindow.MaxSize(), countWindowStore,
				sizeOfKeyTs.SizeOfKeyAndWindowStartTs,
				sizeOfVTs.SizeOfValueTimestamp, q5SizePerStore)
			aggStore = cacheStore
		} else {
			aggStore = countWindowStore
		}
		cachedStore = aggStore
		wsc := map[string]store.WindowStoreOpWithChangelog{countWindowStore.ChangelogTopicName(): aggStore}
		f = stream_task.SetupSnapshotCallbackFunc(func(ctx context.Context, env types.Environment,
			serdeFormat commtypes.SerdeFormat,
			rs *snapshot_store.RedisSnapshotStore,
		) error {
			payloadSerde, err := commtypes.GetPayloadArrSerdeG(serdeFormat)
			if err != nil {
				return err
			}
			stream_task.SetWinStoreWithChangelogSnapshot[K, commtypes.ValueTimestampG[V]](ctx, env, rs, aggStore, payloadSerde)
			return nil
		})
		builder = builder.WindowStoreChangelogs(wsc)
	}
	return cachedStore, builder, f, nil
}

func getKVStoreAndStreamArgs[K comparable, V any](
	ctx context.Context,
	env types.Environment,
	sp *common.QueryInput,
	p *KVStoreStreamArgsParam[K, V],
	ectx *processor.BaseExecutionContext,
) (cachedStore store.CachedKeyValueStore[K, commtypes.ValueTimestampG[V]],
	builder stream_task.BuildStreamTaskArgs,
	f stream_task.SetupSnapshotCallbackFunc,
	err error,
) {
	builder = benchutil.UpdateStreamTaskArgs(sp, stream_task.NewStreamTaskArgsBuilder(env, ectx,
		fmt.Sprintf("%s-%s-%d-%s", p.FuncName, sp.InputTopicNames[0],
			sp.ParNum, sp.OutputTopicNames[0])))
	gua := exactly_once_intr.GuaranteeMth(sp.GuaranteeMth)
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	if gua == exactly_once_intr.ALIGN_CHKPT {
		st := store.NewInMemorySkipmapKeyValueStoreG[K, commtypes.ValueTimestampG[V]](p.StoreName, p.Compare)
		st.SetInstanceId(sp.ParNum)
		err := st.SetKVSerde(serdeFormat, p.MsgSerde.GetKeySerdeG(), p.MsgSerde.GetValSerdeG())
		if err != nil {
			return nil, nil, nil, err
		}
		builder.KVStoreOps([]store.KeyValueStoreOp{st})
		f = func(ctx context.Context, env types.Environment, serdeFormat commtypes.SerdeFormat,
			rs *snapshot_store.RedisSnapshotStore,
		) error {
			payloadSerde, err := commtypes.GetPayloadArrSerdeG(serdeFormat)
			if err != nil {
				return err
			}
			stream_task.SetKVStoreChkpt[K, commtypes.ValueTimestampG[V]](
				ctx,
				rs, cachedStore, payloadSerde)
			return nil
		}
		return st, builder, f, nil
	} else {
		mp, err := store_with_changelog.NewMaterializeParamBuilder[K, commtypes.ValueTimestampG[V]]().
			MessageSerde(p.MsgSerde).
			StoreName(p.StoreName).
			ParNum(sp.ParNum).
			SerdeFormat(serdeFormat).
			ChangelogManagerParam(commtypes.CreateChangelogManagerParam{
				Env:           env,
				NumPartition:  sp.NumChangelogPartition,
				TimeOut:       common.SrcConsumeTimeout,
				FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
			}).
			BufMaxSize(sp.BufMaxSize).
			Build()
		if err != nil {
			return nil, nil, nil, err
		}
		var aggStore store.CachedKeyValueStoreBackedByChangelogG[K, commtypes.ValueTimestampG[V]]
		kvstore, err := store_with_changelog.CreateInMemorySkipmapKVTableWithChangelogG(mp, p.Compare)
		if err != nil {
			return nil, nil, nil, err
		}
		if p.UseCache {
			sizeOfVTs := commtypes.ValueTimestampGSize[V]{
				ValSizeFunc: p.SizeofV,
			}
			cacheStore := store.NewCachingKeyValueStoreG[K, commtypes.ValueTimestampG[V]](
				ctx, kvstore, p.SizeofK, sizeOfVTs.SizeOfValueTimestamp, q5SizePerStore)
			aggStore = cacheStore
		} else {
			aggStore = kvstore
		}
		kvc := map[string]store.KeyValueStoreOpWithChangelog{
			aggStore.ChangelogTopicName(): aggStore,
		}
		builder = builder.KVStoreChangelogs(kvc)
		f = func(ctx context.Context, env types.Environment, serdeFormat commtypes.SerdeFormat,
			rs *snapshot_store.RedisSnapshotStore,
		) error {
			payloadSerde, err := commtypes.GetPayloadArrSerdeG(serdeFormat)
			if err != nil {
				return err
			}
			stream_task.SetKVStoreWithChangelogSnapshot[K, commtypes.ValueTimestampG[V]](
				ctx, env,
				rs, aggStore, payloadSerde)
			return nil
		}
		return aggStore, builder, f, nil
	}
}

func getSrcSink(ctx context.Context, env types.Environment, sp *common.QueryInput,
) ([]*producer_consumer.MeteredConsumer, []producer_consumer.MeteredProducerIntr, error) {
	input_stream, output_streams, err := benchutil.GetShardedInputOutputStreams(ctx, env, sp)
	if err != nil {
		return nil, nil, err
	}
	debug.Assert(len(output_streams) == 1, "expected only one output stream")
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	inConfig := &producer_consumer.StreamConsumerConfig{
		Timeout:     common.SrcConsumeTimeout,
		SerdeFormat: serdeFormat,
	}
	outConfig := &producer_consumer.StreamSinkConfig{
		FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
		Format:        serdeFormat,
	}
	warmup := time.Duration(sp.WarmupS) * time.Second
	consumer, err := producer_consumer.NewShardedSharedLogStreamConsumer(input_stream, inConfig, sp.NumSubstreamProducer[0], sp.ParNum)
	if err != nil {
		return nil, nil, err
	}
	src := producer_consumer.NewMeteredConsumer(consumer, warmup)
	sink, err := producer_consumer.NewMeteredProducer(producer_consumer.NewShardedSharedLogStreamProducer(output_streams[0], outConfig), warmup)
	if err != nil {
		return nil, nil, err
	}
	return []*producer_consumer.MeteredConsumer{src}, []producer_consumer.MeteredProducerIntr{sink}, nil
}

func getSrcSinkUint64Key(
	ctx context.Context,
	env types.Environment,
	sp *common.QueryInput,
) ([]*producer_consumer.MeteredConsumer, []producer_consumer.MeteredProducerIntr, error) {
	input_stream, output_streams, err := benchutil.GetShardedInputOutputStreams(ctx, env, sp)
	if err != nil {
		return nil, nil, err
	}
	debug.Assert(len(output_streams) == 1, "expected only one output stream")
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	warmup := time.Duration(sp.WarmupS) * time.Second
	inConfig := &producer_consumer.StreamConsumerConfig{
		Timeout:     common.SrcConsumeTimeout,
		SerdeFormat: serdeFormat,
	}
	outConfig := &producer_consumer.StreamSinkConfig{
		FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
		Format:        serdeFormat,
	}
	consumer, err := producer_consumer.NewShardedSharedLogStreamConsumer(input_stream, inConfig, sp.NumSubstreamProducer[0], sp.ParNum)
	if err != nil {
		return nil, nil, err
	}
	src := producer_consumer.NewMeteredConsumer(consumer, warmup)
	sink, err := producer_consumer.NewMeteredProducer(producer_consumer.NewShardedSharedLogStreamProducer(output_streams[0], outConfig), warmup)
	if err != nil {
		return nil, nil, err
	}
	return []*producer_consumer.MeteredConsumer{src}, []producer_consumer.MeteredProducerIntr{sink}, nil
}

func GetSerdeFromString(serdeStr string, serdeFormat commtypes.SerdeFormat) (commtypes.Serde, error) {
	switch serdeStr {
	case "StartEndTime":
		return ntypes.GetStartEndTimeSerde(serdeFormat)
	case "AuctionIdCount":
		return ntypes.NewAuctionIdCountSerde(serdeFormat)
	case "AuctionIdCntMax":
		return ntypes.GetAuctionIdCntMaxSerde(serdeFormat)
	case "AuctionIdCategory":
		return ntypes.GetAuctionIdCategorySerde(serdeFormat)
	case "AuctionBid":
		return ntypes.GetAuctionBidSerde(serdeFormat)
	case "AuctionIdSeller":
		return ntypes.GetAuctionIDSellerSerde(serdeFormat)
	case "ChangeUint64":
		return commtypes.GetChangeSerde(serdeFormat, commtypes.Uint64Serde{})
	case "ChangePriceTime":
		ptSerde, err := ntypes.GetPriceTimeSerde(serdeFormat)
		if err != nil {
			return nil, err
		}
		return commtypes.GetChangeSerde(serdeFormat, ptSerde)
	case "BidAndMax":
		return ntypes.GetBidAndMaxSerde(serdeFormat)
	case "Event":
		return ntypes.GetEventSerde(serdeFormat)
	case "Uint64":
		return commtypes.Uint64Serde{}, nil
	case "Float64":
		return commtypes.Float64Serde{}, nil
	case "String":
		return commtypes.StringSerde{}, nil
	case "PersonTime":
		return ntypes.GetPersonTimeSerde(serdeFormat)
	case "PriceTime":
		return ntypes.GetPriceTimeSerde(serdeFormat)
	case "NameCityStateId":
		return ntypes.GetNameCityStateIdSerde(serdeFormat)
	default:
		return nil, fmt.Errorf("Unrecognized serde string %s", serdeStr)
	}
}
