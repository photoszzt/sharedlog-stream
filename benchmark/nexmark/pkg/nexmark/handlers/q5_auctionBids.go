package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/ntypes"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/debug"
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

type q5AuctionBids struct {
	env      types.Environment
	funcName string
	useCache bool
}

func NewQ5AuctionBids(env types.Environment, funcName string) *q5AuctionBids {
	useCache := common.CheckCacheConfig()
	return &q5AuctionBids{
		env:      env,
		funcName: funcName,
		useCache: useCache,
	}
}

func (h *q5AuctionBids) Call(ctx context.Context, input []byte) ([]byte, error) {
	sp := &common.QueryInput{}
	err := json.Unmarshal(input, sp)
	if err != nil {
		return nil, err
	}
	output := h.processQ5AuctionBids(ctx, sp)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		return nil, err
	}
	return common.CompressData(encodedOutput), nil
}

func (h *q5AuctionBids) getSrcSink(ctx context.Context, sp *common.QueryInput,
) ([]*producer_consumer.MeteredConsumer, []producer_consumer.MeteredProducerIntr, error) {
	input_stream, output_streams, err := benchutil.GetShardedInputOutputStreams(ctx, h.env, sp)
	if err != nil {
		return nil, nil, err
	}
	debug.Assert(len(output_streams) == 1, "expected only one output stream")
	warmup := time.Duration(sp.WarmupS) * time.Second
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	consumer, err := producer_consumer.NewShardedSharedLogStreamConsumer(input_stream,
		&producer_consumer.StreamConsumerConfig{
			Timeout:     time.Duration(5) * time.Second,
			SerdeFormat: serdeFormat,
		}, sp.NumSubstreamProducer[0], sp.ParNum)
	if err != nil {
		return nil, nil, err
	}
	src := producer_consumer.NewMeteredConsumer(consumer, warmup)
	sink, err := producer_consumer.NewConcurrentMeteredSyncProducer(
		producer_consumer.NewShardedSharedLogStreamProducer(output_streams[0],
			&producer_consumer.StreamSinkConfig{
				FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
				Format:        serdeFormat,
			}), warmup)
	if err != nil {
		return nil, nil, err
	}
	src.SetInitialSource(false)
	return []*producer_consumer.MeteredConsumer{src}, []producer_consumer.MeteredProducerIntr{sink}, nil
}

func (h *q5AuctionBids) getCountAggProc(ctx context.Context, sp *common.QueryInput,
) (countProc *processor.MeteredProcessorG[uint64, *ntypes.Event, commtypes.WindowedKeyG[uint64], commtypes.ChangeG[uint64]],
	wsc map[string]store.WindowStoreOpWithChangelog,
	setSnapCallbackFunc stream_task.SetupSnapshotCallbackFunc,
	err error,
) {
	hopWindow, err := commtypes.NewTimeWindowsWithGrace(time.Duration(10)*time.Second, time.Duration(5)*time.Second)
	if err != nil {
		return nil, nil, nil, err
	}
	hopWindow, err = hopWindow.AdvanceBy(time.Duration(2) * time.Second)
	if err != nil {
		return nil, nil, nil, err
	}
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	msgSerde, err := processor.MsgSerdeWithValueTsG[uint64, uint64](serdeFormat,
		commtypes.Uint64SerdeG{}, commtypes.Uint64SerdeG{})
	if err != nil {
		return nil, nil, nil, err
	}
	countStoreName := "auctionBidsCountStore"
	countMp, err := store_with_changelog.NewMaterializeParamBuilder[uint64, commtypes.ValueTimestampG[uint64]]().
		MessageSerde(msgSerde).StoreName(countStoreName).ParNum(sp.ParNum).
		SerdeFormat(serdeFormat).
		ChangelogManagerParam(commtypes.CreateChangelogManagerParam{
			Env:           h.env,
			NumPartition:  sp.NumChangelogPartition,
			TimeOut:       common.SrcConsumeTimeout,
			FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
		}).BufMaxSize(sp.BufMaxSize).Build()
	if err != nil {
		return nil, nil, nil, err
	}
	countWindowStore, err := store_with_changelog.CreateInMemSkipMapWindowTableWithChangelogG(
		hopWindow, false, store.IntegerCompare[uint64], countMp)
	if err != nil {
		return nil, nil, nil, err
	}
	var aggStore store.WindowStoreBackedByChangelogG[uint64, commtypes.ValueTimestampG[uint64]]
	if h.useCache {
		sizeOfVTs := commtypes.ValueTimestampGSize[uint64]{
			ValSizeFunc: commtypes.SizeOfUint64,
		}
		sizeOfKeyTs := commtypes.KeyAndWindowStartTsGSize[uint64]{
			KeySizeFunc: commtypes.SizeOfUint64,
		}
		cacheStore := store.NewCachingWindowStoreG[uint64, commtypes.ValueTimestampG[uint64]](
			ctx, hopWindow.SizeMs, countWindowStore, sizeOfKeyTs.SizeOfKeyAndWindowStartTs, sizeOfVTs.SizeOfValueTimestamp, q5SizePerStore)
		aggStore = cacheStore
	} else {
		aggStore = countWindowStore
	}
	countProc = processor.NewMeteredProcessorG(processor.NewStreamWindowAggregateProcessorG[uint64, *ntypes.Event, uint64](
		"countProc", aggStore,
		processor.InitializerFuncG[uint64](func() optional.Option[uint64] { return optional.Some(uint64(0)) }),
		processor.AggregatorFuncG[uint64, *ntypes.Event, uint64](func(key uint64, value *ntypes.Event, aggregate optional.Option[uint64]) optional.Option[uint64] {
			val := aggregate.Unwrap()
			return optional.Some(val + 1)
		}), hopWindow, h.useCache))
	wsc = map[string]store.WindowStoreOpWithChangelog{countWindowStore.ChangelogTopicName(): aggStore}
	setSnapCallbackFunc = stream_task.SetupSnapshotCallbackFunc(func(ctx context.Context, env types.Environment,
		serdeFormat commtypes.SerdeFormat,
		rs *snapshot_store.RedisSnapshotStore,
	) error {
		payloadSerde, err := commtypes.GetPayloadArrSerdeG(serdeFormat)
		if err != nil {
			return err
		}
		stream_task.SetWinStoreSnapshot(ctx, env, rs, aggStore, payloadSerde)
		return nil
	})
	return countProc, wsc, setSnapCallbackFunc, nil
}

func (h *q5AuctionBids) processQ5AuctionBids(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	var seSerde commtypes.SerdeG[ntypes.StartEndTime]
	var aucIdCountSerde commtypes.SerdeG[ntypes.AuctionIdCount]
	if serdeFormat == commtypes.JSON {
		seSerde = ntypes.StartEndTimeJSONSerdeG{}
		aucIdCountSerde = ntypes.AuctionIdCountJSONSerdeG{}
	} else if serdeFormat == commtypes.MSGP {
		seSerde = ntypes.StartEndTimeMsgpSerdeG{}
		aucIdCountSerde = ntypes.AuctionIdCountMsgpSerdeG{}
	} else {
		return common.GenErrFnOutput(fmt.Errorf("serde format should be either json or msgp; but %v is given", sp.SerdeFormat))
	}

	eventSerde, err := ntypes.GetEventSerdeG(serdeFormat)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	srcMsgSerde, err := commtypes.GetMsgGSerdeG[uint64](serdeFormat, commtypes.Uint64SerdeG{}, eventSerde)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	sinkMsgSerde, err := commtypes.GetMsgGSerdeG(serdeFormat, seSerde, aucIdCountSerde)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	srcs, sinks, err := h.getSrcSink(ctx, sp)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	ectx := processor.NewExecutionContext(srcs,
		sinks, h.funcName, sp.ScaleEpoch, sp.ParNum)
	countProc, wsc, setSnapCallbackFunc, err := h.getCountAggProc(ctx, sp)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	tabToStreamProc := processor.NewTableToStreamProcessorG[commtypes.WindowedKeyG[uint64], uint64]()
	mapProc := processor.NewStreamMapProcessorG[commtypes.WindowedKeyG[uint64], uint64, ntypes.StartEndTime, ntypes.AuctionIdCount]("groupByAuction",
		processor.MapperFuncG[commtypes.WindowedKeyG[uint64], uint64, ntypes.StartEndTime, ntypes.AuctionIdCount](
			func(key optional.Option[commtypes.WindowedKeyG[uint64]], value optional.Option[uint64]) (optional.Option[ntypes.StartEndTime], optional.Option[ntypes.AuctionIdCount], error) {
				k := key.Unwrap()
				v := value.Unwrap()
				newKey := ntypes.StartEndTime{
					StartTimeMs: k.Window.Start(),
					EndTimeMs:   k.Window.End(),
				}
				newVal := ntypes.AuctionIdCount{
					AucId: k.Key,
					Count: v,
				}
				return optional.Some(newKey), optional.Some(newVal), nil
			}))
	outProc := processor.NewGroupByOutputProcessorG("subG2Proc", sinks[0], &ectx, sinkMsgSerde)
	countProc.NextProcessor(tabToStreamProc)
	tabToStreamProc.NextProcessor(mapProc)
	mapProc.NextProcessor(outProc)

	task := stream_task.NewStreamTaskBuilder().
		AppProcessFunc(func(ctx context.Context, task *stream_task.StreamTask, args processor.ExecutionContext) (
			*common.FnOutput, optional.Option[commtypes.RawMsgAndSeq],
		) {
			return stream_task.CommonProcess(ctx, task, args.(*processor.BaseExecutionContext),
				func(ctx context.Context, msg commtypes.MessageG[uint64, *ntypes.Event], argsTmp interface{}) error {
					return countProc.Process(ctx, msg)
				}, srcMsgSerde)
		}).
		Build()
	transactionalID := fmt.Sprintf("%s-%s-%d-%s", h.funcName, sp.InputTopicNames[0],
		sp.ParNum, sp.OutputTopicNames[0])
	builder := stream_task.NewStreamTaskArgsBuilder(h.env, &ectx, transactionalID)
	streamTaskArgs := benchutil.UpdateStreamTaskArgs(sp, builder).
		WindowStoreChangelogs(wsc).Build()
	return stream_task.ExecuteApp(ctx, task, streamTaskArgs, setSnapCallbackFunc, func() { outProc.OutputRemainingStats() })
}
