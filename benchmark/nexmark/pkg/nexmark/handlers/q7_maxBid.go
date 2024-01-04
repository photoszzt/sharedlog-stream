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

type q7MaxBid struct {
	env         types.Environment
	funcName    string
	useCache    bool
	inMsgSerde  commtypes.MessageGSerdeG[ntypes.StartEndTime, *ntypes.Event]
	outMsgSerde commtypes.MessageGSerdeG[uint64, ntypes.StartEndTime]
	msgSerde    commtypes.MessageGSerdeG[ntypes.StartEndTime, commtypes.ValueTimestampG[uint64]]
}

const (
	Q7SizePerStore = 5 * 1024 * 1024
)

func NewQ7MaxBid(env types.Environment, funcName string) types.FuncHandler {
	useCache := common.CheckCacheConfig()
	return &q7MaxBid{
		env:      env,
		funcName: funcName,
		useCache: useCache,
	}
}

func (h *q7MaxBid) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &common.QueryInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output := h.q7MaxBidByPrice(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	return common.CompressData(encodedOutput), nil
}

func (h *q7MaxBid) getSrcSink(ctx context.Context, sp *common.QueryInput,
) ([]*producer_consumer.MeteredConsumer, []producer_consumer.MeteredProducerIntr, error) {
	input_stream, output_streams, err := benchutil.GetShardedInputOutputStreams(ctx, h.env, sp)
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
	sink, err := producer_consumer.NewMeteredProducer(
		producer_consumer.NewShardedSharedLogStreamProducer(output_streams[0], outConfig),
		warmup)
	if err != nil {
		return nil, nil, err
	}
	src.SetInitialSource(false)
	return []*producer_consumer.MeteredConsumer{src}, []producer_consumer.MeteredProducerIntr{sink}, nil
}

func (h *q7MaxBid) setupSerde(serdeFormat commtypes.SerdeFormat) *common.FnOutput {
	eventSerde, err := ntypes.GetEventSerdeG(serdeFormat)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	seSerde, err := ntypes.GetStartEndTimeSerdeG(serdeFormat)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	h.inMsgSerde, err = commtypes.GetMsgGSerdeG(serdeFormat, seSerde, eventSerde)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	h.outMsgSerde, err = commtypes.GetMsgGSerdeG[uint64](serdeFormat, commtypes.Uint64SerdeG{}, seSerde)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	h.msgSerde, err = processor.MsgSerdeWithValueTsG[ntypes.StartEndTime, uint64](serdeFormat, seSerde, commtypes.Uint64SerdeG{})
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	return nil
}

func (h *q7MaxBid) setupAggStore(ctx context.Context, sp *common.QueryInput) (
	store.CachedKeyValueStoreBackedByChangelogG[ntypes.StartEndTime, commtypes.ValueTimestampG[uint64]],
	*common.FnOutput,
) {
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	maxBidByWinStoreName := "maxBidByWinKVStore"
	mp, err := store_with_changelog.NewMaterializeParamBuilder[ntypes.StartEndTime, commtypes.ValueTimestampG[uint64]]().
		MessageSerde(h.msgSerde).
		StoreName(maxBidByWinStoreName).
		ParNum(sp.ParNum).
		SerdeFormat(serdeFormat).
		ChangelogManagerParam(commtypes.CreateChangelogManagerParam{
			Env:           h.env,
			NumPartition:  sp.NumChangelogPartition,
			TimeOut:       common.SrcConsumeTimeout,
			FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
		}).BufMaxSize(sp.BufMaxSize).Build()
	if err != nil {
		return nil, common.GenErrFnOutput(err)
	}
	compare := func(a, b ntypes.StartEndTime) bool {
		return ntypes.CompareStartEndTime(a, b) < 0
	}
	var aggStore store.CachedKeyValueStoreBackedByChangelogG[ntypes.StartEndTime, commtypes.ValueTimestampG[uint64]]
	kvstore, err := store_with_changelog.CreateInMemorySkipmapKVTableWithChangelogG(mp, compare)
	if err != nil {
		return nil, common.GenErrFnOutput(err)
	}
	if h.useCache {
		sizeOfVTs := commtypes.ValueTimestampGSize[uint64]{
			ValSizeFunc: commtypes.SizeOfUint64,
		}
		cacheStore := store.NewCachingKeyValueStoreG[ntypes.StartEndTime, commtypes.ValueTimestampG[uint64]](
			ctx, kvstore, ntypes.SizeOfStartEndTime, sizeOfVTs.SizeOfValueTimestamp, Q7SizePerStore)
		aggStore = cacheStore
	} else {
		aggStore = kvstore
	}
	return aggStore, nil
}

func (h *q7MaxBid) q7MaxBidByPrice(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	fn_out := h.setupSerde(serdeFormat)
	if fn_out != nil {
		return fn_out
	}
	srcs, sinks_arr, err := h.getSrcSink(ctx, sp)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	ectx := processor.NewExecutionContext(srcs, sinks_arr, h.funcName, sp.ScaleEpoch, sp.ParNum)
	aggStore, fn_out := h.setupAggStore(ctx, sp)
	if fn_out != nil {
		return fn_out
	}
	aggProc := processor.NewMeteredProcessorG(
		processor.NewStreamAggregateProcessorG[ntypes.StartEndTime, *ntypes.Event, uint64]("maxBid",
			aggStore, processor.InitializerFuncG[uint64](func() optional.Option[uint64] {
				return optional.Some(uint64(0))
			}),
			processor.AggregatorFuncG[ntypes.StartEndTime, *ntypes.Event, uint64](
				func(key ntypes.StartEndTime, v *ntypes.Event, agg optional.Option[uint64]) optional.Option[uint64] {
					aggVal := agg.Unwrap()
					if v.Bid.Price > aggVal {
						return optional.Some(v.Bid.Price)
					}
					return agg
				}), h.useCache))
	toStreamProc := processor.NewTableToStreamProcessorG[ntypes.StartEndTime, uint64]()
	mapProc := processor.NewStreamMapProcessorG[ntypes.StartEndTime, uint64, uint64, ntypes.StartEndTime]("remapKV",
		processor.MapperFuncG[ntypes.StartEndTime, uint64, uint64, ntypes.StartEndTime](
			func(key optional.Option[ntypes.StartEndTime], value optional.Option[uint64]) (
				optional.Option[uint64], optional.Option[ntypes.StartEndTime], error,
			) {
				return value, key, nil
			}))
	sinkProc := processor.NewGroupByOutputProcessorG("subG2Proc", sinks_arr[0], &ectx, h.outMsgSerde)
	aggProc.NextProcessor(toStreamProc)
	toStreamProc.NextProcessor(mapProc)
	mapProc.NextProcessor(sinkProc)
	task := stream_task.NewStreamTaskBuilder().
		AppProcessFunc(func(ctx context.Context, task *stream_task.StreamTask, args processor.ExecutionContext) (
			*common.FnOutput, optional.Option[commtypes.RawMsgAndSeq],
		) {
			return stream_task.CommonProcess(ctx, task, args.(*processor.BaseExecutionContext),
				func(ctx context.Context, msg commtypes.MessageG[ntypes.StartEndTime, *ntypes.Event], argsTmp interface{}) error {
					return aggProc.Process(ctx, msg)
				}, h.inMsgSerde)
		}).
		Build()
	kvc := map[string]store.KeyValueStoreOpWithChangelog{aggStore.ChangelogTopicName(): aggStore}
	transactionalID := fmt.Sprintf("%s-%s-%d-%s", h.funcName,
		sp.InputTopicNames[0], sp.ParNum, sp.OutputTopicNames[0])
	streamTaskArgs := benchutil.UpdateStreamTaskArgs(sp,
		stream_task.NewStreamTaskArgsBuilder(h.env, &ectx, transactionalID)).
		KVStoreChangelogs(kvc).Build()
	return stream_task.ExecuteApp(ctx, task, streamTaskArgs, func(ctx context.Context, env types.Environment,
		serdeFormat commtypes.SerdeFormat, rs *snapshot_store.RedisSnapshotStore,
	) error {
		payloadSerde, err := commtypes.GetPayloadArrSerdeG(serdeFormat)
		if err != nil {
			return err
		}
		stream_task.SetKVStoreWithChangelogSnapshot[ntypes.StartEndTime, commtypes.ValueTimestampG[uint64]](ctx, env, rs, aggStore, payloadSerde)
		return nil
	}, func() { sinkProc.OutputRemainingStats() })
}
