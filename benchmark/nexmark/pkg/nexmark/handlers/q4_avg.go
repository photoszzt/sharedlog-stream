package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/ntypes"
	"sharedlog-stream/pkg/commtypes"
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

type q4Avg struct {
	env types.Environment

	funcName      string
	inMsgSerde    commtypes.MessageGSerdeG[uint64, commtypes.ChangeG[uint64]]
	outMsgSerde   commtypes.MessageGSerdeG[uint64, float64]
	storeMsgSerde commtypes.MessageGSerdeG[uint64, commtypes.ValueTimestampG[ntypes.SumAndCount]]
}

func NewQ4Avg(env types.Environment, funcName string) *q4Avg {
	return &q4Avg{
		env:      env,
		funcName: funcName,
	}
}

func (h *q4Avg) getExecutionCtx(ctx context.Context, sp *common.QueryInput,
) (processor.BaseExecutionContext, error) {
	inputStream, outputStreams, err := benchutil.GetShardedInputOutputStreams(ctx, h.env, sp)
	if err != nil {
		return processor.BaseExecutionContext{}, err
	}
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	warmup := time.Duration(sp.WarmupS) * time.Second

	consumer, err := producer_consumer.NewShardedSharedLogStreamConsumer(inputStream,
		&producer_consumer.StreamConsumerConfig{
			Timeout:     common.SrcConsumeTimeout,
			SerdeFormat: serdeFormat,
		}, sp.NumSubstreamProducer[0], sp.ParNum)
	if err != nil {
		return processor.BaseExecutionContext{}, err
	}
	src := producer_consumer.NewMeteredConsumer(consumer, warmup)
	sink, err := producer_consumer.NewMeteredProducer(
		producer_consumer.NewShardedSharedLogStreamProducer(outputStreams[0],
			&producer_consumer.StreamSinkConfig{
				FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
				Format:        serdeFormat,
			}), warmup)
	if err != nil {
		return processor.BaseExecutionContext{}, err
	}
	sink.MarkFinalOutput()
	ectx := processor.NewExecutionContext([]*producer_consumer.MeteredConsumer{src},
		[]producer_consumer.MeteredProducerIntr{sink}, h.funcName, sp.ScaleEpoch, sp.ParNum)
	return ectx, nil
}

func (h *q4Avg) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &common.QueryInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output := h.Q4Avg(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	return common.CompressData(encodedOutput), nil
}

func (h *q4Avg) setupSerde(serdeFormat commtypes.SerdeFormat) *common.FnOutput {
	changeSerde, err := commtypes.GetChangeGSerdeG[uint64](serdeFormat, commtypes.Uint64SerdeG{})
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	h.inMsgSerde, err = commtypes.GetMsgGSerdeG[uint64](serdeFormat, commtypes.Uint64SerdeG{}, changeSerde)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	h.outMsgSerde, err = commtypes.GetMsgGSerdeG[uint64, float64](serdeFormat, commtypes.Uint64SerdeG{}, commtypes.Float64SerdeG{})
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	scSerde, err := ntypes.GetSumAndCountSerdeG(serdeFormat)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	h.storeMsgSerde, err = processor.MsgSerdeWithValueTsG[uint64](serdeFormat, commtypes.Uint64SerdeG{},
		scSerde)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	return nil
}

func (h *q4Avg) setupAggStore(sp *common.QueryInput) (
	*store_with_changelog.KeyValueStoreWithChangelogG[uint64, commtypes.ValueTimestampG[ntypes.SumAndCount]],
	*common.FnOutput,
) {
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	sumCountStoreName := "q4SumCountKVStore"
	mp, err := store_with_changelog.NewMaterializeParamBuilder[uint64, commtypes.ValueTimestampG[ntypes.SumAndCount]]().
		MessageSerde(h.storeMsgSerde).
		StoreName(sumCountStoreName).
		ParNum(sp.ParNum).
		SerdeFormat(serdeFormat).
		ChangelogManagerParam(commtypes.CreateChangelogManagerParam{
			Env:           h.env,
			NumPartition:  sp.NumChangelogPartition,
			FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
			TimeOut:       time.Duration(4) * time.Millisecond,
		}).BufMaxSize(sp.BufMaxSize).Build()
	if err != nil {
		return nil, common.GenErrFnOutput(err)
	}
	kvstore, err := store_with_changelog.CreateInMemorySkipmapKVTableWithChangelogG(mp, store.Uint64LessFunc)
	if err != nil {
		return nil, common.GenErrFnOutput(err)
	}
	return kvstore, nil
}

func (h *q4Avg) Q4Avg(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	fn_out := h.setupSerde(serdeFormat)
	if fn_out != nil {
		return fn_out
	}
	ectx, err := h.getExecutionCtx(ctx, sp)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	kvstore, fn_out := h.setupAggStore(sp)
	if fn_out != nil {
		return fn_out
	}
	tabAggProc := processor.NewMeteredProcessorG(
		processor.NewTableAggregateProcessorG[uint64, uint64, ntypes.SumAndCount]("sumCount", kvstore,
			processor.InitializerFuncG[ntypes.SumAndCount](func() optional.Option[ntypes.SumAndCount] {
				return optional.Some(ntypes.SumAndCount{
					Sum:   0,
					Count: 0,
				})
			}),
			processor.AggregatorFuncG[uint64, uint64, ntypes.SumAndCount](
				func(_ uint64, val uint64, agg optional.Option[ntypes.SumAndCount]) optional.Option[ntypes.SumAndCount] {
					aggVal := agg.Unwrap()
					return optional.Some(ntypes.SumAndCount{
						Sum:   aggVal.Sum + val,
						Count: aggVal.Count + 1,
					})
				}),
			processor.AggregatorFuncG[uint64, uint64, ntypes.SumAndCount](
				func(_ uint64, val uint64, agg optional.Option[ntypes.SumAndCount]) optional.Option[ntypes.SumAndCount] {
					aggVal := agg.Unwrap()
					return optional.Some(ntypes.SumAndCount{
						Sum:   aggVal.Sum - val,
						Count: aggVal.Count - 1,
					})
				}),
		))
	mapValProc := processor.NewTableMapValuesProcessorG[uint64, ntypes.SumAndCount, float64]("calcAvg",
		processor.ValueMapperWithKeyFuncG[uint64, ntypes.SumAndCount, float64](func(_ optional.Option[uint64], value optional.Option[ntypes.SumAndCount]) (optional.Option[float64], error) {
			sc := value.Unwrap()
			return optional.Some(float64(sc.Sum) / float64(sc.Count)), nil
		}))
	tabToStrProc := processor.NewTableToStreamProcessorG[uint64, float64]()
	outProc := processor.NewFixedSubstreamOutputProcessorG("subG4Proc", ectx.Producers()[0], sp.ParNum, h.outMsgSerde)
	tabAggProc.NextProcessor(mapValProc)
	mapValProc.NextProcessor(tabToStrProc)
	tabToStrProc.NextProcessor(outProc)
	task := stream_task.NewStreamTaskBuilder().MarkFinalStage().
		AppProcessFunc(func(ctx context.Context, task *stream_task.StreamTask, args processor.ExecutionContext) (
			*common.FnOutput, optional.Option[commtypes.RawMsgAndSeq],
		) {
			return stream_task.CommonProcess(ctx, task, args.(*processor.BaseExecutionContext),
				func(ctx context.Context, msg commtypes.MessageG[uint64, commtypes.ChangeG[uint64]], argsTmp interface{}) error {
					return tabAggProc.Process(ctx, msg)
				}, h.inMsgSerde)
		}).
		Build()
	kvc := map[string]store.KeyValueStoreOpWithChangelog{kvstore.ChangelogTopicName(): kvstore}
	transactionalID := fmt.Sprintf("%s-%s-%d-%s", h.funcName, sp.InputTopicNames[0],
		sp.ParNum, sp.OutputTopicNames[0])
	streamTaskArgs, err := benchutil.UpdateStreamTaskArgs(sp,
		stream_task.NewStreamTaskArgsBuilder(h.env, &ectx, transactionalID)).
		KVStoreChangelogs(kvc).
		FixedOutParNum(sp.ParNum).
		Build()
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	return stream_task.ExecuteApp(ctx, task, streamTaskArgs,
		func(ctx context.Context, env types.Environment, serdeFormat commtypes.SerdeFormat,
			rs *snapshot_store.RedisSnapshotStore,
		) error {
			payloadSerde, err := commtypes.GetPayloadArrSerdeG(serdeFormat)
			if err != nil {
				return err
			}
			stream_task.SetKVStoreWithChangelogSnapshot[uint64, commtypes.ValueTimestampG[ntypes.SumAndCount]](ctx, env,
				rs, kvstore, payloadSerde)
			return nil
		}, func() { outProc.OutputRemainingStats() })
}
