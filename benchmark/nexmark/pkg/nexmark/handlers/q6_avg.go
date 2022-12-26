package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/ntypes"
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
	"golang.org/x/exp/slices"
)

type q6Avg struct {
	env      types.Environment
	funcName string
}

func NewQ6Avg(env types.Environment, funcName string) *q6Avg {
	return &q6Avg{
		env:      env,
		funcName: funcName,
	}
}

func (h *q6Avg) getExecutionCtx(ctx context.Context, sp *common.QueryInput) (processor.BaseExecutionContext, error) {
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
		producer_consumer.NewShardedSharedLogStreamProducer(outputStreams[0], &producer_consumer.StreamSinkConfig{
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

func (h *q6Avg) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &common.QueryInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output := h.Q6Avg(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	return common.CompressData(encodedOutput), nil
}

func (h *q6Avg) Q6Avg(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	ptSerde, err := ntypes.GetPriceTimeSerdeG(serdeFormat)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	changeSerde, err := commtypes.GetChangeGSerdeG(serdeFormat, ptSerde)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	inMsgSerde, err := commtypes.GetMsgGSerdeG[uint64](serdeFormat, commtypes.Uint64SerdeG{}, changeSerde)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	outMsgSerde, err := commtypes.GetMsgGSerdeG[uint64, float64](serdeFormat, commtypes.Uint64SerdeG{}, commtypes.Float64SerdeG{})
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	ectx, err := h.getExecutionCtx(ctx, sp)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	aggStoreName := "q6AggKVStore"
	ptlSerde, err := ntypes.GetPriceTimeListSerdeG(serdeFormat)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	storeMsgSerde, err := processor.MsgSerdeWithValueTsG[uint64](serdeFormat, commtypes.Uint64SerdeG{}, ptlSerde)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	mp, err := store_with_changelog.NewMaterializeParamBuilder[uint64, commtypes.ValueTimestampG[ntypes.PriceTimeList]]().
		MessageSerde(storeMsgSerde).
		StoreName(aggStoreName).
		ParNum(sp.ParNum).
		SerdeFormat(serdeFormat).
		ChangelogManagerParam(commtypes.CreateChangelogManagerParam{
			Env:           h.env,
			NumPartition:  sp.NumChangelogPartition,
			FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
			TimeOut:       common.SrcConsumeTimeout,
		}).Build()
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	kvstore, err := store_with_changelog.CreateInMemorySkipmapKVTableWithChangelogG(mp, store.Uint64LessFunc)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	maxSize := 10
	tabAggProc := processor.NewMeteredProcessorG(
		processor.NewTableAggregateProcessorG[uint64, ntypes.PriceTime, ntypes.PriceTimeList]("q6Agg", kvstore,
			processor.InitializerFuncG[ntypes.PriceTimeList](func() optional.Option[ntypes.PriceTimeList] {
				return optional.Some(ntypes.PriceTimeList{
					PTList: make([]ntypes.PriceTime, 0),
				})
			}),
			processor.AggregatorFuncG[uint64, ntypes.PriceTime, ntypes.PriceTimeList](
				func(key uint64, value ntypes.PriceTime, aggregate optional.Option[ntypes.PriceTimeList]) optional.Option[ntypes.PriceTimeList] {
					aggVal := aggregate.Unwrap()
					aggVal.PTList = append(aggVal.PTList, value)
					slices.SortFunc(aggVal.PTList, store.LessFunc[ntypes.PriceTime](func(a, b ntypes.PriceTime) bool {
						return ntypes.ComparePriceTime(a, b) < 0
					}))
					if len(aggVal.PTList) > maxSize {
						aggVal.PTList = ntypes.Delete(aggVal.PTList, 0, 1)
					}
					return optional.Some(aggVal)
				}),
			processor.AggregatorFuncG[uint64, ntypes.PriceTime, ntypes.PriceTimeList](
				func(key uint64, value ntypes.PriceTime, agg optional.Option[ntypes.PriceTimeList]) optional.Option[ntypes.PriceTimeList] {
					aggVal := agg.Unwrap()
					if len(aggVal.PTList) > 0 {
						// debug.Fprintf(os.Stderr, "[RM] element to delete: %+v\n", value)
						val := ntypes.CastToPriceTimePtr(value)
						aggVal.PTList = ntypes.RemoveMatching(aggVal.PTList, val)
						// debug.Fprintf(os.Stderr, "[RM] after delete agg: %v\n", agg)
					}
					return optional.Some(aggVal)
				}),
		))
	tabMapValProc := processor.NewTableMapValuesProcessorG[uint64, ntypes.PriceTimeList, float64]("avg",
		processor.ValueMapperWithKeyFuncG[uint64, ntypes.PriceTimeList, float64](
			func(key optional.Option[uint64], value optional.Option[ntypes.PriceTimeList]) (float64, error) {
				pt := value.Unwrap()
				sum := uint64(0)
				l := len(pt.PTList)
				for _, p := range pt.PTList {
					sum += p.Price
				}
				return float64(sum) / float64(l), nil
			}))
	tabToStreamProc := processor.NewTableToStreamProcessorG[uint64, float64]()
	sinkProc := processor.NewFixedSubstreamOutputProcessorG("subG4Proc", ectx.Producers()[0], sp.ParNum, outMsgSerde)
	tabAggProc.NextProcessor(tabMapValProc)
	tabMapValProc.NextProcessor(tabToStreamProc)
	tabToStreamProc.NextProcessor(sinkProc)
	task := stream_task.NewStreamTaskBuilder().MarkFinalStage().
		AppProcessFunc(func(ctx context.Context, task *stream_task.StreamTask, args processor.ExecutionContext) (
			*common.FnOutput, optional.Option[commtypes.RawMsgAndSeq],
		) {
			return stream_task.CommonProcess(ctx, task, args.(*processor.BaseExecutionContext),
				func(ctx context.Context, msg commtypes.MessageG[uint64, commtypes.ChangeG[ntypes.PriceTime]], argsTmp interface{}) error {
					return tabAggProc.Process(ctx, msg)
				}, inMsgSerde)
		}).
		Build()

	kvc := map[string]store.KeyValueStoreOpWithChangelog{kvstore.ChangelogTopicName(): kvstore}
	transactionalID := fmt.Sprintf("%s-%s-%d-%s", h.funcName, sp.InputTopicNames[0],
		sp.ParNum, sp.OutputTopicNames[0])
	streamTaskArgs := benchutil.UpdateStreamTaskArgs(sp,
		stream_task.NewStreamTaskArgsBuilder(h.env, &ectx, transactionalID)).
		KVStoreChangelogs(kvc).
		FixedOutParNum(sp.ParNum).
		Build()
	return stream_task.ExecuteApp(ctx, task, streamTaskArgs, func(ctx context.Context,
		env types.Environment, serdeFormat commtypes.SerdeFormat, rs *snapshot_store.RedisSnapshotStore,
	) error {
		payloadSerde, err := commtypes.GetPayloadArrSerdeG(serdeFormat)
		if err != nil {
			return err
		}
		stream_task.SetKVStoreSnapshot[uint64, commtypes.ValueTimestampG[ntypes.PriceTimeList]](ctx, env, rs,
			kvstore, payloadSerde)
		return nil
	}, func() { sinkProc.OutputRemainingStats() })
}
