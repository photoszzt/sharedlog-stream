package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/ntypes"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/producer_consumer"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/store_with_changelog"
	"sharedlog-stream/pkg/stream_task"
	"time"

	"cs.utexas.edu/zjia/faas/types"
)

type q4Avg struct {
	env types.Environment

	funcName string
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
	changeSerde, err := commtypes.GetChangeSerdeG(serdeFormat, commtypes.Uint64Serde{})
	if err != nil {
		return processor.BaseExecutionContext{}, err
	}
	inMsgSerde, err := commtypes.GetMsgSerdeG[uint64](serdeFormat, commtypes.Uint64SerdeG{}, changeSerde)
	if err != nil {
		return processor.BaseExecutionContext{}, fmt.Errorf("get msg serde err: %v", err)
	}
	outMsgSerde, err := commtypes.GetMsgSerdeG[uint64, float64](serdeFormat, commtypes.Uint64SerdeG{}, commtypes.Float64SerdeG{})
	if err != nil {
		return processor.BaseExecutionContext{}, fmt.Errorf("get msg serde err: %v", err)
	}
	warmup := time.Duration(sp.WarmupS) * time.Second

	consumer, err := producer_consumer.NewShardedSharedLogStreamConsumerG(inputStream, &producer_consumer.StreamConsumerConfigG[uint64, commtypes.Change]{
		Timeout:     common.SrcConsumeTimeout,
		MsgSerde:    inMsgSerde,
		SerdeFormat: serdeFormat,
	}, sp.NumSubstreamProducer[0], sp.ParNum)
	if err != nil {
		return processor.BaseExecutionContext{}, err
	}
	src := producer_consumer.NewMeteredConsumer(consumer, warmup)
	sink := producer_consumer.NewMeteredProducer(
		producer_consumer.NewShardedSharedLogStreamProducer(outputStreams[0], &producer_consumer.StreamSinkConfig[uint64, float64]{
			MsgSerde:      outMsgSerde,
			FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
		}), warmup)
	sink.MarkFinalOutput()
	ectx := processor.NewExecutionContext([]producer_consumer.MeteredConsumerIntr{src},
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

func (h *q4Avg) Q4Avg(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	ectx, err := h.getExecutionCtx(ctx, sp)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	sumCountStoreName := "q4SumCountKVStore"
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	scSerde, err := ntypes.GetSumAndCountSerde(serdeFormat)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	storeMsgSerde, err := processor.MsgSerdeWithValueTsG[uint64](serdeFormat, commtypes.Uint64SerdeG{},
		scSerde)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	mp, err := store_with_changelog.NewMaterializeParamBuilder[uint64, commtypes.ValueTimestamp]().
		MessageSerde(storeMsgSerde).
		StoreName(sumCountStoreName).
		ParNum(sp.ParNum).
		SerdeFormat(serdeFormat).
		ChangelogManagerParam(commtypes.CreateChangelogManagerParam{
			Env:           h.env,
			NumPartition:  sp.NumInPartition,
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
	ectx.
		Via(processor.NewMeteredProcessor(
			processor.NewTableAggregateProcessorG[uint64, uint64, *ntypes.SumAndCount]("sumCount", kvstore,
				processor.InitializerFuncG[*ntypes.SumAndCount](func() *ntypes.SumAndCount {
					return &ntypes.SumAndCount{
						Sum:   0,
						Count: 0,
					}
				}),
				processor.AggregatorFuncG[uint64, uint64, *ntypes.SumAndCount](func(_ uint64, val uint64, agg *ntypes.SumAndCount) *ntypes.SumAndCount {
					return &ntypes.SumAndCount{
						Sum:   agg.Sum + val,
						Count: agg.Count + 1,
					}
				}),
				processor.AggregatorFuncG[uint64, uint64, *ntypes.SumAndCount](func(_ uint64, val uint64, agg *ntypes.SumAndCount) *ntypes.SumAndCount {
					return &ntypes.SumAndCount{
						Sum:   agg.Sum - val,
						Count: agg.Count - 1,
					}
				}),
			))).
		Via(processor.NewMeteredProcessor(processor.NewTableMapValuesProcessor("calcAvg",
			processor.ValueMapperWithKeyFunc(func(_, value interface{}) (interface{}, error) {
				sc := value.(*ntypes.SumAndCount)
				return float64(sc.Sum) / float64(sc.Count), nil
			}),
		))).
		Via(processor.NewTableToStreamProcessor()).
		Via(processor.NewFixedSubstreamOutputProcessor(ectx.Producers()[0], sp.ParNum))
	task := stream_task.NewStreamTaskBuilder().MarkFinalStage().Build()
	kvc := map[string]store.KeyValueStoreOpWithChangelog{kvstore.ChangelogTopicName(): kvstore}
	transactionalID := fmt.Sprintf("%s-%s-%d-%s", h.funcName, sp.InputTopicNames[0],
		sp.ParNum, sp.OutputTopicNames[0])
	streamTaskArgs := benchutil.UpdateStreamTaskArgs(sp,
		stream_task.NewStreamTaskArgsBuilder(h.env, &ectx, transactionalID)).
		KVStoreChangelogs(kvc).
		FixedOutParNum(sp.ParNum).
		Build()
	return stream_task.ExecuteApp(ctx, task, streamTaskArgs)
}
