package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/execution"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/producer_consumer"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/store_restore"
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
		return processor.EmptyBaseExecutionContext, err
	}
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	changeSerde, err := commtypes.GetChangeSerde(serdeFormat, commtypes.Int64Serde{})
	if err != nil {
		return processor.EmptyBaseExecutionContext, err
	}
	inMsgSerde, err := commtypes.GetMsgSerde(serdeFormat, commtypes.Uint64Serde{}, changeSerde)
	if err != nil {
		return processor.EmptyBaseExecutionContext, fmt.Errorf("get msg serde err: %v", err)
	}
	outMsgSerde, err := commtypes.GetMsgSerde(serdeFormat, commtypes.Uint64Serde{}, commtypes.Float64Serde{})
	if err != nil {
		return processor.EmptyBaseExecutionContext, fmt.Errorf("get msg serde err: %v", err)
	}
	warmup := time.Duration(sp.WarmupS) * time.Second

	src := producer_consumer.NewMeteredConsumer(
		producer_consumer.NewShardedSharedLogStreamConsumer(inputStream, &producer_consumer.StreamConsumerConfig{
			Timeout:  common.SrcConsumeTimeout,
			MsgSerde: inMsgSerde,
		}), warmup)
	sink := producer_consumer.NewMeteredProducer(
		producer_consumer.NewShardedSharedLogStreamProducer(outputStreams[0], &producer_consumer.StreamSinkConfig{
			MsgSerde:      outMsgSerde,
			FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
		}), warmup)
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
	return utils.CompressData(encodedOutput), nil
}

/*
type Q4AvgProcessArgs struct {
	sumCount *processor.MeteredProcessor
	calcAvg  *processor.MeteredProcessor
	processor.BaseExecutionContext
}

func (h *q4Avg) procMsg(ctx context.Context, msg commtypes.Message, argsTmp interface{}) error {
	args := argsTmp.(*Q4AvgProcessArgs)
	sumCounts, err := args.sumCount.ProcessAndReturn(ctx, msg)
	if err != nil {
		return err
	}
	avg, err := args.calcAvg.ProcessAndReturn(ctx, sumCounts[0])
	if err != nil {
		return err
	}
	err = args.Producers()[0].Produce(ctx, avg[0], args.SubstreamNum(), false)
	if err != nil {
		return err
	}
	return nil
}
*/

func (h *q4Avg) Q4Avg(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	ectx, err := h.getExecutionCtx(ctx, sp)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}

	sumCountStoreName := "q4SumCountKVStore"
	warmup := time.Duration(sp.WarmupS) * time.Second
	mp, err := store_with_changelog.NewMaterializeParamBuilder().
		MessageSerde(ectx.Consumers()[0].MsgSerde()).
		StoreName(sumCountStoreName).
		ParNum(sp.ParNum).
		SerdeFormat(commtypes.SerdeFormat(sp.SerdeFormat)).
		StreamParam(commtypes.CreateStreamParam{
			Env:          h.env,
			NumPartition: sp.NumInPartition,
		}).BuildForKVStore(time.Duration(sp.FlushMs)*time.Millisecond, common.SrcConsumeTimeout)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	kvstore := store_with_changelog.CreateInMemKVTableWithChangelog(mp, store.Uint64KeyKVStoreCompare, warmup)
	ectx.Via(processor.NewMeteredProcessor(processor.NewTableAggregateProcessor("sumCount", kvstore,
		processor.InitializerFunc(func() interface{} {
			return &ntypes.SumAndCount{
				Sum:   0,
				Count: 0,
			}
		}),
		processor.AggregatorFunc(func(key, value, aggregate interface{}) interface{} {
			val := value.(uint64)
			agg := aggregate.(*ntypes.SumAndCount)
			return &ntypes.SumAndCount{
				Sum:   agg.Sum + val,
				Count: agg.Count + 1,
			}
		}),
		processor.AggregatorFunc(func(key, value, aggregate interface{}) interface{} {
			val := value.(uint64)
			agg := aggregate.(*ntypes.SumAndCount)
			return &ntypes.SumAndCount{
				Sum:   agg.Sum - val,
				Count: agg.Count - 1,
			}
		}),
	))).
		Via(
			processor.NewMeteredProcessor(processor.NewStreamMapValuesProcessor(
				processor.ValueMapperFunc(func(value interface{}) (interface{}, error) {
					val := value.(commtypes.Change)
					sc := val.NewVal.(*ntypes.SumAndCount)
					return float64(sc.Sum) / float64(sc.Count), nil
				}),
			))).
		Via(processor.NewFixedSubstreamOutputProcessor(ectx.Producers()[0], sp.ParNum))
	task := stream_task.NewStreamTaskBuilder().
		AppProcessFunc(func(ctx context.Context, task *stream_task.StreamTask, argsTmp interface{}) *common.FnOutput {
			args := argsTmp.(processor.ExecutionContext)
			return execution.CommonProcess(ctx, task, args, processor.ProcessMsg)
		}).Build()

	kvc := []*store_restore.KVStoreChangelog{
		store_restore.NewKVStoreChangelog(kvstore, mp.ChangelogManager(), sp.ParNum),
	}
	transactionalID := fmt.Sprintf("%s-%s-%d-%s", h.funcName, sp.InputTopicNames[0],
		sp.ParNum, sp.OutputTopicNames[0])
	streamTaskArgs := benchutil.UpdateStreamTaskArgs(sp,
		stream_task.NewStreamTaskArgsBuilder(h.env, &ectx, transactionalID)).
		KVStoreChangelogs(kvc).
		FixedOutParNum(sp.ParNum).
		Build()
	return task.ExecuteApp(ctx, streamTaskArgs)
}
