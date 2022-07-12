package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/execution"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/producer_consumer"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/store_restore"
	"sharedlog-stream/pkg/store_with_changelog"
	"sharedlog-stream/pkg/stream_task"
	"sort"
	"time"

	"cs.utexas.edu/zjia/faas/types"
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
		return processor.EmptyBaseExecutionContext, err
	}
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	ptSerde, err := ntypes.GetPriceTimeSerde(serdeFormat)
	if err != nil {
		return processor.EmptyBaseExecutionContext, err
	}
	changeSerde, err := commtypes.GetChangeSerde(serdeFormat, ptSerde)
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
	sink.MarkFinalOutput()
	ectx := processor.NewExecutionContext([]producer_consumer.MeteredConsumerIntr{src},
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
	return utils.CompressData(encodedOutput), nil
}

func (h *q6Avg) Q6Avg(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	ectx, err := h.getExecutionCtx(ctx, sp)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	aggStoreName := "q6AggKVStore"
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	ptlSerde, err := ntypes.GetPriceTimeListSerde(serdeFormat)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	storeMsgSerde, err := processor.MsgSerdeWithValueTs(serdeFormat, commtypes.Uint64Serde{}, ptlSerde)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	mp, err := store_with_changelog.NewMaterializeParamBuilder().
		MessageSerde(storeMsgSerde).
		StoreName(aggStoreName).
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
	kvstore, err := store_with_changelog.CreateInMemKVTableWithChangelog(mp, store.Uint64KeyKVStoreCompare)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	maxSize := 10
	ectx.
		Via(processor.NewMeteredProcessor(
			processor.NewTableAggregateProcessor("q6Agg", kvstore,
				processor.InitializerFunc(func() interface{} {
					return ntypes.PriceTimeList{
						PTList: make([]ntypes.PriceTime, 0),
					}
				}),
				processor.AggregatorFunc(func(key, value, aggregate interface{}) interface{} {
					agg := aggregate.(ntypes.PriceTimeList)
					agg.PTList = append(agg.PTList, value.(ntypes.PriceTime))
					sort.Sort(agg.PTList)
					// debug.Fprintf(os.Stderr, "[ADD] agg before delete: %v\n", agg.PTList)
					if len(agg.PTList) > maxSize {
						agg.PTList = ntypes.Delete(agg.PTList, 0, 1)
					}
					// debug.Fprintf(os.Stderr, "[ADD] agg after delete: %v\n", agg.PTList)
					return agg
				}),
				processor.AggregatorFunc(func(key, value, aggregate interface{}) interface{} {
					agg := aggregate.(ntypes.PriceTimeList)
					if len(agg.PTList) > 0 {
						// debug.Fprintf(os.Stderr, "[RM] element to delete: %+v\n", value)
						val := ntypes.CastToPriceTimePtr(value)
						agg.PTList = ntypes.RemoveMatching(agg.PTList, val)
						// debug.Fprintf(os.Stderr, "[RM] after delete agg: %v\n", agg)
					}
					return agg
				}),
			))).
		Via(processor.NewMeteredProcessor(processor.NewTableMapValuesProcessor("avg",
			processor.ValueMapperWithKeyFunc(func(key, value interface{}) (interface{}, error) {
				pt := value.(ntypes.PriceTimeList)
				sum := uint64(0)
				l := len(pt.PTList)
				for _, p := range pt.PTList {
					sum += p.Price
				}
				return float64(sum) / float64(l), nil
			})))).
		Via(processor.NewTableToStreamProcessor()).
		Via(processor.NewFixedSubstreamOutputProcessor(ectx.Producers()[0], sp.ParNum))
	task := stream_task.NewStreamTaskBuilder().
		AppProcessFunc(func(ctx context.Context, task *stream_task.StreamTask, argsTmp interface{}) *common.FnOutput {
			args := argsTmp.(processor.ExecutionContext)
			return execution.CommonProcess(ctx, task, args, processor.ProcessMsg)
		}).Build()

	kvc := []*store_restore.KVStoreChangelog{
		store_restore.NewKVStoreChangelog(kvstore, kvstore.ChangelogManager()),
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
