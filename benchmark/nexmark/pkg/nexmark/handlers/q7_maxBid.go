package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"
	nutils "sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/execution"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/producer_consumer"
	"sharedlog-stream/pkg/store_restore"
	"sharedlog-stream/pkg/store_with_changelog"
	"sharedlog-stream/pkg/stream_task"
	"sharedlog-stream/pkg/treemap"
	"time"

	"cs.utexas.edu/zjia/faas/types"
)

type q7MaxBid struct {
	env      types.Environment
	funcName string
}

func NewQ7MaxBid(env types.Environment, funcName string) types.FuncHandler {
	return &q7MaxBid{
		env:      env,
		funcName: funcName,
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
	return nutils.CompressData(encodedOutput), nil
}

func (h *q7MaxBid) getSrcSink(ctx context.Context, sp *common.QueryInput,
) ([]producer_consumer.MeteredConsumerIntr, []producer_consumer.MeteredProducerIntr, error) {
	input_stream, output_streams, err := benchutil.GetShardedInputOutputStreams(ctx, h.env, sp)
	if err != nil {
		return nil, nil, err
	}
	debug.Assert(len(output_streams) == 1, "expected only one output stream")

	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	eventSerde, err := ntypes.GetEventSerde(serdeFormat)
	if err != nil {
		return nil, nil, err
	}
	seSerde, err := ntypes.GetStartEndTimeSerde(serdeFormat)
	if err != nil {
		return nil, nil, err
	}
	inMsgSerde, err := commtypes.GetMsgSerde(serdeFormat, seSerde, eventSerde)
	if err != nil {
		return nil, nil, err
	}
	outMsgSerde, err := commtypes.GetMsgSerde(serdeFormat, commtypes.Uint64Serde{}, seSerde)
	if err != nil {
		return nil, nil, err
	}
	inConfig := &producer_consumer.StreamConsumerConfig{
		MsgSerde: inMsgSerde,
		Timeout:  common.SrcConsumeTimeout,
	}
	outConfig := &producer_consumer.StreamSinkConfig{
		MsgSerde:      outMsgSerde,
		FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
	}
	warmup := time.Duration(sp.WarmupS) * time.Second
	src := producer_consumer.NewMeteredConsumer(
		producer_consumer.NewShardedSharedLogStreamConsumer(input_stream, inConfig),
		warmup)
	sink := producer_consumer.NewMeteredProducer(
		producer_consumer.NewShardedSharedLogStreamProducer(output_streams[0], outConfig),
		warmup)
	src.SetInitialSource(false)
	return []producer_consumer.MeteredConsumerIntr{src}, []producer_consumer.MeteredProducerIntr{sink}, nil
}

func (h *q7MaxBid) q7MaxBidByPrice(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	srcs, sinks_arr, err := h.getSrcSink(ctx, sp)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	ectx := processor.NewExecutionContext(srcs, sinks_arr, h.funcName, sp.ScaleEpoch, sp.ParNum)
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)

	maxBidByWinStoreName := "maxBidByWinKVStore"
	srcMsgSerde := srcs[0].MsgSerde()
	msgSerde, err := processor.MsgSerdeWithValueTs(serdeFormat, srcMsgSerde.GetKeySerde(), commtypes.Uint64Serde{})
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	mp, err := store_with_changelog.NewMaterializeParamBuilder().
		MessageSerde(msgSerde).
		StoreName(maxBidByWinStoreName).
		ParNum(sp.ParNum).
		SerdeFormat(serdeFormat).
		ChangelogManagerParam(commtypes.CreateChangelogManagerParam{
			Env:           h.env,
			NumPartition:  sp.NumInPartition,
			TimeOut:       common.SrcConsumeTimeout,
			FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
		}).Build()
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	compare := func(a, b treemap.Key) int {
		ka := a.(*ntypes.StartEndTime)
		kb := b.(*ntypes.StartEndTime)
		return ntypes.CompareStartEndTime(ka, kb)
	}
	kvstore, err := store_with_changelog.CreateInMemKVTableWithChangelog(mp, compare)
	if err != nil {
		return common.GenErrFnOutput(err)
	}

	ectx.Via(processor.NewMeteredProcessor(processor.NewStreamAggregateProcessor("maxBid",
		kvstore, processor.InitializerFunc(func() interface{} {
			return uint64(0)
		}),
		processor.AggregatorFunc(func(key, value, aggregate interface{}) interface{} {
			v := value.(*ntypes.Event)
			agg := aggregate.(uint64)
			if v.Bid.Price > agg {
				return v.Bid.Price
			}
			return agg
		})))).
		Via(processor.NewTableToStreamProcessor()).
		Via(processor.NewMeteredProcessor(processor.NewStreamMapProcessor("remapKV",
			processor.MapperFunc(func(key, value interface{}) (interface{}, interface{}, error) {
				return value, key, nil
			})))).
		Via(processor.NewGroupByOutputProcessor(sinks_arr[0], &ectx))
	task := stream_task.NewStreamTaskBuilder().
		AppProcessFunc(func(ctx context.Context, task *stream_task.StreamTask, argsTmp interface{}) *common.FnOutput {
			args := argsTmp.(processor.ExecutionContext)
			return execution.CommonProcess(ctx, task, args, processor.ProcessMsg)
		}).Build()
	kvc := []*store_restore.KVStoreChangelog{
		store_restore.NewKVStoreChangelog(
			kvstore, kvstore.ChangelogManager(),
		),
	}
	transactionalID := fmt.Sprintf("%s-%s-%d-%s", h.funcName,
		sp.InputTopicNames[0], sp.ParNum, sp.OutputTopicNames[0])
	streamTaskArgs := benchutil.UpdateStreamTaskArgs(sp,
		stream_task.NewStreamTaskArgsBuilder(h.env, &ectx, transactionalID)).
		KVStoreChangelogs(kvc).Build()
	return task.ExecuteApp(ctx, streamTaskArgs)
}
