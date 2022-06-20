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

/*
type q7MaxBidByPriceProcessArgs struct {
	maxBid  *processor.MeteredProcessor
	remapKV *processor.MeteredProcessor
	groupBy *processor.GroupBy
	processor.BaseExecutionContext
}

func (h *q7MaxBid) procMsg(ctx context.Context, msg commtypes.Message, argsTmp interface{}) error {
	args := argsTmp.(*q7MaxBidByPriceProcessArgs)
	maxBids, err := args.maxBid.ProcessAndReturn(ctx, msg)
	if err != nil {
		return err
	}
	remaped, err := args.remapKV.ProcessAndReturn(ctx, maxBids[0])
	if err != nil {
		return err
	}
	err = args.groupBy.GroupByAndProduce(ctx, remaped[0], args.TrackParFunc())
	return err
}
*/

func (h *q7MaxBid) getSrcSink(ctx context.Context, sp *common.QueryInput,
) ([]producer_consumer.MeteredConsumerIntr, []producer_consumer.MeteredProducerIntr, error) {
	input_stream, output_streams, err := benchutil.GetShardedInputOutputStreams(ctx, h.env, sp)
	if err != nil {
		return nil, nil, err
	}
	debug.Assert(len(output_streams) == 1, "expected only one output stream")

	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	msgSerde, err := commtypes.GetMsgSerde(serdeFormat)
	if err != nil {
		return nil, nil, err
	}
	eventSerde, err := ntypes.GetEventSerde(serdeFormat)
	if err != nil {
		return nil, nil, err
	}
	seSerde, err := ntypes.GetStartEndTimeSerde(serdeFormat)
	if err != nil {
		return nil, nil, err
	}
	inKVMsgSerdes := commtypes.KVMsgSerdes{
		MsgSerde: msgSerde,
		ValSerde: eventSerde,
		KeySerde: seSerde,
	}
	inConfig := &producer_consumer.StreamConsumerConfig{
		KVMsgSerdes: inKVMsgSerdes,
		Timeout:     common.SrcConsumeTimeout,
	}
	outConfig := &producer_consumer.StreamSinkConfig{
		KVMsgSerdes: commtypes.KVMsgSerdes{
			MsgSerde: msgSerde,
			KeySerde: commtypes.Uint64Serde{},
			ValSerde: seSerde,
		},
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
	warmup := time.Duration(sp.WarmupS) * time.Second
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)

	seSerde, err := ntypes.GetStartEndTimeSerde(serdeFormat)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	msgSerde, err := commtypes.GetMsgSerde(serdeFormat)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	vtSerde, err := commtypes.GetValueTsSerde(serdeFormat, commtypes.Uint64Serde{}, commtypes.Uint64Serde{})
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}

	maxBidByWinStoreName := "maxBidByWinKVStore"
	mp, err := store_with_changelog.NewMaterializeParamBuilder().
		KVMsgSerdes(commtypes.KVMsgSerdes{
			KeySerde: seSerde,
			ValSerde: vtSerde,
			MsgSerde: msgSerde,
		}).
		StoreName(maxBidByWinStoreName).
		ParNum(sp.ParNum).
		SerdeFormat(serdeFormat).
		StreamParam(commtypes.CreateStreamParam{
			Env:          h.env,
			NumPartition: sp.NumInPartition,
		}).Build(time.Duration(sp.FlushMs)*time.Millisecond, common.SrcConsumeTimeout)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	compare := func(a, b treemap.Key) int {
		ka := a.(*ntypes.StartEndTime)
		kb := b.(*ntypes.StartEndTime)
		return ntypes.CompareStartEndTime(ka, kb)
	}
	kvstore := store_with_changelog.CreateInMemKVTableWithChangelog(mp, compare, warmup)

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
		})), warmup)).Via(
		processor.NewMeteredProcessor(processor.NewStreamMapProcessor(
			"remapKV", processor.MapperFunc(func(m commtypes.Message) (commtypes.Message, error) {
				return commtypes.Message{Key: m.Value, Value: m.Key, Timestamp: m.Timestamp}, nil
			})), warmup)).
		Via(processor.NewGroupByOutputProcessor(sinks_arr[0], &ectx))
	task := stream_task.NewStreamTaskBuilder().
		AppProcessFunc(func(ctx context.Context, task *stream_task.StreamTask, argsTmp interface{}) *common.FnOutput {
			args := argsTmp.(processor.ExecutionContext)
			return execution.CommonProcess(ctx, task, args, processor.ProcessMsg)
		}).Build()

	var kvc []*store_restore.KVStoreChangelog
	kvc = []*store_restore.KVStoreChangelog{
		store_restore.NewKVStoreChangelog(
			kvstore, kvstore.MaterializeParam().ChangelogManager(),
			kvstore.MaterializeParam().ParNum(),
		),
	}
	update_stats := func(ret *common.FnOutput) {}
	transactionalID := fmt.Sprintf("%s-%s-%d-%s", h.funcName,
		sp.InputTopicNames[0], sp.ParNum, sp.OutputTopicNames[0])
	streamTaskArgs := benchutil.UpdateStreamTaskArgs(sp,
		stream_task.NewStreamTaskArgsBuilder(h.env, &ectx, transactionalID)).
		KVStoreChangelogs(kvc).Build()
	return task.ExecuteApp(ctx, streamTaskArgs, update_stats)
}
