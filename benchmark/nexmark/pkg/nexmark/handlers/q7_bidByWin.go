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
	"sharedlog-stream/pkg/proc_interface"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/producer_consumer"
	"sharedlog-stream/pkg/stream_task"
	"sharedlog-stream/pkg/utils"
	"time"

	"cs.utexas.edu/zjia/faas/types"
)

type q7BidByWin struct {
	env      types.Environment
	funcName string
}

func NewQ7BidByWin(env types.Environment, funcName string) types.FuncHandler {
	return &q7BidByWin{
		env:      env,
		funcName: funcName,
	}
}

func (h *q7BidByWin) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &common.QueryInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output := h.q7BidByWin(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	return nutils.CompressData(encodedOutput), nil
}

type q7BidByWinProcessArgs struct {
	bid      *processor.MeteredProcessor
	bidByWin *processor.MeteredProcessor
	groupBy  *processor.GroupBy
	proc_interface.BaseExecutionContext
}

func (h *q7BidByWin) procMsg(ctx context.Context, msg commtypes.Message, argsTmp interface{}) error {
	args := argsTmp.(*q7BidByWinProcessArgs)
	bidMsg, err := args.bid.ProcessAndReturn(ctx, msg)
	if err != nil {
		return fmt.Errorf("filter bid err: %v", err)
	}
	if bidMsg != nil {
		mappedKey, err := args.bidByWin.ProcessAndReturn(ctx, bidMsg[0])
		if err != nil {
			return fmt.Errorf("bid keyed by price error: %v", err)
		}
		err = args.groupBy.GroupByAndProduce(ctx, mappedKey[0], args.TrackParFunc())
		if err != nil {
			return err
		}
	}
	return nil
}

func (h *q7BidByWin) getSrcSink(ctx context.Context, sp *common.QueryInput,
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
	msgSerde, err := commtypes.GetMsgSerde(serdeFormat)
	if err != nil {
		return nil, nil, err
	}
	seSerde, err := ntypes.GetStartEndTimeSerde(serdeFormat)
	if err != nil {
		return nil, nil, err
	}
	inConfig := &producer_consumer.StreamConsumerConfig{
		Timeout: common.SrcConsumeTimeout,
		KVMsgSerdes: commtypes.KVMsgSerdes{
			KeySerde: commtypes.StringSerde{},
			ValSerde: eventSerde,
			MsgSerde: msgSerde,
		},
	}
	outConfig := &producer_consumer.StreamSinkConfig{
		KVMsgSerdes: commtypes.KVMsgSerdes{
			MsgSerde: msgSerde,
			ValSerde: eventSerde,
			KeySerde: seSerde,
		},
		FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
	}
	warmup := time.Duration(sp.WarmupS) * time.Second
	src := producer_consumer.NewMeteredConsumer(producer_consumer.NewShardedSharedLogStreamConsumer(input_stream, inConfig), warmup)
	sink := producer_consumer.NewMeteredProducer(producer_consumer.NewShardedSharedLogStreamProducer(output_streams[0], outConfig), warmup)
	src.SetInitialSource(true)
	return []producer_consumer.MeteredConsumerIntr{src}, []producer_consumer.MeteredProducerIntr{sink}, nil
}

func (h *q7BidByWin) q7BidByWin(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	srcs, sinks_arr, err := h.getSrcSink(ctx, sp)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	warmup := time.Duration(sp.WarmupS) * time.Second
	tw, err := processor.NewTimeWindowsWithGrace(time.Duration(10)*time.Second, time.Duration(2)*time.Second)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	bid := processor.NewMeteredProcessor(processor.NewStreamFilterProcessor("filterBids",
		processor.PredicateFunc(func(msg *commtypes.Message) (bool, error) {
			event := msg.Value.(*ntypes.Event)
			return event.Etype == ntypes.BID, nil
		})), warmup)
	bidByWin := processor.NewMeteredProcessor(processor.NewStreamMapProcessor(
		"bidByWin", processor.MapperFunc(func(msg commtypes.Message) (commtypes.Message, error) {
			event := msg.Value.(*ntypes.Event)
			ts := event.Bid.DateTime
			windowStart := utils.MaxInt64(0, ts-tw.SizeMs+tw.AdvanceMs) / tw.AdvanceMs * tw.AdvanceMs
			wEnd := windowStart + tw.SizeMs
			debug.Assert(windowStart >= 0, "window start should be >= 0")
			debug.Assert(wEnd > 0, "window end should be > 0")
			win := ntypes.StartEndTime{StartTimeMs: windowStart, EndTimeMs: wEnd}
			return commtypes.Message{Key: win, Value: msg.Value, Timestamp: msg.Timestamp}, nil
		})), warmup)

	groupBy := processor.NewGroupBy(sinks_arr[0])
	procArgs := &q7BidByWinProcessArgs{
		bid:      bid,
		bidByWin: bidByWin,
		groupBy:  groupBy,
		BaseExecutionContext: proc_interface.NewExecutionContext(srcs, sinks_arr,
			h.funcName, sp.ScaleEpoch, sp.ParNum),
	}
	task := stream_task.NewStreamTaskBuilder().
		AppProcessFunc(func(ctx context.Context, task *stream_task.StreamTask, argsTmp interface{}) *common.FnOutput {
			args := argsTmp.(proc_interface.ExecutionContext)
			return execution.CommonProcess(ctx, task, args, h.procMsg)
		}).
		InitFunc(func(progArgs interface{}) {
			bid.StartWarmup()
			bidByWin.StartWarmup()
		}).Build()

	update_stats := func(ret *common.FnOutput) {
		ret.Latencies["bid"] = bid.GetLatency()
		ret.Latencies["bidKeyedByPrice"] = bidByWin.GetLatency()
	}
	transactionalID := fmt.Sprintf("%s-%s-%d-%s", h.funcName, sp.InputTopicNames[0], sp.ParNum, sp.OutputTopicNames[0])
	streamTaskArgs := benchutil.UpdateStreamTaskArgs(sp,
		stream_task.NewStreamTaskArgsBuilder(h.env, procArgs, transactionalID)).Build()
	return task.ExecuteApp(ctx, streamTaskArgs, update_stats)
}
