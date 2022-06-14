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
	"sharedlog-stream/pkg/proc_interface"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/stream_task"
	"time"

	"cs.utexas.edu/zjia/faas/types"
)

type q7BidByPrice struct {
	env      types.Environment
	funcName string
}

func NewQ7BidByPriceHandler(env types.Environment, funcName string) types.FuncHandler {
	return &q7BidByPrice{
		env:      env,
		funcName: funcName,
	}
}

func (h *q7BidByPrice) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &common.QueryInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output := h.q7BidByPrice(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	return utils.CompressData(encodedOutput), nil
}

type q7BidKeyedByPriceProcessArgs struct {
	bid        *processor.MeteredProcessor
	bidByPrice *processor.MeteredProcessor
	groupBy    *processor.GroupBy
	proc_interface.BaseExecutionContext
}

func (h *q7BidByPrice) procMsg(ctx context.Context, msg commtypes.Message, argsTmp interface{}) error {
	args := argsTmp.(*q7BidKeyedByPriceProcessArgs)
	bidMsg, err := args.bid.ProcessAndReturn(ctx, msg)
	if err != nil {
		return fmt.Errorf("filter bid err: %v", err)
	}
	if bidMsg != nil {
		mappedKey, err := args.bidByPrice.ProcessAndReturn(ctx, bidMsg[0])
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

func (h *q7BidByPrice) q7BidByPrice(ctx context.Context, input *common.QueryInput) *common.FnOutput {
	srcs, sinks_arr, err := getSrcSinkUint64Key(ctx, h.env, input)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	srcs[0].SetInitialSource(true)

	warmup := time.Duration(input.WarmupS) * time.Second
	bid := processor.NewMeteredProcessor(processor.NewStreamFilterProcessor(processor.PredicateFunc(func(msg *commtypes.Message) (bool, error) {
		event := msg.Value.(*ntypes.Event)
		return event.Etype == ntypes.BID, nil
	})), warmup)
	bidKeyedByPrice := processor.NewMeteredProcessor(processor.NewStreamMapProcessor(processor.MapperFunc(func(msg commtypes.Message) (commtypes.Message, error) {
		event := msg.Value.(*ntypes.Event)
		return commtypes.Message{Key: event.Bid.Price, Value: msg.Value, Timestamp: msg.Timestamp}, nil
	})), warmup)
	groupBy := processor.NewGroupBy(sinks_arr[0])
	procArgs := &q7BidKeyedByPriceProcessArgs{
		bid:        bid,
		bidByPrice: bidKeyedByPrice,
		groupBy:    groupBy,
		BaseExecutionContext: proc_interface.NewExecutionContext(srcs, sinks_arr, h.funcName,
			input.ScaleEpoch, input.ParNum),
	}
	task := stream_task.NewStreamTaskBuilder().
		AppProcessFunc(func(ctx context.Context, task *stream_task.StreamTask, argsTmp interface{}) *common.FnOutput {
			args := argsTmp.(proc_interface.ExecutionContext)
			return execution.CommonProcess(ctx, task, args, h.procMsg)
		}).
		InitFunc(func(progArgs interface{}) {
			bid.StartWarmup()
			bidKeyedByPrice.StartWarmup()
		}).Build()

	update_stats := func(ret *common.FnOutput) {
		ret.Latencies["bid"] = bid.GetLatency()
		ret.Latencies["bidKeyedByPrice"] = bidKeyedByPrice.GetLatency()
	}
	transactionalID := fmt.Sprintf("%s-%s-%d-%s", h.funcName, input.InputTopicNames[0], input.ParNum, input.OutputTopicNames[0])
	streamTaskArgs := benchutil.UpdateStreamTaskArgs(input,
		stream_task.NewStreamTaskArgsBuilder(h.env, procArgs, transactionalID)).Build()
	return task.ExecuteApp(ctx, streamTaskArgs, input.EnableTransaction, update_stats)
}
