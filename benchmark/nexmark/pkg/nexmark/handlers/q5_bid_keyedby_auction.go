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

type bidByAuction struct {
	env      types.Environment
	funcName string
}

func NewBidByAuctionHandler(env types.Environment, funcName string) types.FuncHandler {
	return &bidByAuction{
		env:      env,
		funcName: funcName,
	}
}

func (h *bidByAuction) Call(ctx context.Context, input []byte) ([]byte, error) {
	sp := &common.QueryInput{}
	err := json.Unmarshal(input, sp)
	if err != nil {
		return nil, err
	}
	output := h.processBidKeyedByAuction(ctx, sp)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		return nil, err
	}
	return utils.CompressData(encodedOutput), nil
}

type bidKeyedByAuctionProcessArgs struct {
	filterBid *processor.MeteredProcessor
	selectKey *processor.MeteredProcessor
	groupBy   *processor.GroupBy
	proc_interface.BaseExecutionContext
}

func (h *bidByAuction) procMsg(ctx context.Context, msg commtypes.Message, argsTmp interface{}) error {
	args := argsTmp.(*bidKeyedByAuctionProcessArgs)
	bidMsg, err := args.filterBid.ProcessAndReturn(ctx, msg)
	if err != nil {
		return err
	}
	if bidMsg != nil {
		mappedKey, err := args.selectKey.ProcessAndReturn(ctx, bidMsg[0])
		if err != nil {
			return err
		}
		err = args.groupBy.GroupByAndProduce(ctx, mappedKey[0], args.TrackParFunc())
		if err != nil {
			return err
		}
	}
	return nil
}

func (h *bidByAuction) processBidKeyedByAuction(ctx context.Context,
	sp *common.QueryInput,
) *common.FnOutput {
	srcs, sinks, err := getSrcSinkUint64Key(ctx, h.env, sp)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	srcs[0].SetInitialSource(true)
	filterBid := processor.NewMeteredProcessor(processor.NewStreamFilterProcessor(processor.PredicateFunc(
		func(m *commtypes.Message) (bool, error) {
			event := m.Value.(*ntypes.Event)
			return event.Etype == ntypes.BID, nil
		})), time.Duration(sp.WarmupS)*time.Second)
	selectKey := processor.NewMeteredProcessor(processor.NewStreamMapProcessor(processor.MapperFunc(
		func(m commtypes.Message) (commtypes.Message, error) {
			event := m.Value.(*ntypes.Event)
			return commtypes.Message{Key: event.Bid.Auction, Value: m.Value, Timestamp: m.Timestamp}, nil
		})), time.Duration(sp.WarmupS)*time.Second)
	groupBy := processor.NewGroupBy(sinks[0])
	procArgs := &bidKeyedByAuctionProcessArgs{
		filterBid: filterBid,
		selectKey: selectKey,
		groupBy:   groupBy,
		BaseExecutionContext: proc_interface.NewExecutionContext(srcs,
			sinks, h.funcName, sp.ScaleEpoch, sp.ParNum),
	}
	task := stream_task.NewStreamTaskBuilder().
		AppProcessFunc(func(ctx context.Context, task *stream_task.StreamTask, argsTmp interface{}) *common.FnOutput {
			args := argsTmp.(proc_interface.ExecutionContext)
			return execution.CommonProcess(ctx, task, args, h.procMsg)
		}).
		InitFunc(func(progArgs interface{}) {
			filterBid.StartWarmup()
			selectKey.StartWarmup()
		}).Build()

	update_stats := func(ret *common.FnOutput) {
		ret.Latencies["filterBid"] = filterBid.GetLatency()
		ret.Latencies["selectKey"] = selectKey.GetLatency()
	}
	transactionalID := fmt.Sprintf("%s-%s-%d-%s", h.funcName,
		sp.InputTopicNames[0],
		sp.ParNum, sp.OutputTopicNames[0])
	builder := stream_task.NewStreamTaskArgsBuilder(h.env, procArgs, transactionalID)
	streamTaskArgs := benchutil.UpdateStreamTaskArgs(sp, builder).Build()
	return task.ExecuteApp(ctx, streamTaskArgs, update_stats)
}
