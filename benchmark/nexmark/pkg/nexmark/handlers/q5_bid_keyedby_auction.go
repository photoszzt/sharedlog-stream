package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/execution"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/stream_task"

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

/*
type bidKeyedByAuctionProcessArgs struct {
	filterBid *processor.MeteredProcessor
	selectKey *processor.MeteredProcessor
	groupBy   *processor.MeteredProcessor
	processor.BaseExecutionContext
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
		_, err = args.groupBy.ProcessAndReturn(ctx, mappedKey[0])
		if err != nil {
			return err
		}
	}
	return nil
}
*/

func (h *bidByAuction) processBidKeyedByAuction(ctx context.Context,
	sp *common.QueryInput,
) *common.FnOutput {
	srcs, sinks, err := getSrcSinkUint64Key(ctx, h.env, sp)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	srcs[0].SetInitialSource(true)
	ectx := processor.NewExecutionContext(srcs,
		sinks, h.funcName, sp.ScaleEpoch, sp.ParNum)
	ectx.Via(processor.NewMeteredProcessor(
		processor.NewStreamFilterProcessor("filterBid", processor.PredicateFunc(
			func(key, value interface{}) (bool, error) {
				event := value.(*ntypes.Event)
				return event.Etype == ntypes.BID, nil
			})))).
		Via(processor.NewMeteredProcessor(
			processor.NewStreamMapProcessor("selectKey", processor.MapperFunc(
				func(key, value interface{}) (interface{}, interface{}, error) {
					event := value.(*ntypes.Event)
					return event.Bid.Auction, value, nil
				})))).
		Via(processor.NewGroupByOutputProcessor(sinks[0], &ectx))
	task := stream_task.NewStreamTaskBuilder().
		AppProcessFunc(func(ctx context.Context, task *stream_task.StreamTask, argsTmp interface{}) *common.FnOutput {
			args := argsTmp.(*processor.BaseExecutionContext)
			return execution.CommonProcess(ctx, task, args, processor.ProcessMsg)
		}).Build()
	transactionalID := fmt.Sprintf("%s-%s-%d-%s", h.funcName,
		sp.InputTopicNames[0], sp.ParNum, sp.OutputTopicNames[0])
	builder := stream_task.NewStreamTaskArgsBuilder(h.env, &ectx, transactionalID)
	streamTaskArgs := benchutil.UpdateStreamTaskArgs(sp, builder).Build()
	return stream_task.ExecuteApp(ctx, task, streamTaskArgs)
}
