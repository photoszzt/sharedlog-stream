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
	"sharedlog-stream/pkg/stream_task"

	"cs.utexas.edu/zjia/faas/types"
)

type q46GroupByHandler struct {
	env      types.Environment
	funcName string
}

func NewQ46GroupByHandler(env types.Environment, funcName string) types.FuncHandler {
	return &q46GroupByHandler{
		env:      env,
		funcName: funcName,
	}
}

func (h *q46GroupByHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &common.QueryInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output := h.Q46GroupBy(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	return common.CompressData(encodedOutput), nil
}

func (h *q46GroupByHandler) Q46GroupBy(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	ectx, err := getExecutionCtx(ctx, h.env, sp, h.funcName)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	ectx.Consumers()[0].SetInitialSource(true)
	ectx.Producers()[0].SetName("aucsByIDSink")
	ectx.Producers()[1].SetName("bidsByAucIDSink")
	aucByIDChain := processor.NewProcessorChains()
	aucByIDChain.
		Via(processor.NewMeteredProcessor(processor.NewStreamSelectKeyProcessor("auctionsByIDMap",
			processor.SelectKeyFunc(func(key, value interface{}) (interface{}, error) {
				event := value.(*ntypes.Event)
				return event.NewAuction.ID, nil
			})))).
		Via(processor.NewGroupByOutputProcessor(ectx.Producers()[0], &ectx))
	bidsByAucIDChain := processor.NewProcessorChains()
	bidsByAucIDChain.Via(processor.NewMeteredProcessor(processor.NewStreamSelectKeyProcessor("bidsByAuctionIDMap",
		processor.SelectKeyFunc(func(key, value interface{}) (interface{}, error) {
			event := value.(*ntypes.Event)
			return event.Bid.Auction, nil
		})))).
		Via(processor.NewGroupByOutputProcessor(ectx.Producers()[1], &ectx))

	task := stream_task.NewStreamTaskBuilder().AppProcessFunc(
		func(ctx context.Context, task *stream_task.StreamTask,
			argsTmp processor.ExecutionContext,
		) (*common.FnOutput, *commtypes.MsgAndSeq) {
			args := argsTmp.(*processor.BaseExecutionContext)
			return stream_task.CommonProcess(ctx, task, args,
				func(ctx context.Context, msg commtypes.Message, argsTmp interface{}) error {
					event := msg.Value.(*ntypes.Event)
					if event.Etype == ntypes.AUCTION {
						_, err := aucByIDChain.RunChains(ctx, msg)
						if err != nil {
							return err
						}
					} else if event.Etype == ntypes.BID {
						_, err := bidsByAucIDChain.RunChains(ctx, msg)
						if err != nil {
							return err
						}
					}
					return nil
				})
		}).Build()
	transactionalID := fmt.Sprintf("%s-%s-%d", h.funcName, sp.InputTopicNames[0], sp.ParNum)
	streamTaskArgs := benchutil.UpdateStreamTaskArgs(sp,
		stream_task.NewStreamTaskArgsBuilder(h.env, &ectx, transactionalID)).Build()
	return stream_task.ExecuteApp(ctx, task, streamTaskArgs)
}
