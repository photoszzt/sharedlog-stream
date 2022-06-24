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
	"sharedlog-stream/pkg/stream_task"
	"sync"

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
	return utils.CompressData(encodedOutput), nil
}

func (h *q46GroupByHandler) Q46GroupBy(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	ectx, err := getExecutionCtx(ctx, h.env, sp, h.funcName)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	ectx.Consumers()[0].SetInitialSource(true)
	auctionsByIDFunc := h.getAucsByID()
	bidsByAuctionIDFunc := h.getBidsByAuctionID()
	dctx, dcancel := context.WithCancel(ctx)
	defer dcancel()
	task, procArgs := PrepareProcessByTwoGeneralProc(dctx, auctionsByIDFunc,
		bidsByAuctionIDFunc, ectx, procMsgWithTwoMsgChan)
	transactionalID := fmt.Sprintf("%s-%s-%d", h.funcName, sp.InputTopicNames[0], sp.ParNum)
	streamTaskArgs := benchutil.UpdateStreamTaskArgs(sp,
		stream_task.NewStreamTaskArgsBuilder(h.env, procArgs, transactionalID)).Build()
	return task.ExecuteApp(dctx, streamTaskArgs)
}

func (h *q46GroupByHandler) getAucsByID() execution.GeneralProcFunc {
	gpCtx := execution.NewGeneralProcCtx()

	gpCtx.AppendProcessor(processor.NewMeteredProcessor(processor.NewStreamFilterProcessor("filterAuctions",
		processor.PredicateFunc(
			func(m *commtypes.Message) (bool, error) {
				event := m.Value.(*ntypes.Event)
				return event.Etype == ntypes.AUCTION, nil
			}))))

	gpCtx.AppendProcessor(processor.NewMeteredProcessor(processor.NewStreamMapProcessor("auctionsByIDMap",
		processor.MapperFunc(func(msg commtypes.Message) (commtypes.Message, error) {
			event := msg.Value.(*ntypes.Event)
			return commtypes.Message{Key: event.NewAuction.ID, Value: msg.Value, Timestamp: msg.Timestamp}, nil
		}))))

	return func(ctx context.Context, argsTmp interface{}, wg *sync.WaitGroup,
		msgChan chan commtypes.Message, errChan chan error,
	) {
		args := argsTmp.(*TwoMsgChanProcArgs)
		gpCtx.AppendProcessor(processor.NewGroupByOutputProcessor(args.Producers()[0], args))
		defer wg.Done()
		gpCtx.GeneralProc(ctx, args.Producers()[0], msgChan, errChan)
	}
}

func (h *q46GroupByHandler) getBidsByAuctionID() execution.GeneralProcFunc {
	gpCtx := execution.NewGeneralProcCtx()
	gpCtx.AppendProcessor(processor.NewMeteredProcessor(processor.NewStreamFilterProcessor("filterBids",
		processor.PredicateFunc(
			func(m *commtypes.Message) (bool, error) {
				event := m.Value.(*ntypes.Event)
				return event.Etype == ntypes.BID, nil
			}))))

	gpCtx.AppendProcessor(processor.NewMeteredProcessor(processor.NewStreamMapProcessor("bidsByAuctionIDMap",
		processor.MapperFunc(func(msg commtypes.Message) (commtypes.Message, error) {
			event := msg.Value.(*ntypes.Event)
			return commtypes.Message{Key: event.Bid.Auction, Value: msg.Value, Timestamp: msg.Timestamp}, nil
		}))))
	return func(ctx context.Context, argsTmp interface{}, wg *sync.WaitGroup,
		msgChan chan commtypes.Message, errChan chan error,
	) {
		args := argsTmp.(*TwoMsgChanProcArgs)
		gpCtx.AppendProcessor(processor.NewGroupByOutputProcessor(args.Producers()[1], args))
		defer wg.Done()
		gpCtx.GeneralProc(ctx, args.Producers()[1], msgChan, errChan)
	}
}
