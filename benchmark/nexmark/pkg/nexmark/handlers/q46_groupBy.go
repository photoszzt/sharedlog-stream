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
	"sharedlog-stream/pkg/stream_task"
	"sync"
	"time"

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

func (h *q46GroupByHandler) procMsg(ctx context.Context, msg commtypes.Message, argsTmp interface{}) error {
	args := argsTmp.(*TwoMsgChanProcArgs)
	args.msgChan1 <- msg
	args.msgChan2 <- msg
	return nil
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
	srcs, sinks_arr, err := getSrcSinks(ctx, h.env, sp)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	srcs[0].SetInitialSource(true)
	auctionsByIDFunc := h.getAucsByID(time.Duration(sp.WarmupS) * time.Second)
	bidsByAuctionIDFunc := h.getBidsByAuctionID(time.Duration(sp.WarmupS) * time.Second)

	var wg sync.WaitGroup
	auctionsByIDManager := execution.NewGeneralProcManager(auctionsByIDFunc)
	bidsByAuctionIDManager := execution.NewGeneralProcManager(bidsByAuctionIDFunc)
	procArgs := &TwoMsgChanProcArgs{
		msgChan1: auctionsByIDManager.MsgChan(),
		msgChan2: bidsByAuctionIDManager.MsgChan(),
		BaseExecutionContext: processor.NewExecutionContext(srcs, sinks_arr,
			h.funcName, sp.ScaleEpoch, sp.ParNum),
	}
	auctionsByIDManager.LaunchProc(ctx, procArgs, &wg)
	bidsByAuctionIDManager.LaunchProc(ctx, procArgs, &wg)

	handleErrFunc := func() error {
		select {
		case aucErr := <-auctionsByIDManager.ErrChan():
			return aucErr
		case bidErr := <-bidsByAuctionIDManager.ErrChan():
			return bidErr
		default:
		}
		return nil
	}

	auctionsByIDManager.LaunchProc(ctx, procArgs, &wg)
	bidsByAuctionIDManager.LaunchProc(ctx, procArgs, &wg)

	task := stream_task.NewStreamTaskBuilder().
		AppProcessFunc(func(ctx context.Context, task *stream_task.StreamTask, argsTmp interface{}) *common.FnOutput {
			args := argsTmp.(processor.ExecutionContext)
			return execution.CommonProcess(ctx, task, args, h.procMsg)
		}).
		PauseFunc(func() *common.FnOutput {
			// debug.Fprintf(os.Stderr, "begin flush\n")
			auctionsByIDManager.RequestToTerminate()
			bidsByAuctionIDManager.RequestToTerminate()
			wg.Wait()
			if err := handleErrFunc(); err != nil {
				return &common.FnOutput{Success: false, Message: err.Error()}
			}
			// debug.Fprintf(os.Stderr, "done flush\n")
			return nil
		}).
		ResumeFunc(func(task *stream_task.StreamTask) {
			debug.Fprintf(os.Stderr, "start resume\n")
			auctionsByIDManager.RecreateMsgChan(&procArgs.msgChan1)
			bidsByAuctionIDManager.RecreateMsgChan(&procArgs.msgChan2)
			auctionsByIDManager.LaunchProc(ctx, procArgs, &wg)
			bidsByAuctionIDManager.LaunchProc(ctx, procArgs, &wg)
			debug.Fprintf(os.Stderr, "done resume\n")
		}).HandleErrFunc(handleErrFunc).Build()
	transactionalID := fmt.Sprintf("%s-%s-%d", h.funcName, sp.InputTopicNames[0], sp.ParNum)
	streamTaskArgs := benchutil.UpdateStreamTaskArgs(sp,
		stream_task.NewStreamTaskArgsBuilder(h.env, procArgs, transactionalID)).Build()
	return task.ExecuteApp(ctx, streamTaskArgs)
}

func (h *q46GroupByHandler) getAucsByID(warmup time.Duration) execution.GeneralProcFunc {
	filterAuctions := processor.NewMeteredProcessor(processor.NewStreamFilterProcessor("filterAuctions",
		processor.PredicateFunc(
			func(m *commtypes.Message) (bool, error) {
				event := m.Value.(*ntypes.Event)
				return event.Etype == ntypes.AUCTION, nil
			})))

	auctionsByIDMap := processor.NewMeteredProcessor(processor.NewStreamMapProcessor("auctionsByIDMap",
		processor.MapperFunc(func(msg commtypes.Message) (commtypes.Message, error) {
			event := msg.Value.(*ntypes.Event)
			return commtypes.Message{Key: event.NewAuction.ID, Value: msg.Value, Timestamp: msg.Timestamp}, nil
		})))

	return func(ctx context.Context, argsTmp interface{}, wg *sync.WaitGroup,
		msgChan chan commtypes.Message, errChan chan error,
	) {
		args := argsTmp.(*TwoMsgChanProcArgs)
		g := processor.NewGroupByOutputProcessor(args.Producers()[0], args)
		defer wg.Done()
	L:
		for {
			select {
			case <-ctx.Done():
				break L
			case msg, ok := <-msgChan:
				if !ok {
					break L
				}
				filteredMsgs, err := filterAuctions.ProcessAndReturn(ctx, msg)
				if err != nil {
					fmt.Fprintf(os.Stderr, "[ERROR] filterAuctions err: %v", err)
					errChan <- fmt.Errorf("filterAuctions err: %v", err)
					return
				}
				for _, filteredMsg := range filteredMsgs {
					changeKeyedMsg, err := auctionsByIDMap.ProcessAndReturn(ctx, filteredMsg)
					if err != nil {
						fmt.Fprintf(os.Stderr, "[ERROR] auctionsBySellerIDMap err: %v", err)
						errChan <- fmt.Errorf("auctionsBySellerIDMap err: %v", err)
						return
					}
					_, err = g.ProcessAndReturn(ctx, changeKeyedMsg[0])
					if err != nil {
						fmt.Fprintf(os.Stderr, "[ERROR] groupby failed: %v", err)
						errChan <- err
						return
					}
				}
			}
		}
	}
}

func (h *q46GroupByHandler) getBidsByAuctionID(warmup time.Duration) execution.GeneralProcFunc {
	filterBids := processor.NewMeteredProcessor(processor.NewStreamFilterProcessor("filterBids",
		processor.PredicateFunc(
			func(m *commtypes.Message) (bool, error) {
				event := m.Value.(*ntypes.Event)
				return event.Etype == ntypes.BID, nil
			})))

	bidsByAuctionIDMap := processor.NewMeteredProcessor(processor.NewStreamMapProcessor("bidsByAuctionIDMap",
		processor.MapperFunc(func(msg commtypes.Message) (commtypes.Message, error) {
			event := msg.Value.(*ntypes.Event)
			return commtypes.Message{Key: event.Bid.Auction, Value: msg.Value, Timestamp: msg.Timestamp}, nil
		})))
	return func(ctx context.Context, argsTmp interface{}, wg *sync.WaitGroup,
		msgChan chan commtypes.Message, errChan chan error,
	) {
		args := argsTmp.(*TwoMsgChanProcArgs)
		g := processor.NewGroupByOutputProcessor(args.Producers()[1], args)
		defer wg.Done()
	L:
		for {
			select {
			case <-ctx.Done():
				break L
			case msg, ok := <-msgChan:
				if !ok {
					break L
				}
				filteredMsgs, err := filterBids.ProcessAndReturn(ctx, msg)
				if err != nil {
					fmt.Fprintf(os.Stderr, "[ERROR] filterAuctions err: %v", err)
					errChan <- fmt.Errorf("filterAuctions err: %v", err)
					return
				}
				for _, filteredMsg := range filteredMsgs {
					changeKeyedMsg, err := bidsByAuctionIDMap.ProcessAndReturn(ctx, filteredMsg)
					if err != nil {
						fmt.Fprintf(os.Stderr, "[ERROR] auctionsBySellerIDMap err: %v", err)
						errChan <- fmt.Errorf("auctionsBySellerIDMap err: %v", err)
						return
					}
					_, err = g.ProcessAndReturn(ctx, changeKeyedMsg[0])
					if err != nil {
						fmt.Fprintf(os.Stderr, "[ERROR] groupBy failed: %v", err)
						errChan <- err
						return
					}
				}
			}
		}
	}
}
