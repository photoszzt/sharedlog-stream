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
	"sharedlog-stream/pkg/control_channel"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/execution"
	"sharedlog-stream/pkg/hash"
	"sharedlog-stream/pkg/source_sink"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/proc_interface"
	"sharedlog-stream/pkg/transaction"
	"sharedlog-stream/pkg/transaction/tran_interface"
	"sync"
	"time"

	"cs.utexas.edu/zjia/faas/types"
	"golang.org/x/xerrors"
)

type q46GroupByHandler struct {
	env types.Environment

	aucHashMu sync.RWMutex
	aucHash   *hash.ConsistentHash

	bidHashMu sync.RWMutex
	bidHash   *hash.ConsistentHash

	funcName string
}

func NewQ46GroupByHandler(env types.Environment, funcName string) types.FuncHandler {
	return &q46GroupByHandler{
		env:      env,
		aucHash:  hash.NewConsistentHash(),
		bidHash:  hash.NewConsistentHash(),
		funcName: funcName,
	}
}

func (h *q46GroupByHandler) process(
	ctx context.Context,
	t *transaction.StreamTask,
	argsTmp interface{},
) *common.FnOutput {
	args := argsTmp.(*TwoMsgChanProcArgs)
	return execution.CommonProcess(ctx, t, args, func(t *transaction.StreamTask, msg commtypes.MsgAndSeq) error {
		t.CurrentOffset[args.src.TopicName()] = msg.LogSeqNum
		if msg.MsgArr != nil {
			for _, subMsg := range msg.MsgArr {
				if subMsg.Value == nil {
					continue
				}
				args.msgChan1 <- subMsg
				args.msgChan2 <- subMsg
			}
			return nil
		} else {
			args.msgChan1 <- msg.Msg
			args.msgChan2 <- msg.Msg
			return nil
		}
	})
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
	input_stream, output_streams, err := benchutil.GetShardedInputOutputStreams(ctx, h.env, sp)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("get input output stream failed: %v", err),
		}
	}
	debug.Assert(len(output_streams) == 2, "expected 2 output streams")
	src, sinks, err := getSrcSinks(ctx, sp, input_stream, output_streams)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	src.SetInitialSource(true)
	filterAuctions, auctionsByIDMap, auctionsByIDFunc := h.getAucsByID(time.Duration(sp.WarmupS) * time.Second)
	filterBids, bidsByAuctionIDMap, bidsByAuctionIDFunc := h.getBidsByAuctionID(time.Duration(sp.WarmupS) * time.Second)

	var wg sync.WaitGroup
	auctionsByIDManager := execution.NewGeneralProcManager(auctionsByIDFunc)
	bidsByAuctionIDManager := execution.NewGeneralProcManager(bidsByAuctionIDFunc)
	sinks_arr := []source_sink.Sink{sinks[0], sinks[1]}
	procArgs := &TwoMsgChanProcArgs{
		src:                  src,
		msgChan1:             auctionsByIDManager.MsgChan(),
		msgChan2:             bidsByAuctionIDManager.MsgChan(),
		trackParFunc:         tran_interface.DefaultTrackSubstreamFunc,
		BaseProcArgsWithSink: proc_interface.NewBaseProcArgsWithSink(sinks_arr, h.funcName, sp.ScaleEpoch, sp.ParNum),
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

	task := transaction.StreamTask{
		ProcessFunc:   h.process,
		HandleErrFunc: handleErrFunc,
		PauseFunc: func() *common.FnOutput {
			// debug.Fprintf(os.Stderr, "begin flush\n")
			auctionsByIDManager.RequestToTerminate()
			bidsByAuctionIDManager.RequestToTerminate()
			wg.Wait()
			if err := handleErrFunc(); err != nil {
				return &common.FnOutput{Success: false, Message: err.Error()}
			}
			// debug.Fprintf(os.Stderr, "done flush\n")
			return nil
		},
		ResumeFunc: func(task *transaction.StreamTask) {
			debug.Fprintf(os.Stderr, "start resume\n")
			auctionsByIDManager.RecreateMsgChan(&procArgs.msgChan1)
			bidsByAuctionIDManager.RecreateMsgChan(&procArgs.msgChan2)
			auctionsByIDManager.LaunchProc(ctx, procArgs, &wg)
			bidsByAuctionIDManager.LaunchProc(ctx, procArgs, &wg)
			debug.Fprintf(os.Stderr, "done resume\n")
		},
		InitFunc: func(progArgsTmp interface{}) {
			progArgs := progArgsTmp.(*TwoMsgChanProcArgs)
			progArgs.src.StartWarmup()
			sinks[0].StartWarmup()
			sinks[1].StartWarmup()
			filterAuctions.StartWarmup()
			auctionsByIDMap.StartWarmup()
			filterBids.StartWarmup()
			bidsByAuctionIDMap.StartWarmup()
			// debug.Fprintf(os.Stderr, "done warmup start\n")
		},
		CurrentOffset:             make(map[string]uint64),
		CommitEveryForAtLeastOnce: common.CommitDuration,
	}
	control_channel.SetupConsistentHash(&h.aucHashMu, h.aucHash, sp.NumOutPartitions[0])
	control_channel.SetupConsistentHash(&h.bidHashMu, h.bidHash, sp.NumOutPartitions[1])
	srcs := []source_sink.Source{src}

	update_stats := func(ret *common.FnOutput) {
		ret.Latencies["filterAuctions"] = filterAuctions.GetLatency()
		ret.Latencies["auctionsByIDMap"] = auctionsByIDMap.GetLatency()
		ret.Latencies["filterBids"] = filterBids.GetLatency()
		ret.Latencies["bidsByAuctionIDMap"] = bidsByAuctionIDMap.GetLatency()
		ret.Counts["src"] = src.GetCount()
		ret.Counts["aucSink"] = sinks[0].GetCount()
		ret.Counts["bidSink"] = sinks[1].GetCount()
	}

	if sp.EnableTransaction {
		transactionalID := fmt.Sprintf("%s-%s-%d", h.funcName, sp.InputTopicNames[0], sp.ParNum)
		streamTaskArgs := transaction.NewStreamTaskArgsTransaction(h.env, transactionalID, procArgs, srcs, sinks_arr)
		benchutil.UpdateStreamTaskArgsTransaction(sp, streamTaskArgs)
		ret := transaction.SetupManagersAndProcessTransactional(ctx, h.env, streamTaskArgs,
			func(procArgs interface{}, trackParFunc tran_interface.TrackKeySubStreamFunc,
				recordFinishFunc tran_interface.RecordPrevInstanceFinishFunc) {
				procArgs.(*TwoMsgChanProcArgs).trackParFunc = trackParFunc
				procArgs.(*TwoMsgChanProcArgs).SetRecordFinishFunc(recordFinishFunc)
			}, &task)
		if ret != nil && ret.Success {
			update_stats(ret)
		}
		return ret
	} else {
		streamTaskArgs := transaction.NewStreamTaskArgs(h.env, procArgs, srcs, sinks_arr)
		benchutil.UpdateStreamTaskArgs(sp, streamTaskArgs)
		ret := task.Process(ctx, streamTaskArgs)
		if ret != nil && ret.Success {
			update_stats(ret)
		}
		return ret
	}
}

func (h *q46GroupByHandler) getAucsByID(warmup time.Duration) (
	*processor.MeteredProcessor,
	*processor.MeteredProcessor,
	execution.GeneralProcFunc,
) {
	filterAuctions := processor.NewMeteredProcessor(processor.NewStreamFilterProcessor(processor.PredicateFunc(
		func(m *commtypes.Message) (bool, error) {
			event := m.Value.(*ntypes.Event)
			return event.Etype == ntypes.AUCTION, nil
		})), time.Duration(warmup)*time.Second)

	auctionsByIDMap := processor.NewMeteredProcessor(processor.NewStreamMapProcessor(
		processor.MapperFunc(func(msg commtypes.Message) (commtypes.Message, error) {
			event := msg.Value.(*ntypes.Event)
			return commtypes.Message{Key: event.NewAuction.ID, Value: msg.Value, Timestamp: msg.Timestamp}, nil
		})), time.Duration(warmup)*time.Second)

	return filterAuctions, auctionsByIDMap, func(ctx context.Context, argsTmp interface{}, wg *sync.WaitGroup,
		msgChan chan commtypes.Message, errChan chan error,
	) {
		args := argsTmp.(*TwoMsgChanProcArgs)
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
				event := msg.Value.(*ntypes.Event)
				ts, err := event.ExtractEventTime()
				if err != nil {
					fmt.Fprintf(os.Stderr, "[ERROR] fail to extract timestamp: %v\n", err)
					errChan <- fmt.Errorf("fail to extract timestamp: %v", err)
					return
				}
				msg.Timestamp = ts
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

					k := changeKeyedMsg[0].Key.(uint64)
					h.aucHashMu.RLock()
					parTmp, ok := h.aucHash.Get(k)
					h.aucHashMu.RUnlock()
					if !ok {
						fmt.Fprintf(os.Stderr, "[ERROR] fail to get output partition\n")
						errChan <- xerrors.New("fail to get output partition")
						return
					}
					par := parTmp.(uint8)
					err = args.trackParFunc(ctx, k, args.Sinks()[0].KeySerde(), args.Sinks()[0].TopicName(), par)
					if err != nil {
						fmt.Fprintf(os.Stderr, "[ERROR] add topic partition failed: %v\n", err)
						errChan <- fmt.Errorf("add topic partition failed: %v", err)
						return
					}
					err = args.Sinks()[0].Produce(ctx, changeKeyedMsg[0], par, false)
					if err != nil {
						fmt.Fprintf(os.Stderr, "[ERROR] sink err: %v\n", err)
						errChan <- fmt.Errorf("sink err: %v", err)
						return
					}
				}
			}
		}
	}
}

func (h *q46GroupByHandler) getBidsByAuctionID(warmup time.Duration) (
	*processor.MeteredProcessor,
	*processor.MeteredProcessor,
	execution.GeneralProcFunc,
) {
	filterBids := processor.NewMeteredProcessor(processor.NewStreamFilterProcessor(processor.PredicateFunc(
		func(m *commtypes.Message) (bool, error) {
			event := m.Value.(*ntypes.Event)
			return event.Etype == ntypes.BID, nil
		})), time.Duration(warmup)*time.Second)

	bidsByAuctionIDMap := processor.NewMeteredProcessor(processor.NewStreamMapProcessor(
		processor.MapperFunc(func(msg commtypes.Message) (commtypes.Message, error) {
			event := msg.Value.(*ntypes.Event)
			return commtypes.Message{Key: event.Bid.Auction, Value: msg.Value, Timestamp: msg.Timestamp}, nil
		})), time.Duration(warmup)*time.Second)
	return filterBids, bidsByAuctionIDMap, func(ctx context.Context, argsTmp interface{}, wg *sync.WaitGroup,
		msgChan chan commtypes.Message, errChan chan error,
	) {
		args := argsTmp.(*TwoMsgChanProcArgs)
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
				event := msg.Value.(*ntypes.Event)
				ts, err := event.ExtractEventTime()
				if err != nil {
					fmt.Fprintf(os.Stderr, "[ERROR] fail to extract timestamp: %v\n", err)
					errChan <- fmt.Errorf("fail to extract timestamp: %v", err)
					return
				}
				msg.Timestamp = ts
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

					k := changeKeyedMsg[0].Key.(uint64)
					h.bidHashMu.RLock()
					parTmp, ok := h.bidHash.Get(k)
					h.bidHashMu.RUnlock()
					if !ok {
						fmt.Fprintf(os.Stderr, "[ERROR] fail to get output partition")
						errChan <- xerrors.New("fail to get output partition")
						return
					}
					par := parTmp.(uint8)
					err = args.trackParFunc(ctx, k, args.Sinks()[1].KeySerde(), args.Sinks()[1].TopicName(), par)
					if err != nil {
						fmt.Fprintf(os.Stderr, "[ERROR] add topic partition failed: %v", err)
						errChan <- fmt.Errorf("add topic partition failed: %v", err)
						return
					}
					err = args.Sinks()[1].Produce(ctx, changeKeyedMsg[0], par, false)
					if err != nil {
						fmt.Fprintf(os.Stderr, "[ERROR] sink err: %v", err)
						errChan <- fmt.Errorf("sink err: %v", err)
						return
					}
				}
			}
		}
	}
}
