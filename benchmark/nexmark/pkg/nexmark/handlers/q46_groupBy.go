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
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/hash"
	"sharedlog-stream/pkg/source_sink"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"
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
	args := argsTmp.(*q46GroupByProcessArgs)
	return transaction.CommonProcess(ctx, t, args, func(t *transaction.StreamTask, msg commtypes.MsgAndSeq) error {
		t.CurrentOffset[args.src.TopicName()] = msg.LogSeqNum
		if msg.MsgArr != nil {
			for _, subMsg := range msg.MsgArr {
				if subMsg.Value == nil {
					continue
				}
				args.aucMsgChan <- subMsg
				args.bidMsgChan <- subMsg
			}
			return nil
		}
		args.aucMsgChan <- msg.Msg
		args.bidMsgChan <- msg.Msg
		return nil
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

type q46GroupByProcessArgs struct {
	aucMsgChan chan commtypes.Message
	bidMsgChan chan commtypes.Message
	errChan    chan error

	recordFinishFunc tran_interface.RecordPrevInstanceFinishFunc
	trackParFunc     tran_interface.TrackKeySubStreamFunc
	src              *source_sink.MeteredSource

	funcName string
	sinks    []*source_sink.MeteredSyncSink
	curEpoch uint64
	parNum   uint8
}

func (a *q46GroupByProcessArgs) Source() source_sink.Source { return a.src }
func (a *q46GroupByProcessArgs) PushToAllSinks(ctx context.Context, msg commtypes.Message, parNum uint8, isControl bool) error {
	for _, sink := range a.sinks {
		err := sink.Produce(ctx, msg, parNum, isControl)
		if err != nil {
			return err
		}
	}
	return nil
}
func (a *q46GroupByProcessArgs) ParNum() uint8    { return a.parNum }
func (a *q46GroupByProcessArgs) CurEpoch() uint64 { return a.curEpoch }
func (a *q46GroupByProcessArgs) FuncName() string { return a.funcName }
func (a *q46GroupByProcessArgs) RecordFinishFunc() tran_interface.RecordPrevInstanceFinishFunc {
	return a.recordFinishFunc
}
func (a *q46GroupByProcessArgs) ErrChan() chan error {
	return a.errChan
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

	errChan := make(chan error, 2)
	aucMsgChan := make(chan commtypes.Message, 1)
	bidMsgChan := make(chan commtypes.Message, 1)
	procArgs := &q46GroupByProcessArgs{
		src:              src,
		sinks:            sinks,
		aucMsgChan:       aucMsgChan,
		bidMsgChan:       bidMsgChan,
		trackParFunc:     tran_interface.DefaultTrackSubstreamFunc,
		recordFinishFunc: transaction.DefaultRecordPrevInstanceFinishFunc,
		funcName:         h.funcName,
		curEpoch:         sp.ScaleEpoch,
		parNum:           sp.ParNum,
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go auctionsByIDFunc(ctx, procArgs, &wg, aucMsgChan, errChan)
	wg.Add(1)
	go bidsByAuctionIDFunc(ctx, procArgs, &wg, bidMsgChan, errChan)

	task := transaction.StreamTask{
		ProcessFunc: h.process,
		PauseFunc: func() *common.FnOutput {
			// debug.Fprintf(os.Stderr, "begin flush\n")
			close(aucMsgChan)
			close(bidMsgChan)
			wg.Wait()
			// debug.Fprintf(os.Stderr, "done flush\n")
			return nil
		},
		ResumeFunc: func(task *transaction.StreamTask) {
			debug.Fprintf(os.Stderr, "start resume\n")
			aucMsgChan = make(chan commtypes.Message, 1)
			bidMsgChan = make(chan commtypes.Message, 1)
			procArgs.aucMsgChan = aucMsgChan
			procArgs.bidMsgChan = bidMsgChan
			wg.Add(1)
			go auctionsByIDFunc(ctx, procArgs, &wg, aucMsgChan, errChan)
			wg.Add(1)
			go bidsByAuctionIDFunc(ctx, procArgs, &wg, bidMsgChan, errChan)
			debug.Fprintf(os.Stderr, "done resume\n")
		},
		InitFunc: func(progArgsTmp interface{}) {
			progArgs := progArgsTmp.(*q3GroupByProcessArgs)
			progArgs.src.StartWarmup()
			progArgs.sinks[0].StartWarmup()
			progArgs.sinks[1].StartWarmup()
			filterAuctions.StartWarmup()
			auctionsByIDMap.StartWarmup()
			filterBids.StartWarmup()
			bidsByAuctionIDMap.StartWarmup()
			// debug.Fprintf(os.Stderr, "done warmup start\n")
		},
		CurrentOffset:             make(map[string]uint64),
		CommitEveryForAtLeastOnce: common.CommitDuration,
	}
	transaction.SetupConsistentHash(&h.aucHashMu, h.aucHash, sp.NumOutPartitions[0])
	transaction.SetupConsistentHash(&h.bidHashMu, h.bidHash, sp.NumOutPartitions[1])
	srcs := []source_sink.Source{src}
	sinks_arr := []source_sink.Sink{sinks[0], sinks[1]}
	if sp.EnableTransaction {
		transactionalID := fmt.Sprintf("%s-%s-%d", h.funcName, sp.InputTopicNames[0], sp.ParNum)
		streamTaskArgs := transaction.NewStreamTaskArgsTransaction(h.env, transactionalID, procArgs, srcs, sinks_arr)
		benchutil.UpdateStreamTaskArgsTransaction(sp, streamTaskArgs)
		ret := transaction.SetupManagersAndProcessTransactional(ctx, h.env, streamTaskArgs,
			func(procArgs interface{}, trackParFunc tran_interface.TrackKeySubStreamFunc,
				recordFinishFunc tran_interface.RecordPrevInstanceFinishFunc) {
				procArgs.(*q46GroupByProcessArgs).trackParFunc = trackParFunc
				procArgs.(*q46GroupByProcessArgs).recordFinishFunc = recordFinishFunc
			}, &task)
		if ret != nil && ret.Success {
			ret.Latencies["src"] = src.GetLatency()
			ret.Latencies["aucSink"] = sinks[0].GetLatency()
			ret.Latencies["bidSink"] = sinks[1].GetLatency()
			ret.Latencies["filterAuctions"] = filterAuctions.GetLatency()
			ret.Latencies["auctionsByIDMap"] = auctionsByIDMap.GetLatency()
			ret.Latencies["filterBids"] = filterBids.GetLatency()
			ret.Latencies["bidsByAuctionIDMap"] = bidsByAuctionIDMap.GetLatency()
			ret.Consumed["src"] = src.GetCount()
		}
		return ret
	} else {
		streamTaskArgs := transaction.NewStreamTaskArgs(h.env, procArgs, srcs, sinks_arr)
		benchutil.UpdateStreamTaskArgs(sp, streamTaskArgs)
		ret := task.Process(ctx, streamTaskArgs)
		if ret != nil && ret.Success {
			ret.Latencies["src"] = src.GetLatency()
			ret.Latencies["aucSink"] = sinks[0].GetLatency()
			ret.Latencies["bidSink"] = sinks[1].GetLatency()
			ret.Latencies["filterAuctions"] = filterAuctions.GetLatency()
			ret.Latencies["auctionsByIDMap"] = auctionsByIDMap.GetLatency()
			ret.Latencies["filterBids"] = filterBids.GetLatency()
			ret.Latencies["bidsByAuctionIDMap"] = bidsByAuctionIDMap.GetLatency()
			ret.Consumed["src"] = src.GetCount()
		}
		return ret
	}
}

func (h *q46GroupByHandler) getAucsByID(warmup time.Duration) (*processor.MeteredProcessor, *processor.MeteredProcessor,
	func(ctx context.Context, args *q46GroupByProcessArgs, wg *sync.WaitGroup, msgChan chan commtypes.Message, errChan chan error),
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

	return filterAuctions, auctionsByIDMap, func(ctx context.Context, args *q46GroupByProcessArgs, wg *sync.WaitGroup, msgChan chan commtypes.Message, errChan chan error) {
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
				ts, err := event.ExtractStreamTime()
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
					err = args.trackParFunc(ctx, k, args.sinks[0].KeySerde(), args.sinks[0].TopicName(), par)
					if err != nil {
						fmt.Fprintf(os.Stderr, "[ERROR] add topic partition failed: %v\n", err)
						errChan <- fmt.Errorf("add topic partition failed: %v", err)
						return
					}
					err = args.sinks[0].Produce(ctx, changeKeyedMsg[0], par, false)
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

func (h *q46GroupByHandler) getBidsByAuctionID(warmup time.Duration) (*processor.MeteredProcessor, *processor.MeteredProcessor,
	func(ctx context.Context, args *q46GroupByProcessArgs, wg *sync.WaitGroup, msgChan chan commtypes.Message, errChan chan error),
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
	return filterBids, bidsByAuctionIDMap, func(ctx context.Context, args *q46GroupByProcessArgs, wg *sync.WaitGroup, msgChan chan commtypes.Message, errChan chan error) {
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
				ts, err := event.ExtractStreamTime()
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
					err = args.trackParFunc(ctx, k, args.sinks[1].KeySerde(), args.sinks[1].TopicName(), par)
					if err != nil {
						fmt.Fprintf(os.Stderr, "[ERROR] add topic partition failed: %v", err)
						errChan <- fmt.Errorf("add topic partition failed: %v", err)
						return
					}
					err = args.sinks[1].Produce(ctx, changeKeyedMsg[0], par, false)
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
