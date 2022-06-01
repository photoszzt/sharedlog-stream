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
	"sharedlog-stream/pkg/execution"
	"sharedlog-stream/pkg/hash"
	"sharedlog-stream/pkg/proc_interface"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/source_sink"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/transaction"
	"sync"
	"time"

	"cs.utexas.edu/zjia/faas/types"
	"golang.org/x/xerrors"
)

type q8GroupByHandler struct {
	env types.Environment

	aucHashMu sync.RWMutex
	aucHash   *hash.ConsistentHash

	personHashMu sync.RWMutex
	personHash   *hash.ConsistentHash

	funcName string
}

func NewQ8GroupByHandler(env types.Environment, funcName string) types.FuncHandler {
	return &q8GroupByHandler{
		env:        env,
		aucHash:    hash.NewConsistentHash(),
		personHash: hash.NewConsistentHash(),
		funcName:   funcName,
	}
}

func (h *q8GroupByHandler) procMsg(ctx context.Context, msg commtypes.Message, argsTmp interface{}) error {
	args := argsTmp.(*TwoMsgChanProcArgs)
	args.msgChan1 <- msg
	args.msgChan2 <- msg
	return nil
}

func (h *q8GroupByHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &common.QueryInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output := h.Q8GroupBy(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	return utils.CompressData(encodedOutput), nil
}

func (h *q8GroupByHandler) getSrcSink(ctx context.Context, sp *common.QueryInput,
	input_stream *sharedlog_stream.ShardedSharedLogStream,
	output_streams []*sharedlog_stream.ShardedSharedLogStream,
) (*source_sink.MeteredSource, []*source_sink.MeteredSyncSink, error) {
	var sinks []*source_sink.MeteredSyncSink
	msgSerde, err := commtypes.GetMsgSerde(sp.SerdeFormat)
	if err != nil {
		return nil, nil, fmt.Errorf("get msg serde err: %v", err)
	}
	eventSerde, err := getEventSerde(sp.SerdeFormat)
	if err != nil {
		return nil, nil, fmt.Errorf("get event serde err: %v", err)
	}
	kvmsgSerdes := commtypes.KVMsgSerdes{
		KeySerde: commtypes.StringSerde{},
		ValSerde: eventSerde,
		MsgSerde: msgSerde,
	}
	inConfig := &source_sink.StreamSourceConfig{
		Timeout:     common.SrcConsumeTimeout,
		KVMsgSerdes: kvmsgSerdes,
	}
	outConfig := &source_sink.StreamSinkConfig{
		KVMsgSerdes: commtypes.KVMsgSerdes{
			KeySerde: commtypes.Uint64Serde{},
			ValSerde: eventSerde,
			MsgSerde: msgSerde,
		},
		FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
	}
	// fmt.Fprintf(os.Stderr, "output to %v\n", output_stream.TopicName())
	src := source_sink.NewMeteredSource(source_sink.NewShardedSharedLogStreamSource(input_stream, inConfig), time.Duration(sp.WarmupS)*time.Second)
	src.SetInitialSource(true)
	for _, output_stream := range output_streams {
		sink := source_sink.NewMeteredSyncSink(source_sink.NewShardedSharedLogStreamSyncSink(output_stream, outConfig), time.Duration(sp.WarmupS)*time.Second)
		sinks = append(sinks, sink)
	}
	return src, sinks, nil
}

func (h *q8GroupByHandler) Q8GroupBy(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	input_stream, output_streams, err := benchutil.GetShardedInputOutputStreams(ctx, h.env, sp)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("get input output stream failed: %v", err),
		}
	}
	src, sinks, err := h.getSrcSink(ctx, sp, input_stream, output_streams)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}

	filterPerson, personsByIDMap, personsByIDFunc := h.getPersonsByID(time.Duration(sp.WarmupS)*time.Second,
		time.Duration(sp.FlushMs)*time.Millisecond)
	filterAuctions, auctionsBySellerIDMap, auctionsBySellerIDFunc := h.getAucBySellerID(time.Duration(sp.WarmupS)*time.Second,
		time.Duration(sp.FlushMs)*time.Millisecond)

	var wg sync.WaitGroup
	personsByIDManager := execution.NewGeneralProcManager(personsByIDFunc)
	auctionsBySellerIDManager := execution.NewGeneralProcManager(auctionsBySellerIDFunc)
	sinks_arr := []source_sink.Sink{sinks[0], sinks[1]}
	procArgs := &TwoMsgChanProcArgs{
		msgChan1:                auctionsBySellerIDManager.MsgChan(),
		msgChan2:                personsByIDManager.MsgChan(),
		BaseProcArgsWithSrcSink: proc_interface.NewBaseProcArgsWithSrcSink(src, sinks_arr, h.funcName, sp.ScaleEpoch, sp.ParNum),
	}

	personsByIDManager.LaunchProc(ctx, procArgs, &wg)
	auctionsBySellerIDManager.LaunchProc(ctx, procArgs, &wg)

	handleErrFunc := func() error {
		select {
		case perErr := <-personsByIDManager.ErrChan():
			return perErr
		case aucErr := <-auctionsBySellerIDManager.ErrChan():
			return aucErr
		default:
		}
		return nil
	}

	task := transaction.StreamTask{
		ProcessFunc: func(ctx context.Context, task *transaction.StreamTask, argsTmp interface{}) *common.FnOutput {
			args := argsTmp.(proc_interface.ProcArgsWithSrcSink)
			return execution.CommonProcess(ctx, task, args, h.procMsg)
		},
		HandleErrFunc:             handleErrFunc,
		CurrentOffset:             make(map[string]uint64),
		CommitEveryForAtLeastOnce: common.CommitDuration,
		PauseFunc: func() *common.FnOutput {
			// debug.Fprintf(os.Stderr, "begin flush\n")
			personsByIDManager.RequestToTerminate()
			auctionsBySellerIDManager.RequestToTerminate()
			wg.Wait()
			if err := handleErrFunc(); err != nil {
				return &common.FnOutput{Success: false, Message: err.Error()}
			}
			// debug.Fprintf(os.Stderr, "done flush\n")
			return nil
		},
		ResumeFunc: func(task *transaction.StreamTask) {
			// debug.Fprintf(os.Stderr, "begin resume\n")
			personsByIDManager.RecreateMsgChan(&procArgs.msgChan2)
			auctionsBySellerIDManager.RecreateMsgChan(&procArgs.msgChan1)
			personsByIDManager.LaunchProc(ctx, procArgs, &wg)
			auctionsBySellerIDManager.LaunchProc(ctx, procArgs, &wg)
			// debug.Fprintf(os.Stderr, "done resume\n")
		},
		InitFunc: func(progArgs interface{}) {
			sinks[0].StartWarmup()
			sinks[1].StartWarmup()
			src.StartWarmup()
			filterPerson.StartWarmup()
			personsByIDMap.StartWarmup()
			filterAuctions.StartWarmup()
			auctionsBySellerIDMap.StartWarmup()
		},
	}
	control_channel.SetupConsistentHash(&h.aucHashMu, h.aucHash, sp.NumOutPartitions[0])
	control_channel.SetupConsistentHash(&h.personHashMu, h.personHash, sp.NumOutPartitions[1])

	srcs := []source_sink.Source{src}

	update_stats := func(ret *common.FnOutput) {
		ret.Latencies["filterPerson"] = filterPerson.GetLatency()
		ret.Latencies["personsByIDMap"] = personsByIDMap.GetLatency()
		ret.Latencies["filterAuctions"] = filterAuctions.GetLatency()
		ret.Latencies["auctionsBySellerIDMap"] = auctionsBySellerIDMap.GetLatency()
		ret.Counts["src"] = src.GetCount()
		ret.Counts["aucSink"] = sinks[0].GetCount()
		ret.Counts["personSink"] = sinks[1].GetCount()
	}
	if sp.EnableTransaction {
		transactionalID := fmt.Sprintf("%s-%s-%d",
			h.funcName, sp.InputTopicNames[0], sp.ParNum)
		streamTaskArgs := transaction.NewStreamTaskArgsTransaction(h.env, transactionalID, procArgs, srcs, sinks_arr)
		benchutil.UpdateStreamTaskArgsTransaction(sp, streamTaskArgs)
		ret := transaction.SetupManagersAndProcessTransactional(ctx, h.env, streamTaskArgs, &task)
		if ret != nil && ret.Success {
			update_stats(ret)
		}
		return ret
	}
	streamTaskArgs := transaction.NewStreamTaskArgs(h.env, procArgs, srcs, sinks_arr)
	benchutil.UpdateStreamTaskArgs(sp, streamTaskArgs)
	ret := task.Process(ctx, streamTaskArgs)
	if ret != nil && ret.Success {
		update_stats(ret)
	}
	return ret
}

func (h *q8GroupByHandler) getPersonsByID(warmup time.Duration, pollTimeout time.Duration) (
	*processor.MeteredProcessor,
	*processor.MeteredProcessor,
	execution.GeneralProcFunc,
) {
	filterPerson := processor.NewMeteredProcessor(processor.NewStreamFilterProcessor(processor.PredicateFunc(
		func(msg *commtypes.Message) (bool, error) {
			event := msg.Value.(*ntypes.Event)
			return event.Etype == ntypes.PERSON, nil
		})), warmup)
	personsByIDMap := processor.NewMeteredProcessor(processor.NewStreamMapProcessor(
		processor.MapperFunc(func(msg commtypes.Message) (commtypes.Message, error) {
			event := msg.Value.(*ntypes.Event)
			return commtypes.Message{
				Key:       event.NewPerson.ID,
				Value:     msg.Value,
				Timestamp: msg.Timestamp,
			}, nil
		})), warmup)

	return filterPerson, personsByIDMap,
		func(ctx context.Context, argsTmp interface{}, wg *sync.WaitGroup,
			msgChan chan commtypes.Message, errChan chan error,
		) {
			args := argsTmp.(*TwoMsgChanProcArgs)
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case msg, ok := <-msgChan:
					if !ok {
						return
					}
					event := msg.Value.(*ntypes.Event)
					ts, err := event.ExtractEventTime()
					if err != nil {
						fmt.Fprintf(os.Stderr, "[ERROR] fail to extract timestamp: %v\n", err)
						errChan <- fmt.Errorf("fail to extract timestamp: %v", err)
						return
					}
					msg.Timestamp = ts
					filteredMsgs, err := filterPerson.ProcessAndReturn(ctx, msg)
					if err != nil {
						fmt.Fprintf(os.Stderr, "[ERROR] filterPerson err: %v\n", err)
						errChan <- fmt.Errorf("filterPerson err: %v", err)
						return
					}
					for _, filteredMsg := range filteredMsgs {
						changeKeyedMsg, err := personsByIDMap.ProcessAndReturn(ctx, filteredMsg)
						if err != nil {
							fmt.Fprintf(os.Stderr, "[ERROR] personsByIDMap err: %v\n", err)
							errChan <- fmt.Errorf("personsByIDMap err: %v", err)
							return
						}

						k := changeKeyedMsg[0].Key.(uint64)
						h.personHashMu.RLock()
						parTmp, ok := h.personHash.Get(k)
						h.personHashMu.RUnlock()
						if !ok {
							fmt.Fprintf(os.Stderr, "[ERROR] fail to get output partition\n")
							errChan <- xerrors.New("fail to get output partition")
							return
						}
						par := parTmp.(uint8)
						err = args.TrackParFunc()(ctx, k, args.Sinks()[1].KeySerde(), args.Sinks()[1].TopicName(), par)
						if err != nil {
							fmt.Fprintf(os.Stderr, "[ERROR] add topic partition failed: %v\n", err)
							errChan <- fmt.Errorf("add topic partition failed: %v", err)
							return
						}
						// fmt.Fprintf(os.Stderr, "append %v to substream %v\n", changeKeyedMsg[0], par)
						err = args.Sinks()[1].Produce(ctx, changeKeyedMsg[0], par, false)
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

func (h *q8GroupByHandler) getAucBySellerID(warmup time.Duration, pollTimeout time.Duration) (*processor.MeteredProcessor, *processor.MeteredProcessor,
	execution.GeneralProcFunc,
) {
	filterAuctions := processor.NewMeteredProcessor(processor.NewStreamFilterProcessor(processor.PredicateFunc(
		func(m *commtypes.Message) (bool, error) {
			event := m.Value.(*ntypes.Event)
			return event.Etype == ntypes.AUCTION, nil
		})), warmup)

	auctionsBySellerIDMap := processor.NewMeteredProcessor(processor.NewStreamMapProcessor(
		processor.MapperFunc(func(msg commtypes.Message) (commtypes.Message, error) {
			event := msg.Value.(*ntypes.Event)
			return commtypes.Message{Key: event.NewAuction.Seller, Value: msg.Value, Timestamp: msg.Timestamp}, nil
		})), warmup)

	return filterAuctions, auctionsBySellerIDMap, func(ctx context.Context, argsTmp interface{}, wg *sync.WaitGroup, msgChan chan commtypes.Message, errChan chan error) {
		args := argsTmp.(*TwoMsgChanProcArgs)
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-msgChan:
				if !ok {
					return
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
					fmt.Fprintf(os.Stderr, "[ERROR] filterAuctions err: %v\n", err)
					errChan <- fmt.Errorf("filterAuctions err: %v", err)
					return
				}
				for _, filteredMsg := range filteredMsgs {
					changeKeyedMsg, err := auctionsBySellerIDMap.ProcessAndReturn(ctx, filteredMsg)
					if err != nil {
						fmt.Fprintf(os.Stderr, "[ERROR] auctionsBySellerIDMap err: %v\n", err)
						errChan <- fmt.Errorf("auctionsBySellerIDMap err: %v", err)
					}

					k := changeKeyedMsg[0].Key.(uint64)
					h.aucHashMu.RLock()
					parTmp, ok := h.aucHash.Get(k)
					h.aucHashMu.RUnlock()
					if !ok {
						fmt.Fprintf(os.Stderr, "[ERROR] fail to get output partition\n")
						errChan <- xerrors.New("fail to get output partition")
					}
					par := parTmp.(uint8)
					err = args.TrackParFunc()(ctx, k, args.Sinks()[0].KeySerde(), args.Sinks()[0].TopicName(), par)
					if err != nil {
						fmt.Fprintf(os.Stderr, "[ERROR] add topic partition failed: %v", err)
						errChan <- fmt.Errorf("add topic partition failed: %v", err)
						return
					}
					// fmt.Fprintf(os.Stderr, "append %v to substream %v\n", changeKeyedMsg[0], par)
					err = args.Sinks()[0].Produce(ctx, changeKeyedMsg[0], par, false)
					if err != nil {
						fmt.Fprintf(os.Stderr, "[ERROR] sink err: %v\n", err)
						errChan <- fmt.Errorf("sink err: %v", err)
					}
				}
			}
		}
	}
}
