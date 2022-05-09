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
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/transaction"
	"sync"
	"sync/atomic"
	"time"

	"cs.utexas.edu/zjia/faas/types"
	"golang.org/x/xerrors"
)

type q3GroupByHandler struct {
	env types.Environment

	aucHashMu sync.RWMutex
	aucHash   *hash.ConsistentHash

	personHashMu sync.RWMutex
	personHash   *hash.ConsistentHash

	funcName string
}

func NewQ3GroupByHandler(env types.Environment, funcName string) types.FuncHandler {
	return &q3GroupByHandler{
		env:        env,
		aucHash:    hash.NewConsistentHash(),
		personHash: hash.NewConsistentHash(),
		funcName:   funcName,
	}
}

func (h *q3GroupByHandler) process(
	ctx context.Context,
	t *transaction.StreamTask,
	argsTmp interface{},
) (map[string]uint64, *common.FnOutput) {
	args := argsTmp.(*q3GroupByProcessArgs)
	return transaction.CommonProcess(ctx, t, args, func(t *transaction.StreamTask, msg commtypes.MsgAndSeq) error {
		t.CurrentOffset[args.src.TopicName()] = msg.LogSeqNum
		if msg.MsgArr != nil {
			for _, subMsg := range msg.MsgArr {
				if subMsg.Value == nil {
					continue
				}
				args.aucMsgChan <- subMsg
				args.personMsgChan <- subMsg
			}
			return nil
		}
		args.aucMsgChan <- msg.Msg
		args.personMsgChan <- msg.Msg
		return nil
	})
}

func (h *q3GroupByHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &common.QueryInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output := h.Q3GroupBy(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	return utils.CompressData(encodedOutput), nil
}

type q3GroupByProcessArgs struct {
	errChan          chan error
	aucMsgChan       chan commtypes.Message
	personMsgChan    chan commtypes.Message
	recordFinishFunc transaction.RecordPrevInstanceFinishFunc
	trackParFunc     transaction.TrackKeySubStreamFunc
	src              *processor.MeteredSource
	funcName         string
	sinks            []*sharedlog_stream.MeteredSink
	curEpoch         uint64
	personAck        int32
	aucAck           int32
	personCtrl       uint32
	aucCtrl          uint32
	personDone       uint32
	aucDone          uint32
	parNum           uint8
}

func (a *q3GroupByProcessArgs) Source() processor.Source { return a.src }
func (a *q3GroupByProcessArgs) PushToAllSinks(ctx context.Context, msg commtypes.Message, parNum uint8, isControl bool) error {
	for _, sink := range a.sinks {
		err := sink.Sink(ctx, msg, parNum, isControl)
		if err != nil {
			return err
		}
	}
	return nil
}
func (a *q3GroupByProcessArgs) ParNum() uint8    { return a.parNum }
func (a *q3GroupByProcessArgs) CurEpoch() uint64 { return a.curEpoch }
func (a *q3GroupByProcessArgs) FuncName() string { return a.funcName }
func (a *q3GroupByProcessArgs) RecordFinishFunc() func(ctx context.Context, funcName string, instanceId uint8) error {
	return a.recordFinishFunc
}
func (a *q3GroupByProcessArgs) ErrChan() chan error {
	return a.errChan
}

func (h *q3GroupByHandler) getSrcSink(ctx context.Context, sp *common.QueryInput,
	input_stream *sharedlog_stream.ShardedSharedLogStream,
	output_streams []*sharedlog_stream.ShardedSharedLogStream,
) (*processor.MeteredSource, []*sharedlog_stream.MeteredSink, commtypes.MsgSerde, error) {
	var sinks []*sharedlog_stream.MeteredSink
	msgSerde, err := commtypes.GetMsgSerde(sp.SerdeFormat)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("get msg serde err: %v", err)
	}
	eventSerde, err := getEventSerde(sp.SerdeFormat)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("get event serde err: %v", err)
	}
	inConfig := &sharedlog_stream.StreamSourceConfig{
		Timeout:      common.SrcConsumeTimeout,
		KeyDecoder:   commtypes.StringDecoder{},
		ValueDecoder: eventSerde,
		MsgDecoder:   msgSerde,
	}
	outConfig := &sharedlog_stream.StreamSinkConfig{
		KeySerde:      commtypes.Uint64Serde{},
		ValueSerde:    eventSerde,
		MsgSerde:      msgSerde,
		FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
	}
	// fmt.Fprintf(os.Stderr, "output to %v\n", output_stream.TopicName())
	src := processor.NewMeteredSource(sharedlog_stream.NewShardedSharedLogStreamSource(input_stream, inConfig),
		time.Duration(sp.WarmupS)*time.Second)
	for _, output_stream := range output_streams {
		sink := sharedlog_stream.NewMeteredSink(sharedlog_stream.NewShardedSharedLogStreamSink(output_stream, outConfig),
			time.Duration(sp.WarmupS)*time.Second)
		sinks = append(sinks, sink)
	}
	return src, sinks, msgSerde, nil
}

func (h *q3GroupByHandler) Q3GroupBy(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	input_stream, output_streams, err := benchutil.GetShardedInputOutputStreams(ctx, h.env, sp, false)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("get input output stream failed: %v", err),
		}
	}
	debug.Assert(len(output_streams) == 2, "expected 2 output streams")
	src, sinks, msgSerde, err := h.getSrcSink(ctx, sp, input_stream, output_streams)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}

	filterPerson, personsByIDMap, personsByIDFunc := h.getPersonsByID(time.Duration(sp.WarmupS) * time.Second)
	filterAuctions, auctionsBySellerIDMap, auctionsBySellerIDFunc := h.getAucBySellerID(time.Duration(sp.WarmupS) * time.Second)

	errChan := make(chan error, 1)
	aucMsgChan := make(chan commtypes.Message, 100000)
	personMsgChan := make(chan commtypes.Message, 10000)
	procArgs := &q3GroupByProcessArgs{
		src:              src,
		sinks:            sinks,
		aucMsgChan:       aucMsgChan,
		personMsgChan:    personMsgChan,
		aucCtrl:          uint32(Paused),
		personCtrl:       uint32(Paused),
		trackParFunc:     transaction.DefaultTrackSubstreamFunc,
		recordFinishFunc: transaction.DefaultRecordPrevInstanceFinishFunc,
		funcName:         h.funcName,
		curEpoch:         sp.ScaleEpoch,
		parNum:           sp.ParNum,
		aucAck:           0,
		personAck:        0,
		aucDone:          0,
		personDone:       0,
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go personsByIDFunc(ctx, procArgs, &wg, personMsgChan, errChan)
	wg.Add(1)
	go auctionsBySellerIDFunc(ctx, procArgs, &wg, aucMsgChan, errChan)

	task := transaction.StreamTask{
		ProcessFunc: h.process,
		CloseFunc: func() {
			// fmt.Fprintf(os.Stderr, "auction channel has %d msg left\n", len(aucMsgChan))
			// fmt.Fprintf(os.Stderr, "person channel has %d msg left\n", len(personMsgChan))
			close(aucMsgChan)
			close(personMsgChan)
			wg.Wait()
			sinks[0].CloseAsyncPush()
			sinks[1].CloseAsyncPush()
			sinks[0].Flush(ctx)
			sinks[1].Flush(ctx)
		},
		FlushOrPauseFunc: func() {
			atomic.AddInt32(&procArgs.aucAck, 1)
			atomic.AddInt32(&procArgs.personAck, 1)
			atomic.StoreUint32(&procArgs.aucCtrl, uint32(Paused))
			atomic.StoreUint32(&procArgs.personCtrl, uint32(Paused))
			for atomic.LoadInt32(&procArgs.aucAck) != 0 &&
				atomic.LoadInt32(&procArgs.personAck) != 0 {
				if atomic.LoadUint32(&procArgs.personDone) == 1 ||
					atomic.LoadUint32(&procArgs.aucDone) == 1 {
					sinks[0].Flush(ctx)
					sinks[1].Flush(ctx)
					break
				}
				time.Sleep(time.Duration(100) * time.Microsecond)
			}
		},
		ResumeFunc: func() {
			atomic.AddInt32(&procArgs.aucAck, 1)
			atomic.AddInt32(&procArgs.personAck, 1)
			atomic.StoreUint32(&procArgs.aucCtrl, uint32(Running))
			atomic.StoreUint32(&procArgs.personCtrl, uint32(Running))
			for atomic.LoadInt32(&procArgs.aucAck) != 0 &&
				atomic.LoadInt32(&procArgs.personAck) != 0 {
				if atomic.LoadUint32(&procArgs.personDone) == 1 ||
					atomic.LoadUint32(&procArgs.aucDone) == 1 {
					sinks[0].Flush(ctx)
					sinks[1].Flush(ctx)
					break
				}
				time.Sleep(time.Duration(100) * time.Microsecond)
			}
		},
		InitFunc: func(progArgsTmp interface{}) {
			procArgs := progArgsTmp.(*q3GroupByProcessArgs)
			debug.Fprintf(os.Stderr, "start async pusher\n")
			if sp.EnableTransaction {
				sinks[0].InnerSink().StartAsyncPushNoTick(ctx)
				sinks[1].InnerSink().StartAsyncPushNoTick(ctx)
			} else {
				sinks[0].InnerSink().StartAsyncPushWithTick(ctx)
				sinks[1].InnerSink().StartAsyncPushWithTick(ctx)
				sinks[0].InitFlushTimer()
				sinks[1].InitFlushTimer()
			}
			debug.Fprintf(os.Stderr, "done start async pusher\n")

			src.StartWarmup()
			sinks[0].StartWarmup()
			sinks[1].StartWarmup()
			filterPerson.StartWarmup()
			personsByIDMap.StartWarmup()
			filterAuctions.StartWarmup()
			auctionsBySellerIDMap.StartWarmup()
			debug.Fprintf(os.Stderr, "done warmup start\n")

			debug.Fprintf(os.Stderr, "current auc ack is %x\n", procArgs.aucAck)
			debug.Fprintf(os.Stderr, "current person ack is %x\n", procArgs.personAck)
			curAuc := atomic.AddInt32(&procArgs.aucAck, 1)
			curPer := atomic.AddInt32(&procArgs.personAck, 1)
			debug.Fprintf(os.Stderr, "current auc ack is %x\n", curAuc)
			debug.Fprintf(os.Stderr, "current person ack is %x\n", curPer)
			atomic.StoreUint32(&procArgs.aucCtrl, uint32(Running))
			atomic.StoreUint32(&procArgs.personCtrl, uint32(Running))
			debug.Fprintf(os.Stderr, "waiting for ack\n")
			for {
				pPer := atomic.LoadInt32(&procArgs.personAck)
				pAuc := atomic.LoadInt32(&procArgs.aucAck)
				debug.Fprintf(os.Stderr, "per ack: %x, auc ack: %x\n", pPer, pAuc)
				if pPer == 0 && pAuc == 0 {
					break
				}
				time.Sleep(time.Duration(100) * time.Microsecond)
			}
			debug.Fprintf(os.Stderr, "down waiting for ack\n")
		},
		CurrentOffset: make(map[string]uint64),
		CommitEvery:   common.CommitDuration,
	}
	transaction.SetupConsistentHash(&h.aucHashMu, h.aucHash, sp.NumOutPartitions[0])
	transaction.SetupConsistentHash(&h.personHashMu, h.personHash, sp.NumOutPartitions[1])
	srcs := map[string]processor.Source{sp.InputTopicNames[0]: src}
	if sp.EnableTransaction {
		streamTaskArgs := transaction.StreamTaskArgsTransaction{
			ProcArgs:      procArgs,
			Env:           h.env,
			MsgSerde:      msgSerde,
			Srcs:          srcs,
			OutputStreams: output_streams,
			QueryInput:    sp,
			TransactionalId: fmt.Sprintf("%s-%s-%d",
				h.funcName, sp.InputTopicNames[0], sp.ParNum),
			KVChangelogs:          nil,
			WindowStoreChangelogs: nil,
			FixedOutParNum:        0,
		}
		ret := transaction.SetupManagersAndProcessTransactional(ctx, h.env, &streamTaskArgs,
			func(procArgs interface{}, trackParFunc transaction.TrackKeySubStreamFunc,
				recordFinishFunc transaction.RecordPrevInstanceFinishFunc) {
				procArgs.(*q3GroupByProcessArgs).trackParFunc = trackParFunc
				procArgs.(*q3GroupByProcessArgs).recordFinishFunc = recordFinishFunc
			}, &task)
		if ret != nil && ret.Success {
			ret.Latencies["src"] = src.GetLatency()
			ret.Latencies["aucSink"] = sinks[0].GetLatency()
			ret.Latencies["personSink"] = sinks[1].GetLatency()
			ret.Latencies["filterPerson"] = filterPerson.GetLatency()
			ret.Latencies["personsByIDMap"] = personsByIDMap.GetLatency()
			ret.Latencies["filterAuctions"] = filterAuctions.GetLatency()
			ret.Latencies["auctionsBySellerIDMap"] = auctionsBySellerIDMap.GetLatency()
			ret.Consumed["src"] = src.GetCount()
		}
		return ret
	}
	streamTaskArgs := transaction.StreamTaskArgs{
		ProcArgs:       procArgs,
		Duration:       time.Duration(sp.Duration) * time.Second,
		Srcs:           srcs,
		ParNum:         sp.ParNum,
		SerdeFormat:    commtypes.SerdeFormat(sp.SerdeFormat),
		Env:            h.env,
		NumInPartition: sp.NumInPartition,
		WarmupTime:     time.Duration(sp.WarmupS) * time.Second,
	}
	ret := task.Process(ctx, &streamTaskArgs)
	if ret != nil && ret.Success {
		ret.Latencies["src"] = src.GetLatency()
		ret.Latencies["aucSink"] = sinks[0].GetLatency()
		ret.Latencies["personSink"] = sinks[1].GetLatency()
		ret.Latencies["filterPerson"] = filterPerson.GetLatency()
		ret.Latencies["personsByIDMap"] = personsByIDMap.GetLatency()
		ret.Latencies["filterAuctions"] = filterAuctions.GetLatency()
		ret.Latencies["auctionsBySellerIDMap"] = auctionsBySellerIDMap.GetLatency()
		ret.Consumed["src"] = src.GetCount()
	}
	return ret
}

func (h *q3GroupByHandler) getPersonsByID(warmup time.Duration) (*processor.MeteredProcessor, *processor.MeteredProcessor,
	func(ctx context.Context, args *q3GroupByProcessArgs, wg *sync.WaitGroup, msgChan chan commtypes.Message, errChan chan error)) {
	filterPerson := processor.NewMeteredProcessor(processor.NewStreamFilterProcessor(processor.PredicateFunc(
		func(msg *commtypes.Message) (bool, error) {
			event := msg.Value.(*ntypes.Event)
			/*
				fmt.Fprintf(os.Stderr, "input event is %v\n", event)
				if event.Etype == ntypes.PERSON {
					fmt.Fprintf(os.Stderr, "person state is %v\n", event.NewPerson.State)
				}
			*/
			return event.Etype == ntypes.PERSON && ((event.NewPerson.State == "OR") ||
				event.NewPerson.State == "ID" || event.NewPerson.State == "CA"), nil
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
	return filterPerson, personsByIDMap, func(ctx context.Context, args *q3GroupByProcessArgs, wg *sync.WaitGroup, msgChan chan commtypes.Message, errChan chan error) {
		defer wg.Done()
		status := Paused
	L:
		for {
			select {
			case <-ctx.Done():
				break L
			default:
			}
			ctrl := atomic.LoadUint32(&args.personCtrl)
			state := RunningState(uint8(ctrl))
			switch state {
			case Paused:
				status = Paused
				args.sinks[1].Flush(ctx)
				atomic.AddInt32(&args.personAck, -1)
			case Running:
				status = Running
				atomic.AddInt32(&args.personAck, -1)
			case Stopped:
				atomic.StoreUint32(&args.personDone, 1)
				atomic.AddInt32(&args.personAck, -1)
				break L
			}
			if status == Running {
				msg, ok := <-msgChan
				if !ok {
					break L
				}
				event := msg.Value.(*ntypes.Event)
				ts, err := event.ExtractStreamTime()
				if err != nil {
					fmt.Fprintf(os.Stderr, "[ERROR] fail to extract timestamp: %v\n", err)
					atomic.StoreUint32(&args.personDone, 1)
					errChan <- fmt.Errorf("fail to extract timestamp: %v", err)
					return
				}
				msg.Timestamp = ts
				filteredMsgs, err := filterPerson.ProcessAndReturn(ctx, msg)
				if err != nil {
					fmt.Fprintf(os.Stderr, "[ERROR] filterPerson err: %v\n", err)
					atomic.StoreUint32(&args.personDone, 1)
					errChan <- fmt.Errorf("filterPerson err: %v", err)
					return
				}
				for _, filteredMsg := range filteredMsgs {
					changeKeyedMsg, err := personsByIDMap.ProcessAndReturn(ctx, filteredMsg)
					if err != nil {
						fmt.Fprintf(os.Stderr, "[ERROR] personsByIDMap err: %v\n", err)
						atomic.StoreUint32(&args.personDone, 1)
						errChan <- fmt.Errorf("personsByIDMap err: %v", err)
						return
					}

					k := changeKeyedMsg[0].Key.(uint64)
					h.personHashMu.RLock()
					parTmp, ok := h.personHash.Get(k)
					h.personHashMu.RUnlock()
					if !ok {
						fmt.Fprintf(os.Stderr, "[ERROR] fail to get output partition\n")
						atomic.StoreUint32(&args.personDone, 1)
						errChan <- xerrors.New("fail to get output partition")
						return
					}
					par := parTmp.(uint8)
					err = args.trackParFunc(ctx, k, args.sinks[1].KeySerde(), args.sinks[1].TopicName(), par)
					if err != nil {
						fmt.Fprintf(os.Stderr, "[ERROR] add topic partition failed: %v\n", err)
						atomic.StoreUint32(&args.personDone, 1)
						errChan <- fmt.Errorf("add topic partition failed: %v", err)
						return
					}
					// fmt.Fprintf(os.Stderr, "append %v to substream %v\n", changeKeyedMsg[0], par)
					err = args.sinks[1].Sink(ctx, changeKeyedMsg[0], par, false)
					if err != nil {
						fmt.Fprintf(os.Stderr, "[ERROR] sink err: %v\n", err)
						atomic.StoreUint32(&args.personDone, 1)
						errChan <- fmt.Errorf("sink err: %v", err)
						return
					}
				}
			}
		}
	}
}

func (h *q3GroupByHandler) getAucBySellerID(warmup time.Duration) (*processor.MeteredProcessor, *processor.MeteredProcessor,
	func(ctx context.Context, args *q3GroupByProcessArgs, wg *sync.WaitGroup, msgChan chan commtypes.Message, errChan chan error),
) {
	filterAuctions := processor.NewMeteredProcessor(processor.NewStreamFilterProcessor(processor.PredicateFunc(
		func(m *commtypes.Message) (bool, error) {
			event := m.Value.(*ntypes.Event)
			return event.Etype == ntypes.AUCTION && event.NewAuction.Category == 10, nil
		})), warmup)

	auctionsBySellerIDMap := processor.NewMeteredProcessor(processor.NewStreamMapProcessor(
		processor.MapperFunc(func(msg commtypes.Message) (commtypes.Message, error) {
			event := msg.Value.(*ntypes.Event)
			return commtypes.Message{Key: event.NewAuction.Seller, Value: msg.Value, Timestamp: msg.Timestamp}, nil
		})), warmup)

	return filterAuctions, auctionsBySellerIDMap, func(ctx context.Context, args *q3GroupByProcessArgs, wg *sync.WaitGroup, msgChan chan commtypes.Message, errChan chan error) {
		defer wg.Done()
		status := Paused
	L:
		for {
			select {
			case <-ctx.Done():
				break L
			default:
			}
			ctrl := atomic.LoadUint32(&args.aucCtrl)
			state := RunningState(uint8(ctrl))
			switch state {
			case Paused:
				status = Paused
				args.sinks[0].Flush(ctx)
				atomic.AddInt32(&args.aucAck, -1)
			case Running:
				status = Running
				atomic.AddInt32(&args.aucAck, -1)
			case Stopped:
				atomic.StoreUint32(&args.aucDone, 1)
				atomic.AddInt32(&args.aucAck, -1)
				break L
			}
			if status == Running {
				msg, ok := <-msgChan
				if !ok {
					break L
				}

				event := msg.Value.(*ntypes.Event)
				ts, err := event.ExtractStreamTime()
				if err != nil {
					fmt.Fprintf(os.Stderr, "[ERROR] fail to extract timestamp: %v\n", err)
					atomic.StoreUint32(&args.aucDone, 1)
					errChan <- fmt.Errorf("fail to extract timestamp: %v", err)
					return
				}
				msg.Timestamp = ts
				filteredMsgs, err := filterAuctions.ProcessAndReturn(ctx, msg)
				if err != nil {
					fmt.Fprintf(os.Stderr, "filterAuctions err: %v\n", err)
					atomic.StoreUint32(&args.aucDone, 1)
					errChan <- fmt.Errorf("filterAuctions err: %v", err)
					return
				}
				for _, filteredMsg := range filteredMsgs {
					changeKeyedMsg, err := auctionsBySellerIDMap.ProcessAndReturn(ctx, filteredMsg)
					if err != nil {
						fmt.Fprintf(os.Stderr, "[ERROR] auctionsBySellerIDMap err: %v\n", err)
						atomic.StoreUint32(&args.aucDone, 1)
						errChan <- fmt.Errorf("auctionsBySellerIDMap err: %v", err)
						return
					}

					k := changeKeyedMsg[0].Key.(uint64)
					h.aucHashMu.RLock()
					parTmp, ok := h.aucHash.Get(k)
					h.aucHashMu.RUnlock()
					if !ok {
						fmt.Fprintf(os.Stderr, "[ERROR] fail to get output partition\n")
						atomic.StoreUint32(&args.aucDone, 1)
						errChan <- xerrors.New("fail to get output partition")
						return
					}
					par := parTmp.(uint8)
					err = args.trackParFunc(ctx, k, args.sinks[0].KeySerde(), args.sinks[0].TopicName(), par)
					if err != nil {
						fmt.Fprintf(os.Stderr, "[ERROR] add topic partition failed: %v", err)
						atomic.StoreUint32(&args.aucDone, 1)
						errChan <- fmt.Errorf("add topic partition failed: %v", err)
						return
					}
					// fmt.Fprintf(os.Stderr, "append %v to substream %v\n", changeKeyedMsg[0], par)
					err = args.sinks[0].Sink(ctx, changeKeyedMsg[0], par, false)
					if err != nil {
						fmt.Fprintf(os.Stderr, "[ERROR] sink err: %v\n", err)
						atomic.StoreUint32(&args.aucDone, 1)
						errChan <- fmt.Errorf("sink err: %v", err)
						return
					}
				}
			}
		}
	}
}
