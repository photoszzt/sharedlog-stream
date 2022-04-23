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
	"sharedlog-stream/pkg/hash"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/transaction"
	"sync"
	"time"

	"cs.utexas.edu/zjia/faas/types"
	"golang.org/x/xerrors"
)

type q8GroupByHandler struct {
	env      types.Environment
	cHashMu  sync.RWMutex
	cHash    *hash.ConsistentHash
	funcName string
}

func NewQ8GroupByHandler(env types.Environment, funcName string) types.FuncHandler {
	return &q8GroupByHandler{
		env:      env,
		cHash:    hash.NewConsistentHash(),
		funcName: funcName,
	}
}

func (h *q8GroupByHandler) process(
	ctx context.Context,
	t *transaction.StreamTask,
	argsTmp interface{},
) (map[string]uint64, *common.FnOutput) {
	args := argsTmp.(*q8GroupByProcessArgs)
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

type q8GroupByProcessArgs struct {
	src              *processor.MeteredSource
	sinks            []*processor.MeteredSink
	aucMsgChan       chan commtypes.Message
	personMsgChan    chan commtypes.Message
	errChan          chan error
	trackParFunc     transaction.TrackKeySubStreamFunc
	recordFinishFunc transaction.RecordPrevInstanceFinishFunc
	funcName         string
	curEpoch         uint64
	parNum           uint8
}

func (a *q8GroupByProcessArgs) Source() processor.Source { return a.src }
func (a *q8GroupByProcessArgs) PushToAllSinks(ctx context.Context, msg commtypes.Message, parNum uint8, isControl bool) error {
	for _, sink := range a.sinks {
		err := sink.Sink(ctx, msg, parNum, isControl)
		if err != nil {
			return err
		}
	}
	return nil
}
func (a *q8GroupByProcessArgs) ParNum() uint8    { return a.parNum }
func (a *q8GroupByProcessArgs) CurEpoch() uint64 { return a.curEpoch }
func (a *q8GroupByProcessArgs) FuncName() string { return a.funcName }
func (a *q8GroupByProcessArgs) RecordFinishFunc() func(ctx context.Context, funcName string, instanceId uint8) error {
	return a.recordFinishFunc
}
func (a *q8GroupByProcessArgs) ErrChan() chan error {
	return a.errChan
}

func (h *q8GroupByHandler) getSrcSink(ctx context.Context, sp *common.QueryInput,
	input_stream *sharedlog_stream.ShardedSharedLogStream,
	output_streams []*sharedlog_stream.ShardedSharedLogStream,
) (*processor.MeteredSource, []*processor.MeteredSink, commtypes.MsgSerde, error) {
	var sinks []*processor.MeteredSink
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
		KeySerde:   commtypes.Uint64Serde{},
		ValueSerde: eventSerde,
		MsgSerde:   msgSerde,
	}
	// fmt.Fprintf(os.Stderr, "output to %v\n", output_stream.TopicName())
	src := processor.NewMeteredSource(sharedlog_stream.NewShardedSharedLogStreamSource(input_stream, inConfig))
	for _, output_stream := range output_streams {
		sink := processor.NewMeteredSink(sharedlog_stream.NewShardedSharedLogStreamSink(output_stream, outConfig))
		sinks = append(sinks, sink)
	}
	return src, sinks, msgSerde, nil
}

func (h *q8GroupByHandler) Q8GroupBy(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	input_stream, output_streams, err := benchutil.GetShardedInputOutputStreams(ctx, h.env, sp, false)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("get input output stream failed: %v", err),
		}
	}
	src, sinks, msgSerde, err := h.getSrcSink(ctx, sp, input_stream, output_streams)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}

	filterPerson, personsByIDMap, personsByIDFunc := h.getPersonsByID()
	filterAuctions, auctionsBySellerIDMap, auctionsBySellerIDFunc := h.getAucBySellerID()

	errChan := make(chan error)
	aucMsgChan := make(chan commtypes.Message)
	personMsgChan := make(chan commtypes.Message)
	procArgs := &q8GroupByProcessArgs{
		src:              src,
		sinks:            sinks,
		aucMsgChan:       aucMsgChan,
		personMsgChan:    personMsgChan,
		trackParFunc:     transaction.DefaultTrackSubstreamFunc,
		recordFinishFunc: transaction.DefaultRecordPrevInstanceFinishFunc,
		funcName:         h.funcName,
		curEpoch:         sp.ScaleEpoch,
		parNum:           sp.ParNum,
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go personsByIDFunc(ctx, procArgs, &wg, personMsgChan, errChan)
	wg.Add(1)
	go auctionsBySellerIDFunc(ctx, procArgs, &wg, aucMsgChan, errChan)

	task := transaction.StreamTask{
		ProcessFunc:   h.process,
		CurrentOffset: make(map[string]uint64),
		CloseFunc: func() {
			close(aucMsgChan)
			close(personMsgChan)
			wg.Wait()
		},
		FlushFunc: func() {
			// fmt.Fprintf(os.Stderr, "wait for all entries are consumed\n")
			// fmt.Fprintf(os.Stderr, "current auction channel len: %v\n", len(aucMsgChan))
			// fmt.Fprintf(os.Stderr, "current person channel len: %v\n", len(personMsgChan))
			for len(aucMsgChan) != 0 {
				time.Sleep(time.Duration(10) * time.Millisecond)
				// fmt.Fprintf(os.Stderr, "current auction channel len: %v\n", len(aucMsgChan))
			}
			for len(personMsgChan) != 0 {
				time.Sleep(time.Duration(10) * time.Millisecond)
				// fmt.Fprintf(os.Stderr, "current person channel len: %v\n", len(personMsgChan))
			}
		},
	}
	transaction.SetupConsistentHash(&h.cHashMu, h.cHash, sp.NumOutPartitions[0])

	if sp.EnableTransaction {
		srcs := make(map[string]processor.Source)
		srcs[sp.InputTopicNames[0]] = src
		streamTaskArgs := transaction.StreamTaskArgsTransaction{
			ProcArgs:      procArgs,
			Env:           h.env,
			MsgSerde:      msgSerde,
			Srcs:          srcs,
			OutputStreams: output_streams,
			QueryInput:    sp,
			TransactionalId: fmt.Sprintf("%s-%s-%d-%s",
				h.funcName, sp.InputTopicNames[0], sp.ParNum, sp.OutputTopicNames[0]),
			KVChangelogs:          nil,
			WindowStoreChangelogs: nil,
			FixedOutParNum:        0,
		}
		ret := transaction.SetupManagersAndProcessTransactional(ctx, h.env, &streamTaskArgs,
			func(procArgs interface{}, trackParFunc transaction.TrackKeySubStreamFunc,
				recordFinishFunc transaction.RecordPrevInstanceFinishFunc) {
				procArgs.(*q8GroupByProcessArgs).trackParFunc = trackParFunc
				procArgs.(*q8GroupByProcessArgs).recordFinishFunc = recordFinishFunc
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
		ProcArgs: procArgs,
		Duration: time.Duration(sp.Duration) * time.Second,
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

func (h *q8GroupByHandler) getPersonsByID() (*processor.MeteredProcessor, *processor.MeteredProcessor,
	func(ctx context.Context, args *q8GroupByProcessArgs, wg *sync.WaitGroup, msgChan chan commtypes.Message, errChan chan error)) {
	filterPerson := processor.NewMeteredProcessor(processor.NewStreamFilterProcessor(processor.PredicateFunc(
		func(msg *commtypes.Message) (bool, error) {
			event := msg.Value.(*ntypes.Event)
			return event.Etype == ntypes.PERSON, nil
		})))
	personsByIDMap := processor.NewMeteredProcessor(processor.NewStreamMapProcessor(
		processor.MapperFunc(func(msg commtypes.Message) (commtypes.Message, error) {
			event := msg.Value.(*ntypes.Event)
			return commtypes.Message{
				Key:       event.NewPerson.ID,
				Value:     msg.Value,
				Timestamp: msg.Timestamp,
			}, nil
		})))

	return filterPerson, personsByIDMap, func(ctx context.Context, args *q8GroupByProcessArgs, wg *sync.WaitGroup, msgChan chan commtypes.Message, errChan chan error) {
		defer wg.Done()
		for msg := range msgChan {
			event := msg.Value.(*ntypes.Event)
			ts, err := event.ExtractStreamTime()
			if err != nil {
				errChan <- fmt.Errorf("fail to extract timestamp: %v", err)
				return
			}
			msg.Timestamp = ts
			filteredMsgs, err := filterPerson.ProcessAndReturn(ctx, msg)
			if err != nil {
				errChan <- fmt.Errorf("filterPerson err: %v", err)
				return
			}
			for _, filteredMsg := range filteredMsgs {
				changeKeyedMsg, err := personsByIDMap.ProcessAndReturn(ctx, filteredMsg)
				if err != nil {
					errChan <- fmt.Errorf("personsByIDMap err: %v", err)
					return
				}

				k := changeKeyedMsg[0].Key.(uint64)
				h.cHashMu.RLock()
				parTmp, ok := h.cHash.Get(k)
				h.cHashMu.RUnlock()
				if !ok {
					errChan <- xerrors.New("fail to get output partition")
					return
				}
				par := parTmp.(uint8)
				err = args.trackParFunc(ctx, k, args.sinks[1].KeySerde(), args.sinks[1].TopicName(), par)
				if err != nil {
					errChan <- fmt.Errorf("add topic partition failed: %v", err)
					return
				}
				// fmt.Fprintf(os.Stderr, "append %v to substream %v\n", changeKeyedMsg[0], par)
				err = args.sinks[1].Sink(ctx, changeKeyedMsg[0], par, false)
				if err != nil {
					errChan <- fmt.Errorf("sink err: %v", err)
					return
				}
			}
		}
	}
}

func (h *q8GroupByHandler) getAucBySellerID() (*processor.MeteredProcessor, *processor.MeteredProcessor,
	func(ctx context.Context, args *q8GroupByProcessArgs, wg *sync.WaitGroup, msgChan chan commtypes.Message, errChan chan error),
) {
	filterAuctions := processor.NewMeteredProcessor(processor.NewStreamFilterProcessor(processor.PredicateFunc(
		func(m *commtypes.Message) (bool, error) {
			event := m.Value.(*ntypes.Event)
			return event.Etype == ntypes.AUCTION, nil
		})))

	auctionsBySellerIDMap := processor.NewMeteredProcessor(processor.NewStreamMapProcessor(
		processor.MapperFunc(func(msg commtypes.Message) (commtypes.Message, error) {
			event := msg.Value.(*ntypes.Event)
			return commtypes.Message{Key: event.NewAuction.Seller, Value: msg.Value, Timestamp: msg.Timestamp}, nil
		})))

	return filterAuctions, auctionsBySellerIDMap, func(ctx context.Context, args *q8GroupByProcessArgs, wg *sync.WaitGroup, msgChan chan commtypes.Message, errChan chan error) {
		defer wg.Done()
		for msg := range msgChan {
			event := msg.Value.(*ntypes.Event)
			ts, err := event.ExtractStreamTime()
			if err != nil {
				errChan <- fmt.Errorf("fail to extract timestamp: %v", err)
				return
			}
			msg.Timestamp = ts
			filteredMsgs, err := filterAuctions.ProcessAndReturn(ctx, msg)
			if err != nil {
				errChan <- fmt.Errorf("filterAuctions err: %v", err)
				return
			}
			for _, filteredMsg := range filteredMsgs {
				changeKeyedMsg, err := auctionsBySellerIDMap.ProcessAndReturn(ctx, filteredMsg)
				if err != nil {
					errChan <- fmt.Errorf("auctionsBySellerIDMap err: %v", err)
				}

				k := changeKeyedMsg[0].Key.(uint64)
				h.cHashMu.RLock()
				parTmp, ok := h.cHash.Get(k)
				h.cHashMu.RUnlock()
				if !ok {
					errChan <- xerrors.New("fail to get output partition")
				}
				par := parTmp.(uint8)
				err = args.trackParFunc(ctx, k, args.sinks[0].KeySerde(), args.sinks[0].TopicName(), par)
				if err != nil {
					errChan <- fmt.Errorf("add topic partition failed: %v", err)
					return
				}
				fmt.Fprintf(os.Stderr, "append %v to substream %v\n", changeKeyedMsg[0], par)
				err = args.sinks[0].Sink(ctx, changeKeyedMsg[0], par, false)
				if err != nil {
					errChan <- fmt.Errorf("sink err: %v", err)
				}
			}
		}
	}
}
