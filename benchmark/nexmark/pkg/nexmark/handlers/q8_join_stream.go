package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sharedlog-stream/benchmark/common"
	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/concurrent_skiplist"
	"sharedlog-stream/pkg/hash"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"
	"sync"
	"time"

	"cs.utexas.edu/zjia/faas/types"
	"golang.org/x/xerrors"
)

type q8JoinStreamHandler struct {
	env   types.Environment
	cHash *hash.ConsistentHash

	offMu         sync.Mutex
	currentOffset map[string]uint64
}

func NewQ8JoinStreamHandler(env types.Environment) types.FuncHandler {
	return &q8JoinStreamHandler{
		env:           env,
		cHash:         hash.NewConsistentHash(),
		currentOffset: make(map[string]uint64),
	}
}

func (h *q8JoinStreamHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &common.QueryInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output := h.Query8JoinStream(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	// fmt.Printf("query 3 output: %v\n", encodedOutput)
	return utils.CompressData(encodedOutput), nil
}

func (h *q8JoinStreamHandler) getSrcSink(ctx context.Context, sp *common.QueryInput,
	auctionsStream *sharedlog_stream.ShardedSharedLogStream,
	personsStream *sharedlog_stream.ShardedSharedLogStream,
	outputStream *sharedlog_stream.ShardedSharedLogStream,
) (*processor.MeteredSource, /* auctionSrc */
	*processor.MeteredSource, /* personsSrc */
	*processor.MeteredSink, error,
) {
	msgSerde, err := commtypes.GetMsgSerde(sp.SerdeFormat)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("get msg serde err: %v", err)
	}
	eventSerde, err := getEventSerde(sp.SerdeFormat)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("get event serde err: %v", err)
	}
	auctionsConfig := &sharedlog_stream.SharedLogStreamConfig{
		Timeout:      common.SrcConsumeTimeout,
		KeyDecoder:   commtypes.Uint64Decoder{},
		ValueDecoder: eventSerde,
		MsgDecoder:   msgSerde,
	}
	personsConfig := &sharedlog_stream.SharedLogStreamConfig{
		Timeout:      common.SrcConsumeTimeout,
		KeyDecoder:   commtypes.Uint64Decoder{},
		ValueDecoder: eventSerde,
		MsgDecoder:   msgSerde,
	}
	outConfig := &sharedlog_stream.StreamSinkConfig{
		ValueEncoder: commtypes.EncoderFunc(func(val interface{}) ([]byte, error) {
			ret := val.(*ntypes.NameCityStateId)
			if sp.SerdeFormat == uint8(commtypes.JSON) {
				return json.Marshal(ret)
			} else {
				return ret.MarshalMsg(nil)
			}
		}),
	}

	auctionsSrc := processor.NewMeteredSource(sharedlog_stream.NewShardedSharedLogStreamSource(auctionsStream, auctionsConfig))
	personsSrc := processor.NewMeteredSource(sharedlog_stream.NewShardedSharedLogStreamSource(personsStream, personsConfig))
	sink := processor.NewMeteredSink(sharedlog_stream.NewShardedSharedLogStreamSink(outputStream, outConfig))
	return auctionsSrc, personsSrc, sink, nil
}

func (h *q8JoinStreamHandler) getShardedInputOutputStreams(ctx context.Context,
	input *common.QueryInput,
) (*sharedlog_stream.ShardedSharedLogStream, /* auction */
	*sharedlog_stream.ShardedSharedLogStream, /* person */
	*sharedlog_stream.ShardedSharedLogStream, /* output */
	error,
) {
	inputStreamAuction, err := sharedlog_stream.NewShardedSharedLogStream(h.env, input.InputTopicNames[0], uint8(input.NumInPartition))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("NewSharedlogStream for input stream failed: %v", err)

	}
	// fmt.Fprintf(os.Stderr, "reading auctions from %v\n", inputStreamAuction.TopicName())

	inputStreamPerson, err := sharedlog_stream.NewShardedSharedLogStream(h.env, input.InputTopicNames[1], uint8(input.NumInPartition))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("NewSharedlogStream for input stream failed: %v", err)
	}
	// fmt.Fprintf(os.Stderr, "reading persons from %v\n", inputStreamPerson.TopicName())
	outputStream, err := sharedlog_stream.NewShardedSharedLogStream(h.env, input.OutputTopicName, uint8(input.NumOutPartition))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("NewSharedlogStream for output stream failed: %v", err)
	}
	return inputStreamAuction, inputStreamPerson, outputStream, nil
}

type q8JoinStreamProcessArgs struct {
	personSrc  *processor.MeteredSource
	auctionSrc *processor.MeteredSource
	sink       *processor.MeteredSink
	pJoinA     *processor.AsyncFuncRunner
	aJoinP     *processor.AsyncFuncRunner
	parNum     uint8
}

func (h *q8JoinStreamHandler) proc(
	ctx context.Context,
	out chan *common.FnOutput,
	src *processor.MeteredSource,
	wg *sync.WaitGroup,
	parNum uint8,
	runner *processor.AsyncFuncRunner,
) {
	defer wg.Done()
	gotMsgs, err := src.Consume(ctx, parNum)
	if err != nil {
		if xerrors.Is(err, sharedlog_stream.ErrStreamSourceTimeout) {
			out <- &common.FnOutput{
				Success: true,
				Message: err.Error(),
			}
			runner.Close()
			err = runner.Wait()
			if err != nil {
				fmt.Fprintf(os.Stderr, "runner failed: %v\n", err)
			}
			return
		}
		out <- &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
		runner.Close()
		err = runner.Wait()
		if err != nil {
			fmt.Fprintf(os.Stderr, "runner failed: %v\n", err)
		}
		return
	}
	for _, msg := range gotMsgs {
		h.offMu.Lock()
		h.currentOffset[src.TopicName()] = msg.LogSeqNum
		h.offMu.Unlock()

		event := msg.Msg.Value.(*ntypes.Event)
		ts, err := event.ExtractStreamTime()
		if err != nil {
			out <- &common.FnOutput{
				Success: false,
				Message: fmt.Sprintf("fail to extract timestamp: %v", err),
			}
			runner.Close()
			err = runner.Wait()
			if err != nil {
				fmt.Fprintf(os.Stderr, "runner failed: %v\n", err)
			}
			return
		}
		msg.Msg.Timestamp = ts
		runner.Accept(msg.Msg)
	}
	out <- nil
}

func (h *q8JoinStreamHandler) process(
	ctx context.Context,
	argsTmp interface{},
	trackParFunc func([]uint8) error,
) (map[string]uint64, *common.FnOutput) {
	args := argsTmp.(*q8JoinStreamProcessArgs)
	var wg sync.WaitGroup

	personsOutChan := make(chan *common.FnOutput)
	auctionsOutChan := make(chan *common.FnOutput)
	completed := uint32(0)
	wg.Add(1)
	go h.proc(ctx, personsOutChan, args.personSrc, &wg, args.parNum, args.pJoinA)
	wg.Add(1)
	go h.proc(ctx, auctionsOutChan, args.auctionSrc, &wg, args.parNum, args.aJoinP)

L:
	for {
		select {
		case personOutput := <-personsOutChan:
			if personOutput != nil {
				return h.currentOffset, personOutput
			}
			completed += 1
			if completed == 2 {
				break L
			}
		case auctionOutput := <-auctionsOutChan:
			if auctionOutput != nil {
				return h.currentOffset, auctionOutput
			}
			completed += 1
			if completed == 2 {
				break L
			}
		case aJoinPMsgs := <-args.aJoinP.OutChan():
			err := pushMsgsToSink(ctx, args.sink, h.cHash, aJoinPMsgs, trackParFunc)
			if err != nil {
				return h.currentOffset, &common.FnOutput{
					Success: false,
					Message: err.Error(),
				}
			}
		case pJoinAMsgs := <-args.pJoinA.OutChan():
			err := pushMsgsToSink(ctx, args.sink, h.cHash, pJoinAMsgs, trackParFunc)
			if err != nil {
				return h.currentOffset, &common.FnOutput{
					Success: false,
					Message: err.Error(),
				}
			}
		}
	}
	wg.Wait()
	return h.currentOffset, nil
}

func (h *q8JoinStreamHandler) Query8JoinStream(ctx context.Context, sp *common.QueryInput) *common.FnOutput {

	auctionsStream, personsStream, outputStream, err := h.getShardedInputOutputStreams(ctx, sp)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("get input output err: %v", err),
		}
	}
	auctionsSrc, personsSrc, sink, err := h.getSrcSink(ctx, sp, auctionsStream, personsStream, outputStream)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("getSrcSink err: %v\n", err),
		}
	}
	joinWindows := processor.NewJoinWindowsNoGrace(time.Duration(10) * time.Second)

	compare := concurrent_skiplist.CompareFunc(func(lhs, rhs interface{}) int {
		l, ok := lhs.(uint64)
		if ok {
			r := rhs.(uint64)
			if l < r {
				return -1
			} else if l == r {
				return 0
			} else {
				return 1
			}
		} else {
			lv := lhs.(store.VersionedKey)
			rv := rhs.(store.VersionedKey)
			lvk := lv.Key.(uint64)
			rvk := rv.Key.(uint64)
			if lvk < rvk {
				return -1
			} else if lvk == rvk {
				if lv.Version < rv.Version {
					return -1
				} else if lv.Version == rv.Version {
					return 0
				} else {
					return 1
				}
			} else {
				return 1
			}
		}
	})

	toAuctionsWindowTab, auctionsWinStore, err := processor.ToInMemWindowTable(
		"auctionsBySellerIDWinTab", joinWindows, compare)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("to table err: %v", err),
		}
	}

	toPersonsWinTab, personsWinTab, err := processor.ToInMemWindowTable(
		"personsByIDWinTab", joinWindows, compare,
	)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("to table err: %v", err),
		}
	}

	joiner := processor.ValueJoinerWithKeyFunc(func(readOnlyKey interface{}, leftValue interface{}, rightValue interface{}) interface{} {
		rv := rightValue.(*ntypes.Event)
		return &ntypes.PersonTime{
			ID:        rv.NewPerson.ID,
			Name:      rv.NewPerson.Name,
			StartTime: 0,
		}
	})

	sharedTimeTracker := processor.NewTimeTracker()
	personsJoinsAuctions := processor.NewMeteredProcessor(
		processor.NewStreamStreamJoinProcessor(auctionsWinStore, joinWindows, joiner, false, true, sharedTimeTracker))

	auctionsJoinsPersons := processor.NewMeteredProcessor(
		processor.NewStreamStreamJoinProcessor(personsWinTab, joinWindows, joiner, false, false, sharedTimeTracker),
	)

	pJoinA := processor.NewAsyncFuncRunner(ctx, func(c context.Context, m commtypes.Message) ([]commtypes.Message, error) {
		_, err := toPersonsWinTab.ProcessAndReturn(ctx, m)
		if err != nil {
			return nil, err
		}
		joinedMsgs, err := personsJoinsAuctions.ProcessAndReturn(ctx, m)
		return joinedMsgs, err
	})

	aJoinP := processor.NewAsyncFuncRunner(ctx, func(c context.Context, m commtypes.Message) ([]commtypes.Message, error) {
		_, err := toAuctionsWindowTab.ProcessAndReturn(ctx, m)
		if err != nil {
			return nil, err
		}
		joinedMsgs, err := auctionsJoinsPersons.ProcessAndReturn(ctx, m)
		return joinedMsgs, err
	})

	for i := uint8(0); i < sp.NumOutPartition; i++ {
		h.cHash.Add(i)
	}

	procArgs := &q8JoinStreamProcessArgs{
		personSrc:  personsSrc,
		auctionSrc: auctionsSrc,
		sink:       sink,
		pJoinA:     pJoinA,
		aJoinP:     aJoinP,
		parNum:     sp.ParNum,
	}

	task := sharedlog_stream.StreamTask{
		ProcessFunc: h.process,
	}
	if sp.EnableTransaction {
		srcs := make(map[string]processor.Source)
		srcs[sp.InputTopicNames[0]] = auctionsSrc
		srcs[sp.InputTopicNames[1]] = personsSrc
		streamTaskArgs := sharedlog_stream.StreamTaskArgsTransaction{
			ProcArgs:     procArgs,
			Env:          h.env,
			Srcs:         srcs,
			OutputStream: outputStream,
			QueryInput:   sp,
			TransactionalId: fmt.Sprintf("q8JoinStream-%s-%d-%s",
				sp.InputTopicNames[0], sp.ParNum, sp.OutputTopicName),
		}
		ret := task.ProcessWithTransaction(ctx, &streamTaskArgs)
		if ret != nil && ret.Success {
			ret.Latencies["auctionsSrc"] = auctionsSrc.GetLatency()
			ret.Latencies["personsSrc"] = personsSrc.GetLatency()
			ret.Latencies["toAuctionsWindowTab"] = toAuctionsWindowTab.GetLatency()
			ret.Latencies["toPersonsWinTab"] = toPersonsWinTab.GetLatency()
			ret.Latencies["personsJoinsAuctions"] = personsJoinsAuctions.GetLatency()
			ret.Latencies["auctionsJoinsPersons"] = auctionsJoinsPersons.GetLatency()
			ret.Latencies["sink"] = sink.GetLatency()
		}
		return ret
	}
	streamTaskArgs := sharedlog_stream.StreamTaskArgs{
		ProcArgs: procArgs,
		Duration: time.Duration(sp.Duration) * time.Second,
	}
	ret := task.Process(ctx, &streamTaskArgs)
	if ret != nil && ret.Success {
		ret.Latencies["auctionsSrc"] = auctionsSrc.GetLatency()
		ret.Latencies["personsSrc"] = personsSrc.GetLatency()
		ret.Latencies["sink"] = sink.GetLatency()
		ret.Latencies["toAuctionsWindowTab"] = toAuctionsWindowTab.GetLatency()
		ret.Latencies["toPersonsWinTab"] = toPersonsWinTab.GetLatency()
		ret.Latencies["personsJoinsAuctions"] = personsJoinsAuctions.GetLatency()
		ret.Latencies["auctionsJoinsPersons"] = auctionsJoinsPersons.GetLatency()
	}
	return ret
}
