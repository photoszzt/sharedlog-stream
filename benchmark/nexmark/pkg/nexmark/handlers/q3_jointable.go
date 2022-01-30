package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sharedlog-stream/benchmark/common"
	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/hash"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/treemap"
	"sync"
	"time"

	"cs.utexas.edu/zjia/faas/types"
	"golang.org/x/xerrors"
)

type q3JoinTableHandler struct {
	env   types.Environment
	cHash *hash.ConsistentHash

	offMu         sync.Mutex
	currentOffset map[string]uint64
}

func NewQ3JoinTableHandler(env types.Environment) types.FuncHandler {
	return &q3JoinTableHandler{
		env:           env,
		cHash:         hash.NewConsistentHash(),
		currentOffset: make(map[string]uint64),
	}
}

func (h *q3JoinTableHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &common.QueryInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output := h.Query3JoinTable(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	// fmt.Printf("query 3 output: %v\n", encodedOutput)
	return utils.CompressData(encodedOutput), nil
}

func pushMsgsToSink(
	ctx context.Context,
	sink *processor.MeteredSink,
	cHash *hash.ConsistentHash,
	msgs []commtypes.Message,
	trackParFunc func([]uint8) error,
) error {
	for _, msg := range msgs {
		key := msg.Key.(uint64)
		parTmp, ok := cHash.Get(key)
		if !ok {
			return fmt.Errorf("fail to calculate partition")
		}
		par := parTmp.(uint8)
		err := trackParFunc([]uint8{par})
		if err != nil {
			return fmt.Errorf("add topic partition failed: %v", err)
		}
		err = sink.Sink(ctx, msg, par, false)
		if err != nil {
			return err
		}
	}
	return nil
}

func (h *q3JoinTableHandler) proc(
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

func (h *q3JoinTableHandler) process(ctx context.Context,
	argsTmp interface{},
	trackParFunc func([]uint8) error,
) (map[string]uint64, *common.FnOutput) {
	args := argsTmp.(*q3JoinTableProcessArgs)
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

func (h *q3JoinTableHandler) getShardedInputOutputStreams(ctx context.Context,
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

	inputStreamPerson, err := sharedlog_stream.NewShardedSharedLogStream(h.env, input.InputTopicNames[1], uint8(input.NumInPartition))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("NewSharedlogStream for input stream failed: %v", err)
	}
	outputStream, err := sharedlog_stream.NewShardedSharedLogStream(h.env, input.OutputTopicName, uint8(input.NumOutPartition))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("NewSharedlogStream for output stream failed: %v", err)
	}
	return inputStreamAuction, inputStreamPerson, outputStream, nil
}

func (h *q3JoinTableHandler) getSrcSink(ctx context.Context, sp *common.QueryInput,
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

type q3JoinTableProcessArgs struct {
	personSrc  *processor.MeteredSource
	auctionSrc *processor.MeteredSource
	sink       *processor.MeteredSink
	pJoinA     *processor.AsyncFuncRunner
	aJoinP     *processor.AsyncFuncRunner
	parNum     uint8
}

func (h *q3JoinTableHandler) Query3JoinTable(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
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

	compare := func(a, b treemap.Key) int {
		valA := a.(uint64)
		valB := b.(uint64)
		if valA < valB {
			return -1
		} else if valA == valB {
			return 0
		} else {
			return 1
		}
	}

	toAuctionsTable, auctionsStore, err := processor.ToInMemKVTable("auctionsBySellerIDStore", compare)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("toAucTab err: %v\n", err),
		}
	}

	toPersonsTable, personsStore, err := processor.ToInMemKVTable("personsByIDStore", compare)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("toPersonsTab err: %v\n", err),
		}
	}

	joiner := processor.ValueJoinerWithKeyFunc(func(readOnlyKey interface{}, leftVal interface{}, rightVal interface{}) interface{} {
		event := rightVal.(*ntypes.Event)
		return &ntypes.NameCityStateId{
			Name:  event.NewPerson.Name,
			City:  event.NewPerson.City,
			State: event.NewPerson.State,
			ID:    event.NewPerson.ID,
		}
	})

	personJoinsAuctions := processor.NewMeteredProcessor(
		processor.NewTableTableJoinProcessor(auctionsStore.Name(), auctionsStore, joiner))

	auctionJoinsPersons := processor.NewMeteredProcessor(
		processor.NewTableTableJoinProcessor(personsStore.Name(), personsStore, joiner))

	pJoinA := processor.NewAsyncFuncRunner(ctx, func(c context.Context, m commtypes.Message) ([]commtypes.Message, error) {
		// msg is person
		_, err := toPersonsTable.ProcessAndReturn(ctx, m)
		if err != nil {
			return nil, err
		}
		joinedMsgs, err := personJoinsAuctions.ProcessAndReturn(ctx, m)
		return joinedMsgs, err
	})

	aJoinP := processor.NewAsyncFuncRunner(ctx, func(c context.Context, m commtypes.Message) ([]commtypes.Message, error) {
		// msg is auction
		_, err := toAuctionsTable.ProcessAndReturn(ctx, m)
		if err != nil {
			return nil, err
		}
		joinedMsgs, err := auctionJoinsPersons.ProcessAndReturn(ctx, m)
		return joinedMsgs, err
	})

	for i := uint8(0); i < sp.NumOutPartition; i++ {
		h.cHash.Add(i)
	}

	procArgs := &q3JoinTableProcessArgs{
		auctionSrc: auctionsSrc,
		personSrc:  personsSrc,
		sink:       sink,
		aJoinP:     aJoinP,
		pJoinA:     pJoinA,
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
			TransactionalId: fmt.Sprintf("q3JoinTable-%s-%d-%s",
				sp.InputTopicNames[0], sp.ParNum, sp.OutputTopicName),
		}
		ret := task.ProcessWithTransaction(ctx, &streamTaskArgs)
		if ret != nil && ret.Success {
			ret.Latencies["auctionsSrc"] = auctionsSrc.GetLatency()
			ret.Latencies["personsSrc"] = personsSrc.GetLatency()
			ret.Latencies["toAuctionsTable"] = toAuctionsTable.GetLatency()
			ret.Latencies["toPersonsTable"] = toPersonsTable.GetLatency()
			ret.Latencies["personJoinsAuctions"] = personJoinsAuctions.GetLatency()
			ret.Latencies["auctionJoinsPersons"] = auctionJoinsPersons.GetLatency()
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
		ret.Latencies["toAuctionsTable"] = toAuctionsTable.GetLatency()
		ret.Latencies["toPersonsTable"] = toPersonsTable.GetLatency()
		ret.Latencies["personJoinsAuctions"] = personJoinsAuctions.GetLatency()
		ret.Latencies["auctionJoinsPersons"] = auctionJoinsPersons.GetLatency()
		ret.Latencies["sink"] = sink.GetLatency()
	}
	return ret
}
