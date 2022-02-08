package handlers

import (
	"context"
	"encoding/json"
	"fmt"
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

func (h *q3JoinTableHandler) process(ctx context.Context,
	argsTmp interface{},
) (map[string]uint64, *common.FnOutput) {
	args := argsTmp.(*q3JoinTableProcessArgs)
	var wg sync.WaitGroup

	personsOutChan := make(chan *common.FnOutput)
	auctionsOutChan := make(chan *common.FnOutput)
	completed := uint32(0)
	wg.Add(1)
	joinProcPerson := &joinProcArgs{
		src:           args.personSrc,
		sink:          args.sink,
		wg:            &wg,
		parNum:        args.parNum,
		runner:        args.pJoinA,
		offMu:         &h.offMu,
		currentOffset: h.currentOffset,
		trackParFunc:  args.trackParFunc,
	}
	go joinProc(ctx, personsOutChan, joinProcPerson)
	wg.Add(1)
	joinProgArgsAuction := &joinProcArgs{
		src:           args.auctionSrc,
		sink:          args.sink,
		wg:            &wg,
		parNum:        args.parNum,
		runner:        args.aJoinP,
		offMu:         &h.offMu,
		currentOffset: h.currentOffset,
		trackParFunc:  args.trackParFunc,
	}
	go joinProc(ctx, auctionsOutChan, joinProgArgsAuction)

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
		}
	}
	wg.Wait()
	return h.currentOffset, nil
}

func getInOutStreams(
	ctx context.Context,
	env types.Environment,
	input *common.QueryInput,
) (*sharedlog_stream.ShardedSharedLogStream, /* auction */
	*sharedlog_stream.ShardedSharedLogStream, /* person */
	*sharedlog_stream.ShardedSharedLogStream, /* output */
	error,
) {
	inputStreamAuction, err := sharedlog_stream.NewShardedSharedLogStream(env, input.InputTopicNames[0], uint8(input.NumInPartition))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("NewSharedlogStream for input stream failed: %v", err)

	}

	inputStreamPerson, err := sharedlog_stream.NewShardedSharedLogStream(env, input.InputTopicNames[1], uint8(input.NumInPartition))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("NewSharedlogStream for input stream failed: %v", err)
	}
	outputStream, err := sharedlog_stream.NewShardedSharedLogStream(env, input.OutputTopicName, uint8(input.NumOutPartition))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("NewSharedlogStream for output stream failed: %v", err)
	}
	if input.EnableTransaction {
		inputStreamAuction.SetInTransaction(true)
		inputStreamPerson.SetInTransaction(true)
		outputStream.SetInTransaction(true)
	} else {
		inputStreamAuction.SetInTransaction(false)
		inputStreamPerson.SetInTransaction(false)
		outputStream.SetInTransaction(false)
	}
	return inputStreamAuction, inputStreamPerson, outputStream, nil
}

func (h *q3JoinTableHandler) getSrcSink(ctx context.Context, sp *common.QueryInput,
	stream1 *sharedlog_stream.ShardedSharedLogStream,
	stream2 *sharedlog_stream.ShardedSharedLogStream,
	outputStream *sharedlog_stream.ShardedSharedLogStream,
) (*processor.MeteredSource, /* src1 */
	*processor.MeteredSource, /* src2 */
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
		KeyEncoder: commtypes.Uint64Encoder{},
		ValueEncoder: commtypes.EncoderFunc(func(val interface{}) ([]byte, error) {
			ret := val.(*ntypes.NameCityStateId)
			if sp.SerdeFormat == uint8(commtypes.JSON) {
				return json.Marshal(ret)
			} else {
				return ret.MarshalMsg(nil)
			}
		}),
		MsgEncoder: msgSerde,
	}

	src1 := processor.NewMeteredSource(sharedlog_stream.NewShardedSharedLogStreamSource(stream1, auctionsConfig))
	src2 := processor.NewMeteredSource(sharedlog_stream.NewShardedSharedLogStreamSource(stream2, personsConfig))
	sink := processor.NewMeteredSink(sharedlog_stream.NewShardedSharedLogStreamSink(outputStream, outConfig))
	return src1, src2, sink, nil
}

type q3JoinTableProcessArgs struct {
	personSrc    *processor.MeteredSource
	auctionSrc   *processor.MeteredSource
	sink         *processor.MeteredSink
	pJoinA       JoinWorkerFunc
	aJoinP       JoinWorkerFunc
	trackParFunc func([]uint8) error
	parNum       uint8
}

func (h *q3JoinTableHandler) Query3JoinTable(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	auctionsStream, personsStream, outputStream, err := getInOutStreams(ctx, h.env, sp)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("get input output err: %v", err),
		}
	}
	auctionsSrc, personsSrc, sink, err := h.getSrcSink(ctx, sp, auctionsStream,
		personsStream, outputStream)
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

	pJoinA := JoinWorkerFunc(func(c context.Context, m commtypes.Message,
		sink *processor.MeteredSink, trackParFunc func([]uint8) error,
	) error {
		// msg is person
		_, err := toPersonsTable.ProcessAndReturn(ctx, m)
		if err != nil {
			return err
		}
		joinedMsgs, err := personJoinsAuctions.ProcessAndReturn(ctx, m)
		if err != nil {
			return err
		}
		return pushMsgsToSink(ctx, sink, h.cHash, joinedMsgs, trackParFunc)
	})

	aJoinP := JoinWorkerFunc(func(c context.Context, m commtypes.Message,
		sink *processor.MeteredSink, trackParFunc func([]uint8) error,
	) error {
		// msg is auction
		_, err := toAuctionsTable.ProcessAndReturn(ctx, m)
		if err != nil {
			return err
		}
		joinedMsgs, err := auctionJoinsPersons.ProcessAndReturn(ctx, m)
		if err != nil {
			return err
		}
		return pushMsgsToSink(ctx, sink, h.cHash, joinedMsgs, trackParFunc)
	})

	for i := uint8(0); i < sp.NumOutPartition; i++ {
		h.cHash.Add(i)
	}

	procArgs := &q3JoinTableProcessArgs{
		auctionSrc:   auctionsSrc,
		personSrc:    personsSrc,
		sink:         sink,
		aJoinP:       aJoinP,
		pJoinA:       pJoinA,
		parNum:       sp.ParNum,
		trackParFunc: sharedlog_stream.DefaultTrackParFunc,
	}

	task := sharedlog_stream.StreamTask{
		ProcessFunc: h.process,
	}
	if sp.EnableTransaction {
		srcs := make(map[string]processor.Source)
		srcs[sp.InputTopicNames[0]] = auctionsSrc
		srcs[sp.InputTopicNames[1]] = personsSrc
		msgSerde, err := commtypes.GetMsgSerde(sp.SerdeFormat)
		if err != nil {
			return &common.FnOutput{
				Success: false,
				Message: fmt.Sprintf("get msg serde err: %v", err),
			}
		}
		kvchangelogs := []*sharedlog_stream.KVStoreChangelog{
			sharedlog_stream.NewKVStoreChangelog(auctionsStore, auctionsStream, sp.ParNum),
			sharedlog_stream.NewKVStoreChangelog(personsStore, personsStream, sp.ParNum),
		}
		streamTaskArgs := sharedlog_stream.StreamTaskArgsTransaction{
			ProcArgs:     procArgs,
			Env:          h.env,
			Srcs:         srcs,
			OutputStream: outputStream,
			QueryInput:   sp,
			TransactionalId: fmt.Sprintf("q3JoinTable-%s-%d-%s",
				sp.InputTopicNames[0], sp.ParNum, sp.OutputTopicName),
			MsgSerde:              msgSerde,
			KVChangelogs:          kvchangelogs,
			WindowStoreChangelogs: nil,
		}
		tm, trackParFunc, err := sharedlog_stream.SetupTransactionManager(ctx, &streamTaskArgs)
		if err != nil {
			return &common.FnOutput{
				Success: false,
				Message: fmt.Sprintf("setup transaction manager failed: %v\n", err),
			}
		}
		procArgs.trackParFunc = trackParFunc
		ret := task.ProcessWithTransaction(ctx, tm, &streamTaskArgs)
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
