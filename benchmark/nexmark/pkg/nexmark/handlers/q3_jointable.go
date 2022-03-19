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
	"sharedlog-stream/pkg/stream/processor/store"
	"sharedlog-stream/pkg/treemap"
	"sync"
	"time"

	"cs.utexas.edu/zjia/faas/types"
)

type q3JoinTableHandler struct {
	env     types.Environment
	cHashMu sync.RWMutex
	cHash   *hash.ConsistentHash

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
	input_in_tran bool,
) (*sharedlog_stream.ShardedSharedLogStream, /* auction */
	*sharedlog_stream.ShardedSharedLogStream, /* person */
	*sharedlog_stream.ShardedSharedLogStream, /* output */
	error,
) {
	inputStream1, err := sharedlog_stream.NewShardedSharedLogStream(env, input.InputTopicNames[0], input.NumInPartition,
		commtypes.SerdeFormat(input.SerdeFormat))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("NewSharedlogStream for input stream failed: %v", err)

	}

	inputStream2, err := sharedlog_stream.NewShardedSharedLogStream(env, input.InputTopicNames[1], input.NumInPartition,
		commtypes.SerdeFormat(input.SerdeFormat))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("NewSharedlogStream for input stream failed: %v", err)
	}
	outputStream, err := sharedlog_stream.NewShardedSharedLogStream(env, input.OutputTopicName, input.NumOutPartition,
		commtypes.SerdeFormat(input.SerdeFormat))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("NewSharedlogStream for output stream failed: %v", err)
	}
	if input.EnableTransaction {
		inputStream1.SetInTransaction(input_in_tran)
		inputStream2.SetInTransaction(input_in_tran)
		outputStream.SetInTransaction(true)
	} else {
		inputStream1.SetInTransaction(false)
		inputStream2.SetInTransaction(false)
		outputStream.SetInTransaction(false)
	}
	return inputStream1, inputStream2, outputStream, nil
}

type srcSinkSerde struct {
	src1         *processor.MeteredSource
	src2         *processor.MeteredSource
	sink         *processor.MeteredSink
	msgSerde     commtypes.MsgSerde
	src1KeySerde commtypes.Serde
	src1ValSerde commtypes.Serde
	src2KeySerde commtypes.Serde
	src2ValSerde commtypes.Serde
}

func (h *q3JoinTableHandler) getSrcSink(ctx context.Context, sp *common.QueryInput,
	stream1 *sharedlog_stream.ShardedSharedLogStream,
	stream2 *sharedlog_stream.ShardedSharedLogStream,
	outputStream *sharedlog_stream.ShardedSharedLogStream,
) (*srcSinkSerde,
	error,
) {
	msgSerde, err := commtypes.GetMsgSerde(sp.SerdeFormat)
	if err != nil {
		return nil, fmt.Errorf("get msg serde err: %v", err)
	}

	eventSerde, err := getEventSerde(sp.SerdeFormat)
	if err != nil {
		return nil, fmt.Errorf("get event serde err: %v", err)
	}
	auctionsConfig := &sharedlog_stream.SharedLogStreamConfig{
		Timeout:      common.SrcConsumeTimeout,
		KeyDecoder:   commtypes.Uint64Serde{},
		ValueDecoder: eventSerde,
		MsgDecoder:   msgSerde,
	}
	personsConfig := &sharedlog_stream.SharedLogStreamConfig{
		Timeout:      common.SrcConsumeTimeout,
		KeyDecoder:   commtypes.Uint64Serde{},
		ValueDecoder: eventSerde,
		MsgDecoder:   msgSerde,
	}
	var ncsiSerde commtypes.Serde
	if sp.SerdeFormat == uint8(commtypes.JSON) {
		ncsiSerde = ntypes.NameCityStateIdJSONSerde{}
	} else {
		ncsiSerde = ntypes.NameCityStateIdMsgpSerde{}
	}
	outConfig := &sharedlog_stream.StreamSinkConfig{
		KeySerde:   commtypes.Uint64Serde{},
		ValueSerde: ncsiSerde,
		MsgSerde:   msgSerde,
	}

	src1 := processor.NewMeteredSource(sharedlog_stream.NewShardedSharedLogStreamSource(stream1, auctionsConfig))
	src2 := processor.NewMeteredSource(sharedlog_stream.NewShardedSharedLogStreamSource(stream2, personsConfig))
	sink := processor.NewMeteredSink(sharedlog_stream.NewShardedSharedLogStreamSink(outputStream, outConfig))
	sss := &srcSinkSerde{
		src1:         src1,
		src2:         src2,
		sink:         sink,
		src1KeySerde: commtypes.Uint64Serde{},
		src1ValSerde: eventSerde,
		src2KeySerde: commtypes.Uint64Serde{},
		src2ValSerde: eventSerde,
		msgSerde:     msgSerde,
	}
	return sss, nil
}

type q3JoinTableProcessArgs struct {
	personSrc    *processor.MeteredSource
	auctionSrc   *processor.MeteredSource
	sink         *processor.MeteredSink
	pJoinA       JoinWorkerFunc
	aJoinP       JoinWorkerFunc
	trackParFunc sharedlog_stream.TrackKeySubStreamFunc
	parNum       uint8
}

func (h *q3JoinTableHandler) Query3JoinTable(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	auctionsStream, personsStream, outputStream, err := getInOutStreams(ctx, h.env, sp, true)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("get input output err: %v", err),
		}
	}
	sss, err := h.getSrcSink(ctx, sp, auctionsStream,
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

	joiner := processor.ValueJoinerWithKeyFunc(
		func(readOnlyKey interface{}, leftVal interface{}, rightVal interface{}) interface{} {
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
		processor.NewTableTableJoinProcessor(personsStore.Name(), personsStore,
			processor.ReverseValueJoinerWithKey(joiner)))

	pJoinA := JoinWorkerFunc(func(ctx context.Context, m commtypes.Message,
		sink *processor.MeteredSink, trackParFunc sharedlog_stream.TrackKeySubStreamFunc,
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
		return pushMsgsToSink(ctx, sink, h.cHash, &h.cHashMu, joinedMsgs, trackParFunc)
	})

	aJoinP := JoinWorkerFunc(func(c context.Context, m commtypes.Message,
		sink *processor.MeteredSink, trackParFunc sharedlog_stream.TrackKeySubStreamFunc,
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
		return pushMsgsToSink(ctx, sink, h.cHash, &h.cHashMu, joinedMsgs, trackParFunc)
	})

	sharedlog_stream.SetupConsistentHash(&h.cHashMu, h.cHash, sp.NumOutPartition)

	procArgs := &q3JoinTableProcessArgs{
		auctionSrc:   sss.src1,
		personSrc:    sss.src2,
		sink:         sss.sink,
		aJoinP:       aJoinP,
		pJoinA:       pJoinA,
		parNum:       sp.ParNum,
		trackParFunc: sharedlog_stream.DefaultTrackSubstreamFunc,
	}

	task := sharedlog_stream.StreamTask{
		ProcessFunc: h.process,
	}
	if sp.EnableTransaction {
		srcs := make(map[string]processor.Source)
		srcs[sp.InputTopicNames[0]] = sss.src1
		srcs[sp.InputTopicNames[1]] = sss.src2
		kvchangelogs := []*store.KVStoreChangelog{
			store.NewKVStoreChangelog(auctionsStore, auctionsStream, sss.src1KeySerde, sss.src1ValSerde, sp.ParNum),
			store.NewKVStoreChangelog(personsStore, personsStream, sss.src2KeySerde, sss.src2ValSerde, sp.ParNum),
		}
		streamTaskArgs := sharedlog_stream.StreamTaskArgsTransaction{
			ProcArgs:     procArgs,
			Env:          h.env,
			MsgSerde:     sss.msgSerde,
			Srcs:         srcs,
			OutputStream: outputStream,
			QueryInput:   sp,
			TransactionalId: fmt.Sprintf("q3JoinTable-%s-%d-%s",
				sp.InputTopicNames[0], sp.ParNum, sp.OutputTopicName),
			KVChangelogs:          kvchangelogs,
			WindowStoreChangelogs: nil,
			FixedOutParNum:        0,
			CHash:                 h.cHash,
			CHashMu:               &h.cHashMu,
		}
		ret := sharedlog_stream.SetupManagersAndProcessTransactional(ctx, h.env, &streamTaskArgs,
			func(procArgs interface{}, trackParFunc sharedlog_stream.TrackKeySubStreamFunc) {
				procArgs.(*q3JoinTableProcessArgs).trackParFunc = trackParFunc
			}, &task)
		if ret != nil && ret.Success {
			ret.Latencies["auctionsSrc"] = sss.src1.GetLatency()
			ret.Latencies["personsSrc"] = sss.src2.GetLatency()
			ret.Latencies["toAuctionsTable"] = toAuctionsTable.GetLatency()
			ret.Latencies["toPersonsTable"] = toPersonsTable.GetLatency()
			ret.Latencies["personJoinsAuctions"] = personJoinsAuctions.GetLatency()
			ret.Latencies["auctionJoinsPersons"] = auctionJoinsPersons.GetLatency()
			ret.Latencies["sink"] = sss.sink.GetLatency()
			ret.Consumed["auctionSrc"] = sss.src1.GetCount()
			ret.Consumed["personsSrc"] = sss.src2.GetCount()
		}
		return ret
	}
	streamTaskArgs := sharedlog_stream.StreamTaskArgs{
		ProcArgs: procArgs,
		Duration: time.Duration(sp.Duration) * time.Second,
	}
	ret := task.Process(ctx, &streamTaskArgs)
	if ret != nil && ret.Success {
		ret.Latencies["auctionsSrc"] = sss.src1.GetLatency()
		ret.Latencies["personsSrc"] = sss.src2.GetLatency()
		ret.Latencies["toAuctionsTable"] = toAuctionsTable.GetLatency()
		ret.Latencies["toPersonsTable"] = toPersonsTable.GetLatency()
		ret.Latencies["personJoinsAuctions"] = personJoinsAuctions.GetLatency()
		ret.Latencies["auctionJoinsPersons"] = auctionJoinsPersons.GetLatency()
		ret.Latencies["sink"] = sss.sink.GetLatency()
		ret.Consumed["auctionSrc"] = sss.src1.GetCount()
		ret.Consumed["personsSrc"] = sss.src2.GetCount()
	}
	return ret
}
