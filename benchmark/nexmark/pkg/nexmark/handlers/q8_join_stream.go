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
)

type q8JoinStreamHandler struct {
	env types.Environment

	cHashMu sync.RWMutex
	cHash   *hash.ConsistentHash

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

type q8JoinStreamProcessArgs struct {
	personSrc    *processor.MeteredSource
	auctionSrc   *processor.MeteredSource
	sink         *processor.MeteredSink
	pJoinA       JoinWorkerFunc
	aJoinP       JoinWorkerFunc
	trackParFunc sharedlog_stream.TrackKeySubStreamFunc
	parNum       uint8
}

func (h *q8JoinStreamHandler) process(
	ctx context.Context,
	argsTmp interface{},
) (map[string]uint64, *common.FnOutput) {
	args := argsTmp.(*q8JoinStreamProcessArgs)
	var wg sync.WaitGroup

	personsOutChan := make(chan *common.FnOutput)
	auctionsOutChan := make(chan *common.FnOutput)
	completed := uint32(0)
	wg.Add(1)
	joinProcArgsPerson := &joinProcArgs{
		src:           args.personSrc,
		sink:          args.sink,
		wg:            &wg,
		parNum:        args.parNum,
		runner:        args.pJoinA,
		offMu:         &h.offMu,
		currentOffset: h.currentOffset,
		trackParFunc:  args.trackParFunc,
	}
	go joinProc(ctx, personsOutChan, joinProcArgsPerson)
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
			// fmt.Fprintf(os.Stderr, "completed person: %d\n", completed)
			if completed == 2 {
				// fmt.Fprint(os.Stderr, "break for loop per\n")
				break L
			}
		case auctionOutput := <-auctionsOutChan:
			if auctionOutput != nil {
				return h.currentOffset, auctionOutput
			}
			completed += 1
			// fmt.Fprintf(os.Stderr, "completed auc: %d\n", completed)
			if completed == 2 {
				// fmt.Fprint(os.Stderr, "break for loop auc\n")
				break L
			}
		}
	}
	wg.Wait()
	return h.currentOffset, nil
}

func (h *q8JoinStreamHandler) processSerial(
	ctx context.Context,
	argsTmp interface{},
) (map[string]uint64, *common.FnOutput) {
	args := argsTmp.(*q8JoinStreamProcessArgs)

	joinProgArgsAuction := &joinProcArgs{
		src:           args.auctionSrc,
		sink:          args.sink,
		parNum:        args.parNum,
		runner:        args.aJoinP,
		offMu:         &h.offMu,
		currentOffset: h.currentOffset,
		trackParFunc:  args.trackParFunc,
	}
	ret := joinProcSerial(ctx, joinProgArgsAuction)
	if ret != nil {
		return h.currentOffset, ret
	}
	joinProcArgsPerson := &joinProcArgs{
		src:           args.personSrc,
		sink:          args.sink,
		parNum:        args.parNum,
		runner:        args.pJoinA,
		offMu:         &h.offMu,
		currentOffset: h.currentOffset,
		trackParFunc:  args.trackParFunc,
	}
	ret = joinProcSerial(ctx, joinProcArgsPerson)
	return h.currentOffset, ret
}

func (h *q8JoinStreamHandler) getSrcSink(ctx context.Context, sp *common.QueryInput,
	stream1 *sharedlog_stream.ShardedSharedLogStream,
	stream2 *sharedlog_stream.ShardedSharedLogStream,
	outputStream *sharedlog_stream.ShardedSharedLogStream,
) (*srcSinkSerde, error,
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
	var ptSerde commtypes.Serde
	if sp.SerdeFormat == uint8(commtypes.JSON) {
		ptSerde = &ntypes.PersonTimeJSONSerde{}
	} else {
		ptSerde = &ntypes.PersonTimeMsgpSerde{}
	}
	outConfig := &sharedlog_stream.StreamSinkConfig{
		KeySerde:   commtypes.Uint64Serde{},
		ValueSerde: ptSerde,
		MsgSerde:   msgSerde,
	}

	src1 := processor.NewMeteredSource(sharedlog_stream.NewShardedSharedLogStreamSource(stream1, auctionsConfig))
	src2 := processor.NewMeteredSource(sharedlog_stream.NewShardedSharedLogStreamSource(stream2, personsConfig))
	sink := processor.NewMeteredSink(sharedlog_stream.NewShardedSharedLogStreamSink(outputStream, outConfig))
	return &srcSinkSerde{src1: src1, src2: src2, sink: sink,
		keySerdes: []commtypes.Serde{commtypes.Uint64Serde{}, commtypes.Uint64Serde{}},
		valSerdes: []commtypes.Serde{eventSerde, eventSerde}, msgSerde: msgSerde}, nil
}

func (h *q8JoinStreamHandler) Query8JoinStream(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	auctionsStream, personsStream, outputStream, err := getInOutStreams(ctx, h.env, sp, true)
	if err != nil {
		return &common.FnOutput{Success: false, Message: fmt.Sprintf("get input output err: %v", err)}
	}
	sss, err := h.getSrcSink(ctx, sp, auctionsStream,
		personsStream, outputStream)
	if err != nil {
		return &common.FnOutput{Success: false, Message: fmt.Sprintf("getSrcSink err: %v\n", err)}
	}
	auctionsSrc, personsSrc, sink := sss.src1, sss.src2, sss.sink
	joinWindows, err := processor.NewJoinWindowsWithGrace(time.Duration(10)*time.Second, time.Duration(5)*time.Second)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	var toAuctionsWindowTab *processor.MeteredProcessor
	var auctionsWinStore store.WindowStore
	var toPersonsWinTab *processor.MeteredProcessor
	var personsWinTab store.WindowStore
	if sp.TableType == uint8(store.IN_MEM) {
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

		toAuctionsWindowTab, auctionsWinStore, err = processor.ToInMemWindowTable(
			"auctionsBySellerIDWinTab", joinWindows, compare)
		if err != nil {
			return &common.FnOutput{Success: false, Message: fmt.Sprintf("to table err: %v", err)}
		}

		toPersonsWinTab, personsWinTab, err = processor.ToInMemWindowTable(
			"personsByIDWinTab", joinWindows, compare,
		)
		if err != nil {
			return &common.FnOutput{Success: false, Message: fmt.Sprintf("to table err: %v", err)}
		}
	} else if sp.TableType == uint8(store.MONGODB) {
		toAuctionsWindowTab, auctionsWinStore, err = processor.ToMongoDBWindowTable(ctx,
			"auctionsBySellerIDWinTab", sp.MongoAddr, joinWindows, sss.keySerdes[0], sss.valSerdes[0])
		if err != nil {
			return &common.FnOutput{Success: false, Message: fmt.Sprintf("to table err: %v", err)}
		}
		toPersonsWinTab, personsWinTab, err = processor.ToMongoDBWindowTable(ctx,
			"personsByIDWinTab", sp.MongoAddr, joinWindows, sss.keySerdes[1], sss.valSerdes[1])
		if err != nil {
			return &common.FnOutput{Success: false, Message: fmt.Sprintf("to table err: %v", err)}
		}
	}

	joiner := processor.ValueJoinerWithKeyTsFunc(func(readOnlyKey interface{},
		leftValue interface{}, rightValue interface{}, leftTs int64, rightTs int64) interface{} {
		fmt.Fprint(os.Stderr, "get into joiner\n")
		lv := leftValue.(*ntypes.Event)
		rv := rightValue.(*ntypes.Event)
		st := leftTs
		if st > rightTs {
			st = rightTs
		}
		if lv.Etype == ntypes.PERSON {
			return &ntypes.PersonTime{
				ID:        lv.NewPerson.ID,
				Name:      lv.NewPerson.Name,
				StartTime: st,
			}
		} else {
			return &ntypes.PersonTime{
				ID:        rv.NewPerson.ID,
				Name:      rv.NewPerson.Name,
				StartTime: st,
			}
		}
	})

	sharedTimeTracker := processor.NewTimeTracker()
	personsJoinsAuctions := processor.NewMeteredProcessor(
		processor.NewStreamStreamJoinProcessor(auctionsWinStore, joinWindows,
			joiner, false, true, sharedTimeTracker))

	auctionsJoinsPersons := processor.NewMeteredProcessor(
		processor.NewStreamStreamJoinProcessor(personsWinTab, joinWindows,
			processor.ReverseValueJoinerWithKeyTs(joiner), false, false, sharedTimeTracker),
	)

	pJoinA := func(ctx context.Context,
		m commtypes.Message,
		sink *processor.MeteredSink,
		trackParFunc sharedlog_stream.TrackKeySubStreamFunc,
	) error {
		_, err := toPersonsWinTab.ProcessAndReturn(ctx, m)
		if err != nil {
			return err
		}
		joinedMsgs, err := personsJoinsAuctions.ProcessAndReturn(ctx, m)
		if err != nil {
			return err
		}
		return pushMsgsToSink(ctx, sink, h.cHash, &h.cHashMu, joinedMsgs, trackParFunc)
	}

	aJoinP := func(ctx context.Context,
		m commtypes.Message,
		sink *processor.MeteredSink,
		trackParFunc sharedlog_stream.TrackKeySubStreamFunc,
	) error {
		_, err := toAuctionsWindowTab.ProcessAndReturn(ctx, m)
		if err != nil {
			return err
		}
		joinedMsgs, err := auctionsJoinsPersons.ProcessAndReturn(ctx, m)
		if err != nil {
			return err
		}
		return pushMsgsToSink(ctx, sink, h.cHash, &h.cHashMu, joinedMsgs, trackParFunc)
	}

	sharedlog_stream.SetupConsistentHash(&h.cHashMu, h.cHash, sp.NumOutPartition)

	procArgs := &q8JoinStreamProcessArgs{
		personSrc:    personsSrc,
		auctionSrc:   auctionsSrc,
		sink:         sink,
		pJoinA:       pJoinA,
		aJoinP:       aJoinP,
		parNum:       sp.ParNum,
		trackParFunc: sharedlog_stream.DefaultTrackSubstreamFunc,
	}

	task := sharedlog_stream.StreamTask{
		ProcessFunc: h.process,
	}
	if sp.EnableTransaction {
		var wsc []*store.WindowStoreChangelog
		if sp.TableType == uint8(store.IN_MEM) {
			wsc = []*store.WindowStoreChangelog{
				store.NewWindowStoreChangelog(
					auctionsWinStore,
					auctionsStream,
					nil,
					sss.keySerdes[0],
					sss.valSerdes[0],
					sp.ParNum,
				),
				store.NewWindowStoreChangelog(
					personsWinTab,
					personsStream,
					nil,
					sss.keySerdes[1],
					sss.valSerdes[1],
					sp.ParNum,
				),
			}
		} else if sp.TableType == uint8(store.MONGODB) {
			// TODO: MONGODB
			wsc = nil
		} else {
			panic("unrecognized table type")
		}
		streamTaskArgs := sharedlog_stream.StreamTaskArgsTransaction{
			ProcArgs:     procArgs,
			Env:          h.env,
			Srcs:         map[string]processor.Source{sp.InputTopicNames[0]: auctionsSrc, sp.InputTopicNames[1]: personsSrc},
			OutputStream: outputStream,
			QueryInput:   sp,
			TransactionalId: fmt.Sprintf("q8JoinStream-%s-%d-%s",
				sp.InputTopicNames[0], sp.ParNum, sp.OutputTopicName),
			MsgSerde:              sss.msgSerde,
			WindowStoreChangelogs: wsc,
			CHash:                 h.cHash,
			CHashMu:               &h.cHashMu,
		}
		ret := sharedlog_stream.SetupManagersAndProcessTransactional(ctx, h.env, &streamTaskArgs,
			func(procArgs interface{}, trackParFunc sharedlog_stream.TrackKeySubStreamFunc) {
				procArgs.(*q8JoinStreamProcessArgs).trackParFunc = trackParFunc
			}, &task)
		if ret != nil && ret.Success {
			ret.Latencies["auctionsSrc"] = auctionsSrc.GetLatency()
			ret.Latencies["personsSrc"] = personsSrc.GetLatency()
			ret.Latencies["toAuctionsWindowTab"] = toAuctionsWindowTab.GetLatency()
			ret.Latencies["toPersonsWinTab"] = toPersonsWinTab.GetLatency()
			ret.Latencies["personsJoinsAuctions"] = personsJoinsAuctions.GetLatency()
			ret.Latencies["auctionsJoinsPersons"] = auctionsJoinsPersons.GetLatency()
			ret.Latencies["sink"] = sink.GetLatency()
			ret.Consumed["auctionSrc"] = auctionsSrc.GetCount()
			ret.Consumed["personsSrc"] = personsSrc.GetCount()
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
		ret.Consumed["auctionSrc"] = auctionsSrc.GetCount()
		ret.Consumed["personsSrc"] = personsSrc.GetCount()
	}
	return ret
}
