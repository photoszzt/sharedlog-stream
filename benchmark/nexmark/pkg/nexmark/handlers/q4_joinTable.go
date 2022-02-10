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

type q4JoinTableHandler struct {
	env types.Environment

	cHashMu sync.RWMutex
	cHash   *hash.ConsistentHash

	offMu         sync.Mutex
	currentOffset map[string]uint64
}

func NewQ4JoinTableHandler(env types.Environment) types.FuncHandler {
	return &q4JoinTableHandler{
		env:           env,
		cHash:         hash.NewConsistentHash(),
		currentOffset: make(map[string]uint64),
	}
}

func (h *q4JoinTableHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &common.QueryInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output := h.Q4JoinTable(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	// fmt.Printf("query 3 output: %v\n", encodedOutput)
	return utils.CompressData(encodedOutput), nil
}

type q4JoinTableProcessArgs struct {
	auctionsSrc  *processor.MeteredSource
	bidsSrc      *processor.MeteredSource
	sink         *processor.MeteredSink
	aJoinB       JoinWorkerFunc
	bJoinA       JoinWorkerFunc
	trackParFunc func([]uint8) error
	parNum       uint8
}

func (h *q4JoinTableHandler) process(ctx context.Context,
	argsTmp interface{},
) (map[string]uint64, *common.FnOutput) {
	args := argsTmp.(*q4JoinTableProcessArgs)
	var wg sync.WaitGroup

	bidsOutChan := make(chan *common.FnOutput)
	auctionsOutChan := make(chan *common.FnOutput)
	completed := uint32(0)

	joinProgArgsBids := &joinProcArgs{
		src:           args.bidsSrc,
		sink:          args.sink,
		wg:            &wg,
		parNum:        args.parNum,
		runner:        args.bJoinA,
		offMu:         &h.offMu,
		currentOffset: h.currentOffset,
		trackParFunc:  args.trackParFunc,
	}
	wg.Add(1)
	go joinProc(ctx, bidsOutChan, joinProgArgsBids)

	joinProgArgsAuction := &joinProcArgs{
		src:           args.auctionsSrc,
		sink:          args.sink,
		wg:            &wg,
		parNum:        args.parNum,
		runner:        args.aJoinB,
		offMu:         &h.offMu,
		currentOffset: h.currentOffset,
		trackParFunc:  args.trackParFunc,
	}
	wg.Add(1)
	go joinProc(ctx, auctionsOutChan, joinProgArgsAuction)

L:
	for {
		select {
		case personOutput := <-bidsOutChan:
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

func (h *q4JoinTableHandler) getSrcSink(ctx context.Context, sp *common.QueryInput,
	stream1 *sharedlog_stream.ShardedSharedLogStream,
	stream2 *sharedlog_stream.ShardedSharedLogStream,
	outputStream *sharedlog_stream.ShardedSharedLogStream,
) (*processor.MeteredSource, /* src1 */
	*processor.MeteredSource, /* src2 */
	*processor.MeteredSink,
	commtypes.MsgSerde,
	error,
) {
	msgSerde, err := commtypes.GetMsgSerde(sp.SerdeFormat)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("get msg serde err: %v", err)
	}

	eventSerde, err := getEventSerde(sp.SerdeFormat)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("get event serde err: %v", err)
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
	var abSerde commtypes.Serde
	if sp.SerdeFormat == uint8(commtypes.JSON) {
		abSerde = &ntypes.AuctionBidJSONSerde{}
	} else {
		abSerde = &ntypes.AuctionBidMsgpSerde{}
	}
	outConfig := &sharedlog_stream.StreamSinkConfig{
		KeyEncoder:   commtypes.Uint64Encoder{},
		ValueEncoder: abSerde,
		MsgEncoder:   msgSerde,
	}

	src1 := processor.NewMeteredSource(sharedlog_stream.NewShardedSharedLogStreamSource(stream1, auctionsConfig))
	src2 := processor.NewMeteredSource(sharedlog_stream.NewShardedSharedLogStreamSource(stream2, personsConfig))
	sink := processor.NewMeteredSink(sharedlog_stream.NewShardedSharedLogStreamSink(outputStream, outConfig))
	return src1, src2, sink, msgSerde, nil
}

func (h *q4JoinTableHandler) Q4JoinTable(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	auctionsStream, bidsStream, outputStream, err := getInOutStreams(ctx, h.env, sp)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("get input output err: %v", err),
		}
	}
	auctionsSrc, bidsSrc, sink, msgSerde, err := h.getSrcSink(ctx, sp, auctionsStream,
		bidsStream, outputStream)
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

	toAuctionsTable, auctionsStore, err := processor.ToInMemKVTable("auctionsByIDStore", compare)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("toAucTab err: %v\n", err),
		}
	}

	toBidsTable, bidsStore, err := processor.ToInMemKVTable("bidsByAuctionIDStore", compare)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("toBidsTab err: %v\n", err),
		}
	}
	joiner := processor.ValueJoinerWithKeyFunc(func(readOnlyKey interface{}, leftVal interface{}, rightVal interface{}) interface{} {
		leftE := leftVal.(*ntypes.Event)
		auc := leftE.NewAuction
		rightE := rightVal.(*ntypes.Event)
		bid := rightE.Bid
		return &ntypes.AuctionBid{
			BidDateTime: bid.DateTime,
			BidPrice:    bid.Price,
			AucDateTime: auc.DateTime,
			AucExpires:  auc.Expires,
			AucCategory: auc.Category,
		}
	})

	auctionsJoinBids := processor.NewMeteredProcessor(
		processor.NewTableTableJoinProcessor(bidsStore.Name(), bidsStore, joiner))
	bidsJoinAuctions := processor.NewMeteredProcessor(
		processor.NewTableTableJoinProcessor(auctionsStore.Name(), auctionsStore, joiner))

	aJoinB := JoinWorkerFunc(func(c context.Context, m commtypes.Message,
		sink *processor.MeteredSink, trackParFunc func([]uint8) error,
	) error {
		_, err := toAuctionsTable.ProcessAndReturn(ctx, m)
		if err != nil {
			return err
		}
		joinedMsgs, err := auctionsJoinBids.ProcessAndReturn(ctx, m)
		if err != nil {
			return err
		}
		return pushMsgsToSink(ctx, sink, h.cHash, &h.cHashMu, joinedMsgs, trackParFunc)
	})

	bJoinA := JoinWorkerFunc(func(c context.Context, m commtypes.Message,
		sink *processor.MeteredSink, trackParFunc func([]uint8) error,
	) error {
		_, err := toBidsTable.ProcessAndReturn(ctx, m)
		if err != nil {
			return err
		}
		joinedMsgs, err := bidsJoinAuctions.ProcessAndReturn(ctx, m)
		if err != nil {
			return err
		}
		return pushMsgsToSink(ctx, sink, h.cHash, &h.cHashMu, joinedMsgs, trackParFunc)
	})

	sharedlog_stream.SetupConsistentHash(&h.cHashMu, h.cHash, sp.NumOutPartition)

	procArgs := &q4JoinTableProcessArgs{
		auctionsSrc:  auctionsSrc,
		bidsSrc:      bidsSrc,
		sink:         sink,
		aJoinB:       aJoinB,
		bJoinA:       bJoinA,
		parNum:       sp.ParNum,
		trackParFunc: sharedlog_stream.DefaultTrackParFunc,
	}

	task := sharedlog_stream.StreamTask{
		ProcessFunc: h.process,
	}

	if sp.EnableTransaction {
		srcs := make(map[string]processor.Source)
		srcs[sp.InputTopicNames[0]] = auctionsSrc
		srcs[sp.InputTopicNames[1]] = bidsSrc
		streamTaskArgs := sharedlog_stream.StreamTaskArgsTransaction{
			ProcArgs:     procArgs,
			Env:          h.env,
			MsgSerde:     msgSerde,
			Srcs:         srcs,
			OutputStream: outputStream,
			QueryInput:   sp,
			TransactionalId: fmt.Sprintf("q3JoinTable-%s-%d-%s",
				sp.InputTopicNames[0], sp.ParNum, sp.OutputTopicName),
			KVChangelogs:          nil,
			WindowStoreChangelogs: nil,
			FixedOutParNum:        0,
			CHash:                 h.cHash,
			CHashMu:               &h.cHashMu,
		}
		ret := sharedlog_stream.SetupManagersAndProcessTransactional(ctx, h.env, &streamTaskArgs,
			func(procArgs interface{}, trackParFunc func([]uint8) error) {
				procArgs.(*q4JoinTableProcessArgs).trackParFunc = trackParFunc
			}, &task)
		if ret != nil && ret.Success {
			ret.Latencies["auctionsSrc"] = auctionsSrc.GetLatency()
			ret.Latencies["bidsSrc"] = bidsSrc.GetLatency()
			ret.Latencies["toAuctionsTable"] = toAuctionsTable.GetLatency()
			ret.Latencies["toBidsTable"] = toBidsTable.GetLatency()
			ret.Latencies["bidsJoinAuctions"] = bidsJoinAuctions.GetLatency()
			ret.Latencies["auctionsJoinBids"] = auctionsJoinBids.GetLatency()
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
		ret.Latencies["bidsSrc"] = bidsSrc.GetLatency()
		ret.Latencies["toAuctionsTable"] = toAuctionsTable.GetLatency()
		ret.Latencies["toBidsTable"] = toBidsTable.GetLatency()
		ret.Latencies["bidsJoinAuctions"] = bidsJoinAuctions.GetLatency()
		ret.Latencies["auctionsJoinBids"] = auctionsJoinBids.GetLatency()
		ret.Latencies["sink"] = sink.GetLatency()
	}
	return ret
}
