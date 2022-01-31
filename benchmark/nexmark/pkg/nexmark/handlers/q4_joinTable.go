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

	cHash *hash.ConsistentHash

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
	auctionsSrc *processor.MeteredSource
	bidsSrc     *processor.MeteredSource
	sink        *processor.MeteredSink
	aJoinB      *processor.AsyncFuncRunner
	bJoinA      *processor.AsyncFuncRunner
	parNum      uint8
}

func (h *q4JoinTableHandler) process(ctx context.Context,
	argsTmp interface{},
	trackParFunc func([]uint8) error,
) (map[string]uint64, *common.FnOutput) {
	args := argsTmp.(*q4JoinTableProcessArgs)
	var wg sync.WaitGroup

	bidsOutChan := make(chan *common.FnOutput)
	auctionsOutChan := make(chan *common.FnOutput)
	completed := uint32(0)
	wg.Add(1)
	go proc(ctx, bidsOutChan, args.bidsSrc, &wg,
		args.parNum, args.bJoinA, &h.offMu, h.currentOffset)
	wg.Add(1)
	go proc(ctx, auctionsOutChan, args.auctionsSrc, &wg,
		args.parNum, args.aJoinB, &h.offMu, h.currentOffset)

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
		case aJoinBMsgs := <-args.aJoinB.OutChan():
			err := pushMsgsToSink(ctx, args.sink, h.cHash, aJoinBMsgs, trackParFunc)
			if err != nil {
				return h.currentOffset, &common.FnOutput{
					Success: false,
					Message: err.Error(),
				}
			}
		case bJoinAMsgs := <-args.bJoinA.OutChan():
			err := pushMsgsToSink(ctx, args.sink, h.cHash, bJoinAMsgs, trackParFunc)
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

func (h *q4JoinTableHandler) Q4JoinTable(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	auctionsStream, bidsStream, outputStream, err := getInOutStreams(ctx, h.env, sp)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("get input output err: %v", err),
		}
	}
	auctionsSrc, bidsSrc, sink, err := getSrcSink(ctx, sp, auctionsStream,
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

	aJoinB := processor.NewAsyncFuncRunner(ctx, func(c context.Context, m commtypes.Message) ([]commtypes.Message, error) {
		_, err := toAuctionsTable.ProcessAndReturn(ctx, m)
		if err != nil {
			return nil, err
		}
		joinedMsgs, err := auctionsJoinBids.ProcessAndReturn(ctx, m)
		return joinedMsgs, err
	})

	bJoinA := processor.NewAsyncFuncRunner(ctx, func(c context.Context, m commtypes.Message) ([]commtypes.Message, error) {
		_, err := toBidsTable.ProcessAndReturn(ctx, m)
		if err != nil {
			return nil, err
		}
		joinedMsgs, err := bidsJoinAuctions.ProcessAndReturn(ctx, m)
		return joinedMsgs, err
	})

	for i := uint8(0); i < sp.NumOutPartition; i++ {
		h.cHash.Add(i)
	}

	procArgs := &q4JoinTableProcessArgs{
		auctionsSrc: auctionsSrc,
		bidsSrc:     bidsSrc,
		sink:        sink,
		aJoinB:      aJoinB,
		bJoinA:      bJoinA,
		parNum:      sp.ParNum,
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
			Srcs:         srcs,
			OutputStream: outputStream,
			QueryInput:   sp,
			TransactionalId: fmt.Sprintf("q3JoinTable-%s-%d-%s",
				sp.InputTopicNames[0], sp.ParNum, sp.OutputTopicName),
		}
		ret := task.ProcessWithTransaction(ctx, &streamTaskArgs)
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
