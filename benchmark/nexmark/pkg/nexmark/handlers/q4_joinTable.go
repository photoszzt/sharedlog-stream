package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sharedlog-stream/benchmark/common"
	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/hash"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/transaction"
	"sharedlog-stream/pkg/treemap"
	"sync"
	"time"

	"cs.utexas.edu/zjia/faas/types"
)

type q4JoinTableHandler struct {
	env types.Environment

	cHashMu sync.RWMutex
	cHash   *hash.ConsistentHash

	offMu    sync.Mutex
	funcName string
}

func NewQ4JoinTableHandler(env types.Environment, funcName string) types.FuncHandler {
	return &q4JoinTableHandler{
		env:      env,
		cHash:    hash.NewConsistentHash(),
		funcName: funcName,
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
	bidsOutChan      chan *common.FnOutput
	auctionsOutChan  chan *common.FnOutput
	recordFinishFunc transaction.RecordPrevInstanceFinishFunc
	funcName         string
	curEpoch         uint64
	parNum           uint8
}

func (a *q4JoinTableProcessArgs) ParNum() uint8    { return a.parNum }
func (a *q4JoinTableProcessArgs) CurEpoch() uint64 { return a.curEpoch }
func (a *q4JoinTableProcessArgs) FuncName() string { return a.funcName }
func (a *q4JoinTableProcessArgs) RecordFinishFunc() func(ctx context.Context, funcName string, instanceId uint8) error {
	return a.recordFinishFunc
}

func (h *q4JoinTableHandler) process(ctx context.Context,
	t *transaction.StreamTask,
	argsTmp interface{},
) (map[string]uint64, *common.FnOutput) {
	args := argsTmp.(*q4JoinTableProcessArgs)

	var bOut *common.FnOutput = nil
	var aOut *common.FnOutput = nil

	select {
	case bidsOutput := <-args.bidsOutChan:
		bOut = bidsOutput
		debug.Fprintf(os.Stderr, "Got bids out: %v\n", bOut)
	case auctionOutput := <-args.auctionsOutChan:
		aOut = auctionOutput
		debug.Fprintf(os.Stderr, "Got auctions out: %v\n", aOut)
	default:
	}
	debug.Fprintf(os.Stderr, "aOut: %v\n", aOut)
	debug.Fprintf(os.Stderr, "bOut: %v\n", bOut)
	if bOut != nil && !bOut.Success {
		return t.CurrentOffset, bOut
	}
	if aOut != nil && !aOut.Success {
		return t.CurrentOffset, aOut
	}
	return t.CurrentOffset, nil
}

func (h *q4JoinTableHandler) getSrcSink(ctx context.Context, sp *common.QueryInput,
	stream1 *sharedlog_stream.ShardedSharedLogStream,
	stream2 *sharedlog_stream.ShardedSharedLogStream,
	outputStream *sharedlog_stream.ShardedSharedLogStream,
) (*processor.MeteredSource, /* src1 */
	*processor.MeteredSource, /* src2 */
	*sharedlog_stream.ConcurrentMeteredSyncSink,
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
	auctionsConfig := &sharedlog_stream.StreamSourceConfig{
		Timeout:      common.SrcConsumeTimeout,
		KeyDecoder:   commtypes.Uint64Decoder{},
		ValueDecoder: eventSerde,
		MsgDecoder:   msgSerde,
	}
	personsConfig := &sharedlog_stream.StreamSourceConfig{
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
		KeySerde:      commtypes.Uint64Serde{},
		ValueSerde:    abSerde,
		MsgSerde:      msgSerde,
		FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
	}

	src1 := processor.NewMeteredSource(sharedlog_stream.NewShardedSharedLogStreamSource(stream1, auctionsConfig),
		time.Duration(sp.WarmupS)*time.Second)
	src2 := processor.NewMeteredSource(sharedlog_stream.NewShardedSharedLogStreamSource(stream2, personsConfig),
		time.Duration(sp.WarmupS)*time.Second)
	sink := sharedlog_stream.NewConcurrentMeteredSyncSink(sharedlog_stream.NewShardedSharedLogStreamSyncSink(outputStream, outConfig),
		time.Duration(sp.WarmupS)*time.Second)
	return src1, src2, sink, msgSerde, nil
}

func (h *q4JoinTableHandler) Q4JoinTable(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	auctionsStream, bidsStream, outputStream, err := getInOutStreams(ctx, h.env, sp, true)
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

	toAuctionsTable, auctionsStore, err := processor.ToInMemKVTable("auctionsByIDStore", compare,
		time.Duration(sp.WarmupS)*time.Second)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("toAucTab err: %v\n", err),
		}
	}

	toBidsTable, bidsStore, err := processor.ToInMemKVTable("bidsByAuctionIDStore", compare,
		time.Duration(sp.WarmupS)*time.Second)
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
		processor.NewTableTableJoinProcessor(bidsStore.Name(), bidsStore, joiner),
		time.Duration(sp.WarmupS)*time.Second)
	bidsJoinAuctions := processor.NewMeteredProcessor(
		processor.NewTableTableJoinProcessor(auctionsStore.Name(), auctionsStore, joiner),
		time.Duration(sp.WarmupS)*time.Second)
	getValidBid := processor.NewMeteredProcessor(
		processor.NewStreamFilterProcessor(
			processor.PredicateFunc(
				func(msg *commtypes.Message) (bool, error) {
					ab := msg.Value.(*ntypes.AuctionBid)
					return ab.BidDateTime >= ab.AucDateTime && ab.BidDateTime <= ab.AucExpires, nil
				})),
		time.Duration(sp.WarmupS)*time.Second,
	)

	filterAndGroupMsg := func(ctx context.Context, msgs []commtypes.Message) ([]commtypes.Message, error) {
		var newMsgs []commtypes.Message
		for _, msg := range msgs {
			validBid, err := getValidBid.ProcessAndReturn(ctx, msg)
			if err != nil {
				return nil, err
			}
			if validBid != nil {
				aic := ntypes.AuctionIdCategory{
					AucId:    validBid[0].Key.(uint64),
					Category: validBid[0].Value.(*ntypes.AuctionBid).AucCategory,
				}
				newMsg := commtypes.Message{Key: &aic, Value: validBid[0].Value, Timestamp: validBid[0].Timestamp}
				newMsgs = append(newMsgs, newMsg)
			}
		}
		return newMsgs, nil
	}

	aJoinB := JoinWorkerFunc(func(ctx context.Context, m commtypes.Message) ([]commtypes.Message, error) {
		_, err := toAuctionsTable.ProcessAndReturn(ctx, m)
		if err != nil {
			return nil, err
		}
		msgs, err := auctionsJoinBids.ProcessAndReturn(ctx, m)
		if err != nil {
			return nil, err
		}
		return filterAndGroupMsg(ctx, msgs)
	})

	bJoinA := JoinWorkerFunc(func(c context.Context, m commtypes.Message) ([]commtypes.Message, error) {
		_, err := toBidsTable.ProcessAndReturn(ctx, m)
		if err != nil {
			return nil, err
		}
		msgs, err := bidsJoinAuctions.ProcessAndReturn(ctx, m)
		if err != nil {
			return nil, err
		}
		return filterAndGroupMsg(ctx, msgs)
	})

	transaction.SetupConsistentHash(&h.cHashMu, h.cHash, sp.NumOutPartitions[0])
	bidsOutChan := make(chan *common.FnOutput, 1)
	auctionsOutChan := make(chan *common.FnOutput, 1)
	procArgs := &q4JoinTableProcessArgs{
		bidsOutChan:      bidsOutChan,
		auctionsOutChan:  auctionsOutChan,
		parNum:           sp.ParNum,
		recordFinishFunc: transaction.DefaultRecordPrevInstanceFinishFunc,
		curEpoch:         sp.ScaleEpoch,
		funcName:         h.funcName,
	}
	currentOffset := make(map[string]uint64)
	joinProcBid := &joinProcArgs{
		src:           bidsSrc,
		sink:          sink,
		parNum:        sp.ParNum,
		runner:        bJoinA,
		offMu:         &h.offMu,
		trackParFunc:  transaction.DefaultTrackSubstreamFunc,
		cHashMu:       &h.cHashMu,
		cHash:         h.cHash,
		currentOffset: currentOffset,
	}
	joinProcAuction := &joinProcArgs{
		src:           auctionsSrc,
		sink:          sink,
		parNum:        sp.ParNum,
		runner:        aJoinB,
		offMu:         &h.offMu,
		trackParFunc:  transaction.DefaultTrackSubstreamFunc,
		cHashMu:       &h.cHashMu,
		cHash:         h.cHash,
		currentOffset: currentOffset,
	}
	var wg sync.WaitGroup
	bidsDone := make(chan struct{})
	aucDone := make(chan struct{})
	bctx := context.WithValue(ctx, "id", "bid")
	actx := context.WithValue(ctx, "id", "auction")

	bidRun := make(chan struct{})
	aucRun := make(chan struct{})
	task := transaction.StreamTask{
		ProcessFunc:   h.process,
		CurrentOffset: make(map[string]uint64),
		PauseFunc: func() {
			close(bidsDone)
			close(aucDone)
			wg.Wait()
			/*
				sink.CloseAsyncPush()
				if err = sink.Flush(ctx); err != nil {
					panic(err)
				}
			*/
			err := sink.Flush(ctx)
			if err != nil {
				panic(err)
			}
		},
		ResumeFunc: func() {
			/*
				sink.InnerSink().RebuildMsgChan()
				if sp.EnableTransaction {
					sink.InnerSink().StartAsyncPushNoTick(ctx)
				} else {
					sink.InnerSink().StartAsyncPushWithTick(ctx)
					sink.InitFlushTimer()
				}
			*/
			bidsDone = make(chan struct{})
			aucDone = make(chan struct{})
			wg.Add(1)
			go joinProcLoop(bctx, bidsOutChan, joinProcBid, &wg, bidRun, bidsDone)
			wg.Add(1)
			go joinProcLoop(actx, auctionsOutChan, joinProcAuction, &wg, aucRun, aucDone)
			aucRun <- struct{}{}
			bidRun <- struct{}{}
		},
		CloseFunc: nil,
		InitFunc: func(progArgs interface{}) {
			/*
				if sp.EnableTransaction {
					sink.InnerSink().StartAsyncPushNoTick(ctx)
				} else {
					sink.InnerSink().StartAsyncPushWithTick(ctx)
					sink.InitFlushTimer()
				}
			*/

			auctionsSrc.StartWarmup()
			bidsSrc.StartWarmup()
			sink.StartWarmup()
			toAuctionsTable.StartWarmup()
			toBidsTable.StartWarmup()

		},
	}

	if sp.EnableTransaction {
		srcs := make(map[string]processor.Source)
		srcs[sp.InputTopicNames[0]] = auctionsSrc
		srcs[sp.InputTopicNames[1]] = bidsSrc
		streamTaskArgs := transaction.StreamTaskArgsTransaction{
			ProcArgs:      procArgs,
			Env:           h.env,
			MsgSerde:      msgSerde,
			Srcs:          srcs,
			OutputStreams: []*sharedlog_stream.ShardedSharedLogStream{outputStream},
			QueryInput:    sp,
			TransactionalId: fmt.Sprintf("%s-%s-%d-%s", h.funcName,
				sp.InputTopicNames[0], sp.ParNum, sp.OutputTopicNames[0]),
			KVChangelogs:          nil,
			WindowStoreChangelogs: nil,
			FixedOutParNum:        0,
		}
		ret := transaction.SetupManagersAndProcessTransactional(ctx, h.env, &streamTaskArgs,
			func(procArgs interface{}, trackParFunc transaction.TrackKeySubStreamFunc, recordFinishFunc transaction.RecordPrevInstanceFinishFunc) {
				joinProcAuction.trackParFunc = trackParFunc
				joinProcBid.trackParFunc = trackParFunc
				procArgs.(*q4JoinTableProcessArgs).recordFinishFunc = recordFinishFunc
			}, &task)
		if ret != nil && ret.Success {
			ret.Latencies["auctionsSrc"] = auctionsSrc.GetLatency()
			ret.Latencies["bidsSrc"] = bidsSrc.GetLatency()
			ret.Latencies["toAuctionsTable"] = toAuctionsTable.GetLatency()
			ret.Latencies["toBidsTable"] = toBidsTable.GetLatency()
			ret.Latencies["bidsJoinAuctions"] = bidsJoinAuctions.GetLatency()
			ret.Latencies["auctionsJoinBids"] = auctionsJoinBids.GetLatency()
			ret.Latencies["sink"] = sink.GetLatency()
			ret.Consumed["auctionsSrc"] = auctionsSrc.GetCount()
			ret.Consumed["bidsSrc"] = bidsSrc.GetCount()
		}
		return ret
	}
	streamTaskArgs := transaction.StreamTaskArgs{
		ProcArgs:   procArgs,
		Duration:   time.Duration(sp.Duration) * time.Second,
		WarmupTime: time.Duration(sp.WarmupS) * time.Second,
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
		ret.Consumed["auctionsSrc"] = auctionsSrc.GetCount()
		ret.Consumed["bidsSrc"] = bidsSrc.GetCount()
	}
	return ret
}
