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
	auctionsSrc      *processor.MeteredSource
	bidsSrc          *processor.MeteredSource
	sink             *sharedlog_stream.ConcurrentMeteredSink
	aJoinB           JoinWorkerFunc
	bJoinA           JoinWorkerFunc
	trackParFunc     transaction.TrackKeySubStreamFunc
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
		currentOffset: t.CurrentOffset,
		trackParFunc:  args.trackParFunc,
		cHashMu:       &h.cHashMu,
		cHash:         h.cHash,
	}
	bidsDone := uint32(0)
	aucDone := uint32(0)
	wg.Add(1)
	go joinProc(ctx, bidsOutChan, joinProgArgsBids, &bidsDone)

	joinProgArgsAuction := &joinProcArgs{
		src:           args.auctionsSrc,
		sink:          args.sink,
		wg:            &wg,
		parNum:        args.parNum,
		runner:        args.aJoinB,
		offMu:         &h.offMu,
		currentOffset: t.CurrentOffset,
		trackParFunc:  args.trackParFunc,
		cHashMu:       &h.cHashMu,
		cHash:         h.cHash,
	}
	wg.Add(1)
	go joinProc(ctx, auctionsOutChan, joinProgArgsAuction, &aucDone)
	var pOut *common.FnOutput = nil
	var aOut *common.FnOutput = nil
L:
	for {
		select {
		case personOutput := <-bidsOutChan:
			pOut = personOutput
			completed += 1
			if completed == 2 {
				break L
			}
		case auctionOutput := <-auctionsOutChan:
			aOut = auctionOutput
			completed += 1
			if completed == 2 {
				break L
			}
		}
	}
	wg.Wait()
	if pOut != nil {
		if aOut != nil {
			succ := pOut.Success && aOut.Success
			return t.CurrentOffset, &common.FnOutput{Success: succ, Message: pOut.Message + "," + aOut.Message}
		}
		return t.CurrentOffset, pOut
	} else if aOut != nil {
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
	*sharedlog_stream.ConcurrentMeteredSink,
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
	sink := sharedlog_stream.NewConcurrentMeteredSink(sharedlog_stream.NewShardedSharedLogStreamSink(outputStream, outConfig),
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

	aJoinB := JoinWorkerFunc(func(c context.Context, m commtypes.Message) ([]commtypes.Message, error) {
		_, err := toAuctionsTable.ProcessAndReturn(ctx, m)
		if err != nil {
			return nil, err
		}
		return auctionsJoinBids.ProcessAndReturn(ctx, m)
	})

	bJoinA := JoinWorkerFunc(func(c context.Context, m commtypes.Message) ([]commtypes.Message, error) {
		_, err := toBidsTable.ProcessAndReturn(ctx, m)
		if err != nil {
			return nil, err
		}
		return bidsJoinAuctions.ProcessAndReturn(ctx, m)
	})

	transaction.SetupConsistentHash(&h.cHashMu, h.cHash, sp.NumOutPartitions[0])

	procArgs := &q4JoinTableProcessArgs{
		auctionsSrc:      auctionsSrc,
		bidsSrc:          bidsSrc,
		sink:             sink,
		aJoinB:           aJoinB,
		bJoinA:           bJoinA,
		parNum:           sp.ParNum,
		trackParFunc:     transaction.DefaultTrackSubstreamFunc,
		recordFinishFunc: transaction.DefaultRecordPrevInstanceFinishFunc,
		curEpoch:         sp.ScaleEpoch,
		funcName:         h.funcName,
	}

	task := transaction.StreamTask{
		ProcessFunc:   h.process,
		CurrentOffset: make(map[string]uint64),
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
				procArgs.(*q4JoinTableProcessArgs).trackParFunc = trackParFunc
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
