package handlers

import (
	"context"
	"encoding/json"
	"fmt"
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

type bidKeyedByAuction struct {
	env      types.Environment
	cHashMu  sync.RWMutex
	cHash    *hash.ConsistentHash
	funcName string
}

func NewBidKeyedByAuctionHandler(env types.Environment, funcName string) types.FuncHandler {
	return &bidKeyedByAuction{
		env:      env,
		cHash:    hash.NewConsistentHash(),
		funcName: funcName,
	}
}

func (h *bidKeyedByAuction) Call(ctx context.Context, input []byte) ([]byte, error) {
	sp := &common.QueryInput{}
	err := json.Unmarshal(input, sp)
	if err != nil {
		return nil, err
	}
	output := h.processBidKeyedByAuction(ctx, sp)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		return nil, err
	}
	return utils.CompressData(encodedOutput), nil
}

type bidKeyedByAuctionProcessArgs struct {
	src              *processor.MeteredSource
	sink             *processor.MeteredSink
	filterBid        *processor.MeteredProcessor
	selectKey        *processor.MeteredProcessor
	output_stream    *sharedlog_stream.ShardedSharedLogStream
	trackParFunc     transaction.TrackKeySubStreamFunc
	recordFinishFunc transaction.RecordPrevInstanceFinishFunc
	funcName         string
	curEpoch         uint64
	parNum           uint8
	numOutPartition  uint8
}

func (a *bidKeyedByAuctionProcessArgs) Source() processor.Source { return a.src }
func (a *bidKeyedByAuctionProcessArgs) Sink() processor.Sink     { return a.sink }
func (a *bidKeyedByAuctionProcessArgs) ParNum() uint8            { return a.parNum }
func (a *bidKeyedByAuctionProcessArgs) CurEpoch() uint64         { return a.curEpoch }
func (a *bidKeyedByAuctionProcessArgs) FuncName() string         { return a.funcName }
func (a *bidKeyedByAuctionProcessArgs) RecordFinishFunc() func(ctx context.Context, funcName string, instanceId uint8) error {
	return a.recordFinishFunc
}

func (h *bidKeyedByAuction) process(ctx context.Context,
	t *transaction.StreamTask,
	argsTmp interface{},
) (map[string]uint64, *common.FnOutput) {
	args := argsTmp.(*bidKeyedByAuctionProcessArgs)
	return transaction.CommonProcess(ctx, t, args, func(t *transaction.StreamTask, msg commtypes.MsgAndSeq) error {
		t.CurrentOffset[args.src.TopicName()] = msg.LogSeqNum
		event := msg.Msg.Value.(*ntypes.Event)
		ts, err := event.ExtractStreamTime()
		if err != nil {
			return fmt.Errorf("fail to extract timestamp: %v", err)
		}
		msg.Msg.Timestamp = ts
		bidMsg, err := args.filterBid.ProcessAndReturn(ctx, msg.Msg)
		if err != nil {
			return err
		}
		if bidMsg != nil {
			mappedKey, err := args.selectKey.ProcessAndReturn(ctx, bidMsg[0])
			if err != nil {
				return err
			}
			key := mappedKey[0].Key.(uint64)
			h.cHashMu.RLock()
			parTmp, ok := h.cHash.Get(key)
			h.cHashMu.RUnlock()
			if !ok {
				return xerrors.New("fail to calculate partition")
			}
			par := parTmp.(uint8)
			// par := uint8(key % uint64(args.numOutPartition))
			err = args.trackParFunc(ctx, key, args.sink.KeySerde(), args.sink.TopicName(), par)
			if err != nil {
				return fmt.Errorf("add topic partition failed: %v", err)
			}
			// fmt.Fprintf(os.Stderr, "out msg ts: %v\n", mappedKey[0].Timestamp)
			err = args.sink.Sink(ctx, mappedKey[0], par, false)
			if err != nil {
				return err
			}
		}
		return nil
	})

}

func (h *bidKeyedByAuction) processBidKeyedByAuction(ctx context.Context,
	sp *common.QueryInput,
) *common.FnOutput {
	input_stream, output_stream, err := benchutil.GetShardedInputOutputStreams(ctx, h.env, sp, false)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}

	src, sink, msgSerde, err := getSrcSinkUint64Key(sp, input_stream, output_stream)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	filterBid := processor.NewMeteredProcessor(processor.NewStreamFilterProcessor(processor.PredicateFunc(
		func(m *commtypes.Message) (bool, error) {
			event := m.Value.(*ntypes.Event)
			return event.Etype == ntypes.BID, nil
		})))
	selectKey := processor.NewMeteredProcessor(processor.NewStreamMapProcessor(processor.MapperFunc(
		func(m commtypes.Message) (commtypes.Message, error) {
			event := m.Value.(*ntypes.Event)
			return commtypes.Message{Key: event.Bid.Auction, Value: m.Value, Timestamp: m.Timestamp}, nil
		})))

	procArgs := &bidKeyedByAuctionProcessArgs{
		src:              src,
		sink:             sink,
		filterBid:        filterBid,
		selectKey:        selectKey,
		output_stream:    output_stream,
		parNum:           sp.ParNum,
		numOutPartition:  sp.NumOutPartition,
		trackParFunc:     transaction.DefaultTrackSubstreamFunc,
		recordFinishFunc: transaction.DefaultRecordPrevInstanceFinishFunc,
		curEpoch:         sp.ScaleEpoch,
		funcName:         h.funcName,
	}

	task := transaction.StreamTask{
		ProcessFunc:   h.process,
		CurrentOffset: make(map[string]uint64),
	}

	transaction.SetupConsistentHash(&h.cHashMu, h.cHash, sp.NumOutPartition)
	if sp.EnableTransaction {
		srcs := make(map[string]processor.Source)
		srcs[sp.InputTopicNames[0]] = src
		streamTaskArgs := transaction.StreamTaskArgsTransaction{
			ProcArgs:     procArgs,
			Env:          h.env,
			MsgSerde:     msgSerde,
			Srcs:         srcs,
			OutputStream: output_stream,
			QueryInput:   sp,
			TransactionalId: fmt.Sprintf("%s-%s-%d-%s", h.funcName,
				sp.InputTopicNames[0],
				sp.ParNum, sp.OutputTopicName),
			KVChangelogs:          nil,
			WindowStoreChangelogs: nil,
			FixedOutParNum:        0,
			CHash:                 h.cHash,
			CHashMu:               &h.cHashMu,
		}
		ret := transaction.SetupManagersAndProcessTransactional(ctx, h.env, &streamTaskArgs,
			func(procArgs interface{}, trackParFunc transaction.TrackKeySubStreamFunc, recordFinish transaction.RecordPrevInstanceFinishFunc) {
				procArgs.(*bidKeyedByAuctionProcessArgs).trackParFunc = trackParFunc
				procArgs.(*bidKeyedByAuctionProcessArgs).recordFinishFunc = recordFinish
			}, &task)
		if ret != nil && ret.Success {
			ret.Latencies["src"] = src.GetLatency()
			ret.Latencies["sink"] = sink.GetLatency()
			ret.Latencies["filterBid"] = filterBid.GetLatency()
			ret.Latencies["selectKey"] = selectKey.GetLatency()
			ret.Consumed["src"] = src.GetCount()
		}
		return ret
	}
	// return h.process(ctx, sp, args)
	streamTaskArgs := transaction.StreamTaskArgs{
		ProcArgs: procArgs,
		Duration: time.Duration(sp.Duration) * time.Second,
	}
	ret := task.Process(ctx, &streamTaskArgs)
	if ret != nil && ret.Success {
		ret.Latencies["src"] = src.GetLatency()
		ret.Latencies["sink"] = sink.GetLatency()
		ret.Latencies["filterBid"] = filterBid.GetLatency()
		ret.Latencies["selectKey"] = selectKey.GetLatency()
		ret.Consumed["src"] = src.GetCount()
	}
	return ret
}
