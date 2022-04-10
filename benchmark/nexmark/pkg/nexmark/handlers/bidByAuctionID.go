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
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/transaction"
	"sync"
	"time"

	"cs.utexas.edu/zjia/faas/types"
	"golang.org/x/xerrors"
)

type bidByAuctionIDHandler struct {
	env      types.Environment
	cHashMu  sync.RWMutex
	cHash    *hash.ConsistentHash
	funcName string
}

func NewBidByAuctionIDHandler(env types.Environment, funcName string) types.FuncHandler {
	return &bidByAuctionIDHandler{
		env:      env,
		cHash:    hash.NewConsistentHash(),
		funcName: funcName,
	}
}

func (h *bidByAuctionIDHandler) process(
	ctx context.Context,
	t *transaction.StreamTask,
	argsTmp interface{},
) (map[string]uint64, *common.FnOutput) {
	args := argsTmp.(*bidsByAuctionIDProcessArgs)
	return transaction.CommonProcess(ctx, t, args, func(t *transaction.StreamTask, msg commtypes.MsgAndSeq) error {
		t.CurrentOffset[args.src.TopicName()] = msg.LogSeqNum
		if msg.MsgArr != nil {
			for _, subMsg := range msg.MsgArr {
				err := h.procMsg(ctx, subMsg, args)
				if err != nil {
					return err
				}
			}
			return nil
		}
		return h.procMsg(ctx, msg.Msg, args)
	})
}

func (h *bidByAuctionIDHandler) procMsg(ctx context.Context, msg commtypes.Message, args *bidsByAuctionIDProcessArgs) error {
	event := msg.Value.(*ntypes.Event)
	ts, err := event.ExtractStreamTime()
	if err != nil {
		return fmt.Errorf("fail to extract timestamp: %v", err)
	}
	msg.Timestamp = ts
	filteredMsgs, err := args.filterBids.ProcessAndReturn(ctx, msg)
	if err != nil {
		return fmt.Errorf("filterAuctions err: %v", err)
	}
	for _, filteredMsg := range filteredMsgs {
		changeKeyedMsg, err := args.bidsByAuctionID.ProcessAndReturn(ctx, filteredMsg)
		if err != nil {
			return fmt.Errorf("auctionsBySellerIDMap err: %v", err)
		}

		k := changeKeyedMsg[0].Key.(uint64)
		h.cHashMu.RLock()
		parTmp, ok := h.cHash.Get(k)
		h.cHashMu.RUnlock()
		if !ok {
			return xerrors.New("fail to get output partition")
		}
		par := parTmp.(uint8)
		err = args.trackParFunc(ctx, k, args.sink.KeySerde(), args.sink.TopicName(), par)
		if err != nil {
			return fmt.Errorf("add topic partition failed: %v", err)
		}
		err = args.sink.Sink(ctx, changeKeyedMsg[0], par, false)
		if err != nil {
			return fmt.Errorf("sink err: %v", err)
		}
	}
	return nil
}

func (h *bidByAuctionIDHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &common.QueryInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output := h.bidByAuctionID(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	return utils.CompressData(encodedOutput), nil
}

type bidsByAuctionIDProcessArgs struct {
	src              *processor.MeteredSource
	sink             *processor.MeteredSink
	filterBids       *processor.MeteredProcessor
	bidsByAuctionID  *processor.MeteredProcessor
	trackParFunc     transaction.TrackKeySubStreamFunc
	recordFinishFunc transaction.RecordPrevInstanceFinishFunc
	funcName         string
	curEpoch         uint64
	parNum           uint8
}

func (a *bidsByAuctionIDProcessArgs) Source() processor.Source { return a.src }
func (a *bidsByAuctionIDProcessArgs) Sink() processor.Sink     { return a.sink }
func (a *bidsByAuctionIDProcessArgs) ParNum() uint8            { return a.parNum }
func (a *bidsByAuctionIDProcessArgs) CurEpoch() uint64         { return a.curEpoch }
func (a *bidsByAuctionIDProcessArgs) FuncName() string         { return a.funcName }
func (a *bidsByAuctionIDProcessArgs) RecordFinishFunc() func(ctx context.Context, funcName string, instanceId uint8) error {
	return a.recordFinishFunc
}

func (h *bidByAuctionIDHandler) bidByAuctionID(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	input_stream, output_stream, err := benchutil.GetShardedInputOutputStreams(ctx, h.env, sp, false)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("get input output stream failed: %v", err),
		}
	}
	src, sink, msgSerde, err := CommonGetSrcSink(ctx, sp, input_stream, output_stream)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}

	filterBids := processor.NewMeteredProcessor(processor.NewStreamFilterProcessor(processor.PredicateFunc(
		func(m *commtypes.Message) (bool, error) {
			event := m.Value.(*ntypes.Event)
			return event.Etype == ntypes.BID, nil
		})))

	bidsByAuctionIDMap := processor.NewMeteredProcessor(processor.NewStreamMapProcessor(
		processor.MapperFunc(func(msg commtypes.Message) (commtypes.Message, error) {
			event := msg.Value.(*ntypes.Event)
			return commtypes.Message{Key: event.Bid.Auction, Value: msg.Value, Timestamp: msg.Timestamp}, nil
		})))
	procArgs := &bidsByAuctionIDProcessArgs{
		src:              src,
		sink:             sink,
		filterBids:       filterBids,
		bidsByAuctionID:  bidsByAuctionIDMap,
		parNum:           sp.ParNum,
		funcName:         h.funcName,
		curEpoch:         sp.ScaleEpoch,
		trackParFunc:     transaction.DefaultTrackSubstreamFunc,
		recordFinishFunc: transaction.DefaultRecordPrevInstanceFinishFunc,
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
				sp.InputTopicNames[0], sp.ParNum, sp.OutputTopicName),
			KVChangelogs:          nil,
			WindowStoreChangelogs: nil,
			FixedOutParNum:        0,
			CHash:                 h.cHash,
			CHashMu:               &h.cHashMu,
		}
		ret := transaction.SetupManagersAndProcessTransactional(ctx, h.env, &streamTaskArgs,
			func(procArgs interface{}, trackParFunc transaction.TrackKeySubStreamFunc, recordFinishFunc transaction.RecordPrevInstanceFinishFunc) {
				procArgs.(*bidsByAuctionIDProcessArgs).trackParFunc = trackParFunc
				procArgs.(*bidsByAuctionIDProcessArgs).recordFinishFunc = recordFinishFunc
			}, &task)
		if ret != nil && ret.Success {
			ret.Latencies["src"] = src.GetLatency()
			ret.Latencies["sink"] = sink.GetLatency()
			ret.Latencies["filterBids"] = filterBids.GetLatency()
			ret.Latencies["bidsByAuctionIDMap"] = bidsByAuctionIDMap.GetLatency()
		}
		return ret
	}
	streamTaskArgs := transaction.StreamTaskArgs{
		ProcArgs: procArgs,
		Duration: time.Duration(sp.Duration) * time.Second,
	}
	ret := task.Process(ctx, &streamTaskArgs)
	if ret != nil && ret.Success {
		ret.Latencies["src"] = src.GetLatency()
		ret.Latencies["sink"] = sink.GetLatency()
		ret.Latencies["filterBids"] = filterBids.GetLatency()
		ret.Latencies["bidsByAuctionIDMap"] = bidsByAuctionIDMap.GetLatency()
	}
	return ret
}
