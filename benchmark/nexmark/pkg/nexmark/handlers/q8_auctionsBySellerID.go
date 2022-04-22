package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/hash"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/transaction"
	"sync"
	"time"

	"cs.utexas.edu/zjia/faas/types"
	"golang.org/x/xerrors"
)

type q8AuctionsBySellerIDHandler struct {
	env      types.Environment
	cHashMu  sync.RWMutex
	cHash    *hash.ConsistentHash
	funcName string
}

func NewQ8AuctionsBySellerIDHandler(env types.Environment, funcName string) types.FuncHandler {
	return &q8AuctionsBySellerIDHandler{
		env:      env,
		cHash:    hash.NewConsistentHash(),
		funcName: funcName,
	}
}

func (h *q8AuctionsBySellerIDHandler) process(
	ctx context.Context,
	t *transaction.StreamTask,
	argsTmp interface{},
) (map[string]uint64, *common.FnOutput) {
	args := argsTmp.(*AuctionsBySellerIDProcessArgs)
	return transaction.CommonProcess(ctx, t, args, func(t *transaction.StreamTask, msg commtypes.MsgAndSeq) error {
		t.CurrentOffset[args.src.TopicName()] = msg.LogSeqNum
		if msg.MsgArr != nil {
			for _, subMsg := range msg.MsgArr {
				if subMsg.Value == nil {
					continue
				}
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

func (h *q8AuctionsBySellerIDHandler) procMsg(ctx context.Context, msg commtypes.Message, args *AuctionsBySellerIDProcessArgs) error {
	event := msg.Value.(*ntypes.Event)
	ts, err := event.ExtractStreamTime()
	if err != nil {
		return fmt.Errorf("fail to extract timestamp: %v", err)
	}
	msg.Timestamp = ts
	filteredMsgs, err := args.filterAuctions.ProcessAndReturn(ctx, msg)
	if err != nil {
		return fmt.Errorf("filterAuctions err: %v", err)
	}
	for _, filteredMsg := range filteredMsgs {
		changeKeyedMsg, err := args.auctionsBySellerIDMap.ProcessAndReturn(ctx, filteredMsg)
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
		// fmt.Fprintf(os.Stderr, "append to %d with msg %v\n", par, changeKeyedMsg[0])
		err = args.sink.Sink(ctx, changeKeyedMsg[0], par, false)
		if err != nil {
			return fmt.Errorf("sink err: %v", err)
		}
	}
	return nil
}

func (h *q8AuctionsBySellerIDHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &common.QueryInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output := h.q8AuctionsBySellerID(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	return utils.CompressData(encodedOutput), nil
}

func (h *q8AuctionsBySellerIDHandler) q8AuctionsBySellerID(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	input_stream, output_streams, err := benchutil.GetShardedInputOutputStreams(ctx, h.env, sp, false)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("get input output stream failed: %v", err),
		}
	}
	debug.Assert(len(output_streams) == 1, "expected only one output stream")
	src, sink, msgSerde, err := CommonGetSrcSink(ctx, sp, input_stream, output_streams[0])
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}

	filterAuctions := processor.NewMeteredProcessor(processor.NewStreamFilterProcessor(processor.PredicateFunc(
		func(m *commtypes.Message) (bool, error) {
			event := m.Value.(*ntypes.Event)
			return event.Etype == ntypes.AUCTION, nil
		})))

	auctionsBySellerIDMap := processor.NewMeteredProcessor(processor.NewStreamMapProcessor(
		processor.MapperFunc(func(msg commtypes.Message) (commtypes.Message, error) {
			event := msg.Value.(*ntypes.Event)
			return commtypes.Message{Key: event.NewAuction.Seller, Value: msg.Value, Timestamp: msg.Timestamp}, nil
		})))

	procArgs := &AuctionsBySellerIDProcessArgs{
		src:                   src,
		sink:                  sink,
		filterAuctions:        filterAuctions,
		auctionsBySellerIDMap: auctionsBySellerIDMap,
		parNum:                sp.ParNum,
		trackParFunc:          transaction.DefaultTrackSubstreamFunc,
		recordFinishFunc:      transaction.DefaultRecordPrevInstanceFinishFunc,
		curEpoch:              sp.ScaleEpoch,
		funcName:              h.funcName,
	}

	task := transaction.StreamTask{
		ProcessFunc:   h.process,
		CurrentOffset: make(map[string]uint64),
	}

	transaction.SetupConsistentHash(&h.cHashMu, h.cHash, sp.NumOutPartitions[0])

	if sp.EnableTransaction {
		srcs := make(map[string]processor.Source)
		srcs[sp.InputTopicNames[0]] = src
		streamTaskArgs := transaction.StreamTaskArgsTransaction{
			ProcArgs:      procArgs,
			Env:           h.env,
			MsgSerde:      msgSerde,
			Srcs:          srcs,
			OutputStreams: output_streams,
			QueryInput:    sp,
			TransactionalId: fmt.Sprintf("%s-%s-%d-%s", h.funcName,
				sp.InputTopicNames[0], sp.ParNum, sp.OutputTopicNames[0]),
			KVChangelogs:          nil,
			WindowStoreChangelogs: nil,
		}
		ret := transaction.SetupManagersAndProcessTransactional(ctx, h.env, &streamTaskArgs,
			func(procArgs interface{}, trackParFunc transaction.TrackKeySubStreamFunc, recordFinishFunc transaction.RecordPrevInstanceFinishFunc) {
				procArgs.(*AuctionsBySellerIDProcessArgs).trackParFunc = trackParFunc
				procArgs.(*AuctionsBySellerIDProcessArgs).recordFinishFunc = recordFinishFunc
			}, &task)
		if ret != nil && ret.Success {
			ret.Latencies["src"] = src.GetLatency()
			ret.Latencies["sink"] = sink.GetLatency()
			ret.Latencies["filterAuctions"] = filterAuctions.GetLatency()
			ret.Latencies["auctionsBySellerIDMap"] = auctionsBySellerIDMap.GetLatency()
			ret.Consumed["src"] = src.GetCount()
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
		ret.Latencies["filterAuctions"] = filterAuctions.GetLatency()
		ret.Latencies["auctionsBySellerIDMap"] = auctionsBySellerIDMap.GetLatency()
		ret.Consumed["src"] = src.GetCount()
	}
	return ret
}
