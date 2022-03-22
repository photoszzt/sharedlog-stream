package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/errors"
	"sharedlog-stream/pkg/hash"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sync"
	"time"

	"cs.utexas.edu/zjia/faas/types"
	"golang.org/x/xerrors"
)

type q7BidKeyedByPrice struct {
	env           types.Environment
	cHashMu       sync.RWMutex
	cHash         *hash.ConsistentHash
	currentOffset map[string]uint64
}

func NewQ7BidKeyedByPriceHandler(env types.Environment) types.FuncHandler {
	return &q7BidKeyedByPrice{
		env:           env,
		cHash:         hash.NewConsistentHash(),
		currentOffset: make(map[string]uint64),
	}
}

func (h *q7BidKeyedByPrice) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &common.QueryInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output := h.processQ7BidKeyedByPrice(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	return utils.CompressData(encodedOutput), nil
}

type q7BidKeyedByPriceProcessArgs struct {
	src             *processor.MeteredSource
	sink            *processor.MeteredSink
	bid             *processor.MeteredProcessor
	bidKeyedByPrice *processor.MeteredProcessor
	output_stream   *sharedlog_stream.ShardedSharedLogStream
	trackParFunc    sharedlog_stream.TrackKeySubStreamFunc
	parNum          uint8
	numOutPartition uint8
}

func (h *q7BidKeyedByPrice) process(ctx context.Context,
	argsTmp interface{},
) (map[string]uint64, *common.FnOutput) {
	args := argsTmp.(*q7BidKeyedByPriceProcessArgs)
	gotMsgs, err := args.src.Consume(ctx, args.parNum)
	if err != nil {
		if xerrors.Is(err, errors.ErrStreamSourceTimeout) {
			return h.currentOffset, &common.FnOutput{
				Success: true,
				Message: err.Error(),
			}
		}
		return h.currentOffset, &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}

	for _, msg := range gotMsgs {
		h.currentOffset[args.src.TopicName()] = msg.LogSeqNum
		event := msg.Msg.Value.(*ntypes.Event)
		ts, err := event.ExtractStreamTime()
		if err != nil {
			return h.currentOffset, &common.FnOutput{
				Success: false,
				Message: fmt.Sprintf("fail to extract timestamp: %v", err),
			}
		}
		msg.Msg.Timestamp = ts
		bidMsg, err := args.bid.ProcessAndReturn(ctx, msg.Msg)
		if err != nil {
			return h.currentOffset, &common.FnOutput{
				Success: false,
				Message: fmt.Sprintf("filter bid err: %v", err),
			}
		}
		if bidMsg != nil {
			mappedKey, err := args.bidKeyedByPrice.ProcessAndReturn(ctx, bidMsg[0])
			if err != nil {
				return h.currentOffset, &common.FnOutput{
					Success: false,
					Message: fmt.Sprintf("bid keyed by price error: %v\n", err),
				}
			}
			key := mappedKey[0].Key.(uint64)
			h.cHashMu.RLock()
			parTmp, ok := h.cHash.Get(key)
			h.cHashMu.RUnlock()
			if !ok {
				return h.currentOffset, &common.FnOutput{
					Success: false,
					Message: "fail to get output substream number",
				}
			}
			par := parTmp.(uint8)
			// par := uint8(key % uint64(args.numOutPartition))
			err = args.trackParFunc(ctx, key, args.sink.KeySerde(), args.sink.TopicName(), par)
			if err != nil {
				return h.currentOffset, &common.FnOutput{
					Success: false,
					Message: fmt.Sprintf("track par err: %v\n", err),
				}
			}
			err = args.sink.Sink(ctx, mappedKey[0], par, false)
			if err != nil {
				return h.currentOffset, &common.FnOutput{
					Success: false,
					Message: fmt.Sprintf("sink err: %v\n", err),
				}
			}
		}
	}
	return h.currentOffset, nil
}

func (h *q7BidKeyedByPrice) processQ7BidKeyedByPrice(ctx context.Context, input *common.QueryInput) *common.FnOutput {
	input_stream, output_stream, err := benchutil.GetShardedInputOutputStreams(ctx, h.env, input, false)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}

	src, sink, msgSerde, err := getSrcSinkUint64Key(input, input_stream, output_stream)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}

	bid := processor.NewMeteredProcessor(processor.NewStreamFilterProcessor(processor.PredicateFunc(func(msg *commtypes.Message) (bool, error) {
		event := msg.Value.(*ntypes.Event)
		return event.Etype == ntypes.BID, nil
	})))
	bidKeyedByPrice := processor.NewMeteredProcessor(processor.NewStreamMapProcessor(processor.MapperFunc(func(msg commtypes.Message) (commtypes.Message, error) {
		event := msg.Value.(*ntypes.Event)
		return commtypes.Message{Key: event.Bid.Price, Value: msg.Value, Timestamp: msg.Timestamp}, nil
	})))

	procArgs := &q7BidKeyedByPriceProcessArgs{
		src:             src,
		sink:            sink,
		bid:             bid,
		bidKeyedByPrice: bidKeyedByPrice,
		output_stream:   output_stream,
		parNum:          input.ParNum,
		numOutPartition: input.NumOutPartition,
		trackParFunc:    sharedlog_stream.DefaultTrackSubstreamFunc,
	}

	task := sharedlog_stream.StreamTask{
		ProcessFunc: h.process,
	}

	sharedlog_stream.SetupConsistentHash(&h.cHashMu, h.cHash, input.NumOutPartition)

	if input.EnableTransaction {
		srcs := make(map[string]processor.Source)
		srcs[input.InputTopicNames[0]] = src
		streamTaskArgs := sharedlog_stream.StreamTaskArgsTransaction{
			ProcArgs:              procArgs,
			Env:                   h.env,
			MsgSerde:              msgSerde,
			Srcs:                  srcs,
			OutputStream:          output_stream,
			QueryInput:            input,
			TransactionalId:       fmt.Sprintf("q7BidKeyedByPrice-%s-%d-%s", input.InputTopicNames[0], input.ParNum, input.OutputTopicName),
			FixedOutParNum:        0,
			KVChangelogs:          nil,
			WindowStoreChangelogs: nil,
			CHash:                 h.cHash,
			CHashMu:               &h.cHashMu,
		}
		ret := sharedlog_stream.SetupManagersAndProcessTransactional(ctx, h.env, &streamTaskArgs,
			func(procArgs interface{}, trackParFunc sharedlog_stream.TrackKeySubStreamFunc) {
				procArgs.(*q7BidKeyedByPriceProcessArgs).trackParFunc = trackParFunc
			}, &task)
		if ret != nil && ret.Success {
			ret.Latencies["src"] = src.GetLatency()
			ret.Latencies["sink"] = sink.GetLatency()
			ret.Latencies["bid"] = bid.GetLatency()
			ret.Latencies["bidKeyedByPrice"] = bidKeyedByPrice.GetLatency()
			ret.Consumed["src"] = src.GetCount()
		}
		return ret
	}
	streamTaskArgs := sharedlog_stream.StreamTaskArgs{
		ProcArgs: procArgs,
		Duration: time.Duration(input.Duration) * time.Second,
	}
	ret := task.Process(ctx, &streamTaskArgs)
	if ret != nil && ret.Success {
		ret.Latencies["src"] = src.GetLatency()
		ret.Latencies["sink"] = sink.GetLatency()
		ret.Latencies["bid"] = bid.GetLatency()
		ret.Latencies["bidKeyedByPrice"] = bidKeyedByPrice.GetLatency()
		ret.Consumed["src"] = src.GetCount()
	}
	return ret
}
