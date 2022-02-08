package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/hash"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"time"

	"cs.utexas.edu/zjia/faas/types"
)

type q7BidKeyedByPrice struct {
	env           types.Environment
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
	trackParFunc    func([]uint8) error
	parNum          uint8
	numOutPartition uint8
}

func (h *q7BidKeyedByPrice) process(ctx context.Context,
	argsTmp interface{},
) (map[string]uint64, *common.FnOutput) {
	args := argsTmp.(*q7BidKeyedByPriceProcessArgs)
	gotMsgs, err := args.src.Consume(ctx, args.parNum)
	if err != nil {
		if errors.Is(err, sharedlog_stream.ErrStreamSourceTimeout) {
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
			parTmp, ok := h.cHash.Get(key)
			if !ok {
				return h.currentOffset, &common.FnOutput{
					Success: false,
					Message: "fail to get output substream number",
				}
			}
			par := parTmp.(uint8)
			// par := uint8(key % uint64(args.numOutPartition))
			err = args.trackParFunc([]uint8{par})
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

func (h *q7BidKeyedByPrice) getSrcSink(
	sp *common.QueryInput,
	input_stream *sharedlog_stream.ShardedSharedLogStream,
	output_stream *sharedlog_stream.ShardedSharedLogStream,
) (*processor.MeteredSource, *processor.MeteredSink, error) {
	msgSerde, err := commtypes.GetMsgSerde(sp.SerdeFormat)
	if err != nil {
		return nil, nil, err
	}
	eventSerde, err := getEventSerde(sp.SerdeFormat)
	if err != nil {
		return nil, nil, err
	}
	inConfig := &sharedlog_stream.SharedLogStreamConfig{
		Timeout:      common.SrcConsumeTimeout,
		KeyDecoder:   commtypes.StringDecoder{},
		ValueDecoder: eventSerde,
		MsgDecoder:   msgSerde,
	}
	outConfig := &sharedlog_stream.StreamSinkConfig{
		MsgEncoder:   msgSerde,
		KeyEncoder:   commtypes.Uint64Encoder{},
		ValueEncoder: eventSerde,
	}
	src := processor.NewMeteredSource(sharedlog_stream.NewShardedSharedLogStreamSource(input_stream, inConfig))
	sink := processor.NewMeteredSink(sharedlog_stream.NewShardedSharedLogStreamSink(output_stream, outConfig))
	return src, sink, nil
}

func (h *q7BidKeyedByPrice) processQ7BidKeyedByPrice(ctx context.Context, input *common.QueryInput) *common.FnOutput {
	input_stream, output_stream, err := benchutil.GetShardedInputOutputStreams(ctx, h.env, input)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}

	src, sink, err := h.getSrcSink(input, input_stream, output_stream)
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
		trackParFunc:    sharedlog_stream.DefaultTrackParFunc,
	}

	task := sharedlog_stream.StreamTask{
		ProcessFunc: h.process,
	}

	for i := uint8(0); i < input.NumOutPartition; i++ {
		h.cHash.Add(i)
	}

	if input.EnableTransaction {
		srcs := make(map[string]processor.Source)
		srcs[input.InputTopicNames[0]] = src
		streamTaskArgs := sharedlog_stream.StreamTaskArgsTransaction{
			ProcArgs:        procArgs,
			Env:             h.env,
			Srcs:            srcs,
			OutputStream:    output_stream,
			QueryInput:      input,
			TransactionalId: fmt.Sprintf("q7BidKeyedByPrice-%s-%d-%s", input.InputTopicNames[0], input.ParNum, input.OutputTopicName),
			FixedOutParNum:  0,
		}
		tm, trackParFunc, err := sharedlog_stream.SetupTransactionManager(ctx, &streamTaskArgs)
		if err != nil {
			return &common.FnOutput{
				Success: false,
				Message: fmt.Sprintf("setup transaction manager failed: %v\n", err),
			}
		}
		procArgs.trackParFunc = trackParFunc
		ret := task.ProcessWithTransaction(ctx, tm, &streamTaskArgs)
		if ret != nil && ret.Success {
			ret.Latencies["src"] = src.GetLatency()
			ret.Latencies["sink"] = sink.GetLatency()
			ret.Latencies["bid"] = bid.GetLatency()
			ret.Latencies["bidKeyedByPrice"] = bidKeyedByPrice.GetLatency()
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
	}
	return ret
}
