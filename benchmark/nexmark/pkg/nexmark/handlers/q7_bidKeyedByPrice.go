package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sharedlog-stream/benchmark/common"
	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"time"

	"cs.utexas.edu/zjia/faas/types"
)

type q7BidKeyedByPrice struct {
	env types.Environment
}

func NewQ7BidKeyedByPriceHandler(env types.Environment) types.FuncHandler {
	return &q7BidKeyedByPrice{
		env: env,
	}
}

func (h *q7BidKeyedByPrice) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &common.QueryInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output := h.process(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	return utils.CompressData(encodedOutput), nil
}

func (h *q7BidKeyedByPrice) process(ctx context.Context, input *common.QueryInput) *common.FnOutput {
	input_stream, output_stream, err := getShardedInputOutputStreams(ctx, h.env, input)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}

	msgSerde, err := commtypes.GetMsgSerde(input.SerdeFormat)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	eventSerde, err := getEventSerde(input.SerdeFormat)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	inConfig := &sharedlog_stream.SharedLogStreamConfig{
		Timeout:      time.Duration(input.Duration) * time.Second,
		KeyDecoder:   commtypes.StringDecoder{},
		ValueDecoder: eventSerde,
		MsgDecoder:   msgSerde,
	}
	outConfig := &sharedlog_stream.StreamSinkConfig{
		MsgEncoder:   msgSerde,
		KeyEncoder:   commtypes.Uint64Encoder{},
		ValueEncoder: eventSerde,
	}

	duration := time.Duration(input.Duration) * time.Second
	src := processor.NewMeteredSource(sharedlog_stream.NewShardedSharedLogStreamSource(input_stream, inConfig))
	sink := processor.NewMeteredSink(sharedlog_stream.NewShardedSharedLogStreamSink(output_stream, outConfig))

	bid := processor.NewMeteredProcessor(processor.NewStreamFilterProcessor(processor.PredicateFunc(func(msg *commtypes.Message) (bool, error) {
		event := msg.Value.(*ntypes.Event)
		return event.Etype == ntypes.BID, nil
	})))
	bidKeyedByPrice := processor.NewMeteredProcessor(processor.NewStreamMapProcessor(processor.MapperFunc(func(msg commtypes.Message) (commtypes.Message, error) {
		event := msg.Value.(*ntypes.Event)
		return commtypes.Message{Key: event.Bid.Price, Value: msg.Value, Timestamp: msg.Timestamp}, nil
	})))
	latencies := make([]int, 0, 128)
	startTime := time.Now()
	for {
		if duration != 0 && time.Since(startTime) >= duration {
			break
		}
		procStart := time.Now()
		msg, err := src.Consume(ctx, input.ParNum)
		if err != nil {
			if errors.Is(err, sharedlog_stream.ErrStreamSourceTimeout) {
				return &common.FnOutput{
					Success:  true,
					Message:  err.Error(),
					Duration: time.Since(startTime).Seconds(),
					Latencies: map[string][]int{
						"e2e":       latencies,
						"sink":      sink.GetLatency(),
						"src":       src.GetLatency(),
						"filterBid": bid.GetLatency(),
						"selectKey": bidKeyedByPrice.GetLatency(),
					},
				}
			}
			return &common.FnOutput{
				Success: false,
				Message: err.Error(),
			}
		}
		bidMsg, err := bid.ProcessAndReturn(ctx, msg)
		if err != nil {
			return &common.FnOutput{
				Success: false,
				Message: fmt.Sprintf("filter bid err: %v", err),
			}
		}
		if bidMsg != nil {
			mappedKey, err := bidKeyedByPrice.ProcessAndReturn(ctx, bidMsg[0])
			if err != nil {
				return &common.FnOutput{
					Success: false,
					Message: fmt.Sprintf("bid keyed by price error: %v\n", err),
				}
			}
			key := mappedKey[0].Key.(uint64)
			par := uint8(key % uint64(input.NumOutPartition))
			err = sink.Sink(ctx, mappedKey[0], par)
			if err != nil {
				return &common.FnOutput{
					Success: false,
					Message: fmt.Sprintf("sink err: %v\n", err),
				}
			}
		}
		elapsed := time.Since(procStart)
		latencies = append(latencies, int(elapsed.Microseconds()))
	}
	return &common.FnOutput{
		Success:  true,
		Duration: time.Since(startTime).Seconds(),
		Latencies: map[string][]int{
			"e2e":       latencies,
			"sink":      sink.GetLatency(),
			"src":       src.GetLatency(),
			"filterBid": bid.GetLatency(),
			"selectKey": bidKeyedByPrice.GetLatency(),
		},
	}
}
