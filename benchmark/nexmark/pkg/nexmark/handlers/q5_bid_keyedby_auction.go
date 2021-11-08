package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"sharedlog-stream/benchmark/common"
	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"time"

	"cs.utexas.edu/zjia/faas/types"
)

type bidKeyedByAuction struct {
	env types.Environment
}

func NewBidKeyedByAuctionHandler(env types.Environment) types.FuncHandler {
	return &bidKeyedByAuction{
		env: env,
	}
}

func (h *bidKeyedByAuction) Call(ctx context.Context, input []byte) ([]byte, error) {
	sp := &common.QueryInput{}
	err := json.Unmarshal(input, sp)
	if err != nil {
		return nil, err
	}
	output := h.process(ctx, sp)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		return nil, err
	}
	return utils.CompressData(encodedOutput), nil
}

func (h *bidKeyedByAuction) process(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	input_stream, output_stream, err := getShardedInputOutputStreams(ctx, h.env, sp)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}

	msgSerde, err := commtypes.GetMsgSerde(sp.SerdeFormat)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	eventSerde, err := getEventSerde(sp.SerdeFormat)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	duration := time.Duration(sp.Duration) * time.Second
	inConfig := &sharedlog_stream.SharedLogStreamConfig{
		Timeout:      duration,
		MsgDecoder:   msgSerde,
		KeyDecoder:   commtypes.StringDecoder{},
		ValueDecoder: eventSerde,
	}
	outConfig := &sharedlog_stream.StreamSinkConfig{
		MsgEncoder:   msgSerde,
		ValueEncoder: eventSerde,
		KeyEncoder:   commtypes.Uint64Encoder{},
	}

	src := processor.NewMeteredSource(sharedlog_stream.NewShardedSharedLogStreamSource(input_stream, inConfig))
	sink := processor.NewMeteredSink(sharedlog_stream.NewShardedSharedLogStreamSink(output_stream, outConfig))
	latencies := make([]int, 0, 128)

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
	startTime := time.Now()
	for {
		if duration != 0 && time.Since(startTime) >= duration {
			break
		}
		procStart := time.Now()
		msg, _, err := src.Consume(ctx, sp.ParNum)
		if err != nil {
			if errors.Is(err, sharedlog_stream.ErrStreamSourceTimeout) {
				return &common.FnOutput{
					Success:  true,
					Message:  err.Error(),
					Duration: time.Since(startTime).Seconds(),
					Latencies: map[string][]int{
						"e2e":       latencies,
						"src":       src.GetLatency(),
						"sink":      sink.GetLatency(),
						"filterBid": filterBid.GetLatency(),
						"selectKey": selectKey.GetLatency(),
					},
				}
			}
			return &common.FnOutput{
				Success: false,
				Message: err.Error(),
			}
		}
		bidMsg, err := filterBid.ProcessAndReturn(ctx, msg)
		if err != nil {
			return &common.FnOutput{
				Success: false,
				Message: err.Error(),
			}
		}
		if bidMsg != nil {
			mappedKey, err := selectKey.ProcessAndReturn(ctx, bidMsg[0])
			if err != nil {
				return &common.FnOutput{
					Success: false,
					Message: err.Error(),
				}
			}
			key := mappedKey[0].Key.(uint64)
			par := uint8(key % uint64(sp.NumOutPartition))
			err = sink.Sink(ctx, mappedKey[0], par)
			if err != nil {
				return &common.FnOutput{
					Success: false,
					Message: err.Error(),
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
			"src":       src.GetLatency(),
			"sink":      sink.GetLatency(),
			"filterBid": filterBid.GetLatency(),
			"selectKey": selectKey.GetLatency(),
		},
	}
}
