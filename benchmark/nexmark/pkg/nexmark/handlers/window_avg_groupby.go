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
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"time"

	"cs.utexas.edu/zjia/faas/types"
)

// groupBy(bid.Auction).
// windowBy(TimeWindow(10 s)).
// aggregate(bid.price)

type windowAvgGroupBy struct {
	env types.Environment
}

func NewWindowAvgGroupByHandler(env types.Environment) types.FuncHandler {
	return &windowAvgGroupBy{
		env: env,
	}
}

func (h *windowAvgGroupBy) Call(ctx context.Context, input []byte) ([]byte, error) {
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

func (h *windowAvgGroupBy) process(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	input_stream, err := sharedlog_stream.NewShardedSharedLogStream(ctx, h.env, sp.InputTopicName, uint8(sp.NumInPartition))
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("NewShardedSharedLogStream failed: %v", err),
		}
	}
	output_stream, err := sharedlog_stream.NewShardedSharedLogStream(ctx, h.env, sp.OutputTopicName, uint8(sp.NumOutPartition))
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("NewShardedSharedLogStream failed: %v", err),
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
		KeyEncoder:   commtypes.Uint64Encoder{},
		ValueEncoder: eventSerde,
	}

	src := sharedlog_stream.NewShardedSharedLogStreamSource(input_stream, inConfig)
	sink := sharedlog_stream.NewShardedSharedLogStreamSink(output_stream, outConfig)
	latencies := make([]int, 0, 128)
	consumeLatencies := make([]int, 0, 128)
	sinkLatencies := make([]int, 0, 128)
	startTime := time.Now()
	for {
		if duration != 0 && time.Since(startTime) >= duration {
			break
		}
		procStart := time.Now()
		msg, err := src.Consume(sp.ParNum)
		if err != nil {
			if errors.Is(err, sharedlog_stream.ErrStreamSourceTimeout) {
				return &common.FnOutput{
					Success: true,
					Message: err.Error(),
					Latencies: map[string][]int{
						"e2e":  latencies,
						"sink": sinkLatencies,
						"src":  consumeLatencies,
					},
					Duration: time.Since(startTime).Seconds(),
				}
			}
			return &common.FnOutput{
				Success: false,
				Message: err.Error(),
			}
		}
		consumeLat := time.Since(procStart)
		consumeLatencies = append(consumeLatencies, int(consumeLat.Microseconds()))
		val := msg.Value.(*ntypes.Event)
		if val.Etype == ntypes.BID {
			par := uint8(val.Bid.Auction % uint64(sp.NumOutPartition))
			newMsg := commtypes.Message{Key: val.Bid.Auction, Value: msg.Value}

			sinkStart := time.Now()
			err = sink.Sink(newMsg, par)
			if err != nil {
				return &common.FnOutput{
					Success: false,
					Message: fmt.Sprintf("sink failed: %v\n", err),
				}
			}
			sinkLat := time.Since(sinkStart)
			sinkLatencies = append(sinkLatencies, int(sinkLat.Microseconds()))
		}
		elapsed := time.Since(procStart)
		latencies = append(latencies, int(elapsed.Microseconds()))
	}
	return &common.FnOutput{
		Success:  true,
		Duration: time.Since(startTime).Seconds(),
		Latencies: map[string][]int{
			"e2e":  latencies,
			"sink": sinkLatencies,
			"src":  consumeLatencies,
		},
	}
}
