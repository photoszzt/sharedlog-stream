package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream/processor"
	"time"

	"cs.utexas.edu/zjia/faas/types"
)

type q5Count struct {
	env types.Environment
}

func NewQ5Count(env types.Environment) *q5Count {
	return &q5Count{
		env: env,
	}
}

func (h *q5Count) Call(ctx context.Context, input []byte) ([]byte, error) {
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

func (h *q5Count) process(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
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
	msgSerde, err := processor.GetMsgSerde(sp.SerdeFormat)
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
		Timeout:      time.Duration(sp.Duration),
		KeyDecoder:   processor.Uint64Serde{},
		ValueDecoder: eventSerde,
		MsgDecoder:   msgSerde,
	}
	outConfig := &sharedlog_stream.StreamSinkConfig{
		MsgEncoder:   msgSerde,
		KeyEncoder:   processor.Uint64Encoder{},
		ValueEncoder: processor.Uint64Encoder{},
	}
	src := sharedlog_stream.NewShardedSharedLogStreamSource(input_stream, inConfig)
	sink := sharedlog_stream.NewShardedSharedLogStreamSink(output_stream, outConfig)
	hopWindow := processor.NewTimeWindowsNoGrace(time.Duration(10) * time.Second).AdvanceBy(time.Duration(2) * time.Second)
	countMp := &processor.MaterializeParam{
		KeySerde:   processor.Uint64Serde{},
		ValueSerde: processor.Uint64Serde{},
		MsgSerde:   msgSerde,
		StoreName:  "q5-count-store",
		Changelog:  output_stream,
	}
	countWindowStore := processor.NewInMemoryWindowStoreWithChangelog(
		hopWindow.MaxSize()+hopWindow.GracePeriodMs(),
		hopWindow.MaxSize(), countMp,
	)
	countProc := processor.NewStreamWindowAggregateProcessor(countWindowStore,
		processor.InitializerFunc(func() interface{} { return 0 }),
		processor.AggregatorFunc(func(key, value, aggregate interface{}) interface{} {
			val := aggregate.(uint64)
			return val + 1
		}), hopWindow)
	latencies := make([]int, 0, 128)
	startTime := time.Now()
	for {
		if duration != 0 && time.Since(startTime) >= duration {
			break
		}
		procStart := time.Now()
		msg, err := src.Consume(sp.ParNum)
		if err != nil {
			return &common.FnOutput{
				Success: false,
				Message: err.Error(),
			}
		}
		countMsgs, err := countProc.ProcessAndReturn(msg)
		if err != nil {
			return &common.FnOutput{
				Success: false,
				Message: err.Error(),
			}
		}
		for _, countMsg := range countMsgs {
			err = sink.Sink(*countMsg, sp.ParNum)
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
		Success:   true,
		Duration:  time.Since(startTime).Seconds(),
		Latencies: latencies,
	}
}
