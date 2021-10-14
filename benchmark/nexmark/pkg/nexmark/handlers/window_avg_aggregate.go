package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream/processor"
	"time"

	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"

	"cs.utexas.edu/zjia/faas/types"
)

// groupBy(bid.Auction).
// windowBy(TimeWindow(10 s)).
// aggregate(bid.price)

type windowedAvg struct {
	env types.Environment
}

func NewWindowedAvg(env types.Environment) types.FuncHandler {
	return &windowedAvg{
		env: env,
	}
}

func (h *windowedAvg) Call(ctx context.Context, input []byte) ([]byte, error) {
	sp := &common.QueryInput{}
	err := json.Unmarshal(input, sp)
	if err != nil {
		fmt.Fprintf(os.Stderr, "unmarshal error")
		return nil, err
	}
	output := h.process(ctx, sp)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		fmt.Fprintf(os.Stderr, "marshal error")
		return nil, err
	}
	return utils.CompressData(encodedOutput), nil
}

func (h *windowedAvg) process(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	input_stream, err := sharedlog_stream.NewShardedSharedLogStream(ctx, h.env, sp.InputTopicName, sp.NumInPartition)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("NewShardedSharedLogStream failed: %v", err),
		}
	}
	output_stream, err := sharedlog_stream.NewShardedSharedLogStream(ctx, h.env, sp.OutputTopicName, sp.NumOutPartition)
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
			Message: fmt.Sprintf("get msg serde failed: %v", err),
		}
	}
	eventSerde, err := getEventSerde(sp.SerdeFormat)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("get evnet serde error: %v\n", err),
		}
	}
	var scSerde processor.Serde
	var vtSerde processor.Serde
	if sp.SerdeFormat == uint8(processor.JSON) {
		scSerde = ntypes.SumAndCountJSONSerde{}
		vtSerde = processor.ValueTimestampJSONSerde{
			ValJSONSerde: scSerde,
		}
	} else if sp.SerdeFormat == uint8(processor.MSGP) {
		scSerde = ntypes.SumAndCountMsgpSerde{}
		vtSerde = processor.ValueTimestampMsgpSerde{
			ValMsgpSerde: scSerde,
		}
	} else {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("serde format should be either json or msgp; but %v is given", sp.SerdeFormat),
		}
	}

	duration := time.Duration(sp.Duration) * time.Second
	inConfig := &sharedlog_stream.SharedLogStreamConfig{
		Timeout:      time.Duration(20) * time.Second,
		MsgDecoder:   msgSerde,
		KeyDecoder:   processor.Uint64Decoder{},
		ValueDecoder: eventSerde,
	}

	outConfig := &sharedlog_stream.StreamSinkConfig{
		MsgEncoder:   msgSerde,
		KeyEncoder:   processor.Uint64Encoder{},
		ValueEncoder: processor.Float64Serde{},
	}

	src := sharedlog_stream.NewShardedSharedLogStreamSource(input_stream, inConfig)
	sink := sharedlog_stream.NewShardedSharedLogStreamSink(output_stream, outConfig)
	timeWindows := processor.NewTimeWindowsNoGrace(time.Duration(10) * time.Second)

	winStoreMp := &processor.MaterializeParam{
		StoreName:  "windowed-avg-store",
		MsgSerde:   msgSerde,
		KeySerde:   processor.Uint64Serde{},
		ValueSerde: vtSerde,
		Changelog:  output_stream,
		ParNum:     sp.ParNum,
	}
	store := processor.NewInMemoryWindowStoreWithChangelog(
		timeWindows.MaxSize()+timeWindows.GracePeriodMs(), timeWindows.MaxSize(), winStoreMp)
	aggProc := processor.NewStreamWindowAggregateProcessor(store,
		processor.InitializerFunc(func() interface{} {
			return &ntypes.SumAndCount{
				Sum:   0,
				Count: 0,
			}
		}),
		processor.AggregatorFunc(func(key, value, aggregate interface{}) interface{} {
			val := value.(*ntypes.Event)
			agg := aggregate.(*ntypes.SumAndCount)
			return &ntypes.SumAndCount{
				Sum:   agg.Sum + val.Bid.Price,
				Count: agg.Count + 1,
			}
		}), timeWindows)
	calcAvg := processor.NewStreamMapValuesProcessor(
		processor.ValueMapperFunc(func(value interface{}) (interface{}, error) {
			val := value.(*ntypes.SumAndCount)
			return float64(val.Sum) / float64(val.Count), nil
		}))
	latencies := make([]int, 0, 128)
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
					Success:   true,
					Message:   err.Error(),
					Latencies: latencies,
					Duration:  time.Since(startTime).Seconds(),
				}
			}
			return &common.FnOutput{
				Success: false,
				Message: err.Error(),
			}
		}
		newMsgs, err := aggProc.ProcessAndReturn(msg)
		if err != nil {
			return &common.FnOutput{
				Success: false,
				Message: fmt.Sprintf("aggregate failed: %v\n", err),
			}
		}
		for _, newMsg := range newMsgs {
			avg, err := calcAvg.ProcessAndReturn(*newMsg)
			if err != nil {
				return &common.FnOutput{
					Success: false,
					Message: fmt.Sprintf("calculate avg failed: %v\n", err),
				}
			}
			err = sink.Sink(*avg, sp.ParNum)
			if err != nil {
				return &common.FnOutput{
					Success: false,
					Message: fmt.Sprintf("sink failed: %v", err),
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
