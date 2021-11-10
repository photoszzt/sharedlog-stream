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
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"
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
	input_stream, output_stream, err := getShardedInputOutputStreams(ctx, h.env, sp)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	changelog_stream, err := sharedlog_stream.NewShardedSharedLogStream(h.env, "windowedAvgAgg_changelog", uint8(sp.NumOutPartition))
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("create changelog stream failed: %v\n", err),
		}
	}
	err = changelog_stream.InitStream(ctx)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("changelog stream init failed: %v\n", err),
		}
	}
	msgSerde, err := commtypes.GetMsgSerde(sp.SerdeFormat)
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
	var scSerde commtypes.Serde
	var vtSerde commtypes.Serde
	var wkSerde commtypes.Serde
	if sp.SerdeFormat == uint8(commtypes.JSON) {
		scSerde = ntypes.SumAndCountJSONSerde{}
		vtSerde = commtypes.ValueTimestampJSONSerde{
			ValJSONSerde: scSerde,
		}
		wkSerde = commtypes.WindowedKeyJSONSerde{
			KeyJSONSerde:    commtypes.Uint64Serde{},
			WindowJSONSerde: processor.TimeWindowJSONSerde{},
		}
	} else if sp.SerdeFormat == uint8(commtypes.MSGP) {
		scSerde = ntypes.SumAndCountMsgpSerde{}
		vtSerde = commtypes.ValueTimestampMsgpSerde{
			ValMsgpSerde: scSerde,
		}
		wkSerde = commtypes.WindowedKeyMsgpSerde{
			KeyMsgpSerde:    commtypes.Uint64Serde{},
			WindowMsgpSerde: processor.TimeWindowMsgpSerde{},
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
		KeyDecoder:   commtypes.Uint64Decoder{},
		ValueDecoder: eventSerde,
	}

	outConfig := &sharedlog_stream.StreamSinkConfig{
		MsgEncoder:   msgSerde,
		KeyEncoder:   wkSerde,
		ValueEncoder: commtypes.Float64Serde{},
	}

	src := processor.NewMeteredSource(sharedlog_stream.NewShardedSharedLogStreamSource(input_stream, inConfig))
	sink := processor.NewMeteredSink(sharedlog_stream.NewShardedSharedLogStreamSink(output_stream, outConfig))
	timeWindows := processor.NewTimeWindowsNoGrace(time.Duration(10) * time.Second)

	winStoreMp := &store.MaterializeParam{
		StoreName:  "windowed-avg-store",
		MsgSerde:   msgSerde,
		KeySerde:   commtypes.Uint64Serde{},
		ValueSerde: vtSerde,
		Changelog:  changelog_stream,
		ParNum:     sp.ParNum,
	}

	store := store.NewInMemoryWindowStoreWithChangelog(
		timeWindows.MaxSize()+timeWindows.GracePeriodMs(), timeWindows.MaxSize(), winStoreMp)

	aggProc := processor.NewMeteredProcessor(processor.NewStreamWindowAggregateProcessor(store,
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
		}), timeWindows))

	calcAvg := processor.NewMeteredProcessor(processor.NewStreamMapValuesProcessor(
		processor.ValueMapperFunc(func(value interface{}) (interface{}, error) {
			val := value.(*ntypes.SumAndCount)
			return float64(val.Sum) / float64(val.Count), nil
		})))

	latencies := make([]int, 0, 128)
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
					Success: true,
					Message: err.Error(),
					Latencies: map[string][]int{
						"e2e":     latencies,
						"agg":     aggProc.GetLatency(),
						"calcAvg": calcAvg.GetLatency(),
						"src":     src.GetLatency(),
						"sink":    sink.GetLatency(),
					},
					Duration: time.Since(startTime).Seconds(),
				}
			}
			return &common.FnOutput{
				Success: false,
				Message: err.Error(),
			}
		}
		newMsgs, err := aggProc.ProcessAndReturn(ctx, msg)
		if err != nil {
			return &common.FnOutput{
				Success: false,
				Message: fmt.Sprintf("aggregate failed: %v\n", err),
			}
		}
		for _, newMsg := range newMsgs {
			avg, err := calcAvg.ProcessAndReturn(ctx, newMsg)
			if err != nil {
				return &common.FnOutput{
					Success: false,
					Message: fmt.Sprintf("calculate avg failed: %v\n", err),
				}
			}
			err = sink.Sink(ctx, avg[0], sp.ParNum)
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
		Success:  true,
		Duration: time.Since(startTime).Seconds(),
		Latencies: map[string][]int{
			"e2e":     latencies,
			"agg":     aggProc.GetLatency(),
			"calcAvg": calcAvg.GetLatency(),
			"src":     src.GetLatency(),
			"sink":    sink.GetLatency(),
		},
	}
}
