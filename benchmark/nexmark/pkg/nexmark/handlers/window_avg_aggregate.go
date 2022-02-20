package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
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
	output := h.windowavg_aggregate(ctx, sp)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		fmt.Fprintf(os.Stderr, "marshal error")
		return nil, err
	}
	return utils.CompressData(encodedOutput), nil
}

func (h *windowedAvg) getSrcSink(ctx context.Context, sp *common.QueryInput, msgSerde commtypes.MsgSerde) (*processor.MeteredSource, *processor.MeteredSink, error) {
	eventSerde, err := getEventSerde(sp.SerdeFormat)
	if err != nil {
		return nil, nil, fmt.Errorf("get evnet serde error: %v", err)
	}
	var wkSerde commtypes.Serde
	if sp.SerdeFormat == uint8(commtypes.JSON) {
		wkSerde = commtypes.WindowedKeyJSONSerde{
			KeyJSONSerde:    commtypes.Uint64Serde{},
			WindowJSONSerde: processor.TimeWindowJSONSerde{},
		}
	} else if sp.SerdeFormat == uint8(commtypes.MSGP) {
		wkSerde = commtypes.WindowedKeyMsgpSerde{
			KeyMsgpSerde:    commtypes.Uint64Serde{},
			WindowMsgpSerde: processor.TimeWindowMsgpSerde{},
		}
	} else {
		return nil, nil, fmt.Errorf("serde format should be either json or msgp; but %v is given", sp.SerdeFormat)
	}

	input_stream, output_stream, err := benchutil.GetShardedInputOutputStreams(ctx, h.env, sp, true)
	if err != nil {
		return nil, nil, err
	}
	inConfig := &sharedlog_stream.SharedLogStreamConfig{
		Timeout:      common.SrcConsumeTimeout,
		MsgDecoder:   msgSerde,
		KeyDecoder:   commtypes.Uint64Decoder{},
		ValueDecoder: eventSerde,
	}

	outConfig := &sharedlog_stream.StreamSinkConfig{
		MsgSerde:   msgSerde,
		KeySerde:   wkSerde,
		ValueSerde: commtypes.Float64Serde{},
	}

	src := processor.NewMeteredSource(sharedlog_stream.NewShardedSharedLogStreamSource(input_stream, inConfig))
	sink := processor.NewMeteredSink(sharedlog_stream.NewShardedSharedLogStreamSink(output_stream, outConfig))
	return src, sink, nil
}

func (h *windowedAvg) getAggProcessor(ctx context.Context, sp *common.QueryInput, msgSerde commtypes.MsgSerde) (*processor.MeteredProcessor, error) {
	var scSerde commtypes.Serde
	var vtSerde commtypes.Serde
	changelog_stream, err := sharedlog_stream.NewShardedSharedLogStream(h.env, "windowedAvgAgg_changelog", uint8(sp.NumOutPartition), commtypes.SerdeFormat(sp.SerdeFormat))
	if err != nil {
		return nil, fmt.Errorf("create changelog stream failed: %v", err)
	}
	/*
		err = changelog_stream.InitStream(ctx, true)
		if err != nil {
			return nil, fmt.Errorf("changelog stream init failed: %v", err)
		}
	*/
	if sp.SerdeFormat == uint8(commtypes.JSON) {
		scSerde = ntypes.SumAndCountJSONSerde{}
		vtSerde = commtypes.ValueTimestampJSONSerde{
			ValJSONSerde: scSerde,
		}
	} else if sp.SerdeFormat == uint8(commtypes.MSGP) {
		scSerde = ntypes.SumAndCountMsgpSerde{}
		vtSerde = commtypes.ValueTimestampMsgpSerde{
			ValMsgpSerde: scSerde,
		}
	} else {
		return nil, fmt.Errorf("serde format should be either json or msgp; but %v is given", sp.SerdeFormat)
	}

	timeWindows := processor.NewTimeWindowsNoGrace(time.Duration(10) * time.Second)
	winStoreMp := &store.MaterializeParam{
		StoreName:  "windowed-avg-store",
		MsgSerde:   msgSerde,
		KeySerde:   commtypes.Uint64Serde{},
		ValueSerde: vtSerde,
		Changelog:  changelog_stream,
		ParNum:     sp.ParNum,
	}

	store, err := store.NewInMemoryWindowStoreWithChangelog(
		timeWindows.MaxSize()+timeWindows.GracePeriodMs(), timeWindows.MaxSize(), false, winStoreMp)
	if err != nil {
		return nil, err
	}
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
	return aggProc, nil
}

func (h *windowedAvg) process(ctx context.Context, sp *common.QueryInput,
	src *processor.MeteredSource, sink *processor.MeteredSink,
	aggProc *processor.MeteredProcessor, calcAvg *processor.MeteredProcessor,
) *common.FnOutput {
	duration := time.Duration(sp.Duration) * time.Second
	latencies := make([]int, 0, 128)
	startTime := time.Now()
	for {
		if duration != 0 && time.Since(startTime) >= duration {
			break
		}
		procStart := time.Now()
		msgs, err := src.Consume(ctx, sp.ParNum)
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

		for _, msg := range msgs {
			newMsgs, err := aggProc.ProcessAndReturn(ctx, msg.Msg)
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
				err = sink.Sink(ctx, avg[0], sp.ParNum, false)
				if err != nil {
					return &common.FnOutput{
						Success: false,
						Message: fmt.Sprintf("sink failed: %v", err),
					}
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

func (h *windowedAvg) windowavg_aggregate(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	msgSerde, err := commtypes.GetMsgSerde(sp.SerdeFormat)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("get msg serde failed: %v", err),
		}
	}
	src, sink, err := h.getSrcSink(ctx, sp, msgSerde)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}

	aggProc, err := h.getAggProcessor(ctx, sp, msgSerde)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	calcAvg := processor.NewMeteredProcessor(processor.NewStreamMapValuesProcessor(
		processor.ValueMapperFunc(func(value interface{}) (interface{}, error) {
			val := value.(*ntypes.SumAndCount)
			return float64(val.Sum) / float64(val.Count), nil
		})))
	return h.process(ctx, sp, src, sink, aggProc, calcAvg)
}
