package wordcount

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"
	"time"

	"cs.utexas.edu/zjia/faas/types"
)

type wordcountCounterAgg struct {
	env types.Environment
}

func NewWordCountCounterAgg(env types.Environment) *wordcountCounterAgg {
	return &wordcountCounterAgg{
		env: env,
	}
}

func (h *wordcountCounterAgg) Call(ctx context.Context, input []byte) ([]byte, error) {
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

func (h *wordcountCounterAgg) process(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	input_stream, err := sharedlog_stream.NewShardedSharedLogStream(h.env, sp.InputTopicName, sp.NumInPartition)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("NewShardedSharedLogStream failed: %v", err),
		}
	}
	err = input_stream.InitStream(ctx)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("InitStream failed: %v", err),
		}
	}
	output_stream, err := sharedlog_stream.NewShardedSharedLogStream(h.env, sp.OutputTopicName, sp.NumOutPartition)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("NewShardedSharedLogStream failed: %v", err),
		}
	}
	err = output_stream.InitStream(ctx)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("InitStream failed: %v", err),
		}
	}

	var vtSerde commtypes.Serde
	if sp.SerdeFormat == uint8(commtypes.JSON) {
		vtSerde = commtypes.ValueTimestampJSONSerde{
			ValJSONSerde: commtypes.Uint64Serde{},
		}
	} else if sp.SerdeFormat == uint8(commtypes.MSGP) {
		vtSerde = commtypes.ValueTimestampMsgpSerde{
			ValMsgpSerde: commtypes.Uint64Serde{},
		}
	} else {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("serde format should be either json or msgp; but %v is given", sp.SerdeFormat),
		}
	}
	msgSerde, err := commtypes.GetMsgSerde(sp.SerdeFormat)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	inConfig := &sharedlog_stream.SharedLogStreamConfig{
		Timeout:      time.Duration(sp.Duration) * time.Second,
		KeyDecoder:   commtypes.StringDecoder{},
		ValueDecoder: commtypes.StringDecoder{},
		MsgDecoder:   msgSerde,
	}
	outConfig := &sharedlog_stream.StreamSinkConfig{
		KeyEncoder:   commtypes.StringEncoder{},
		ValueEncoder: commtypes.Uint64Encoder{},
		MsgEncoder:   msgSerde,
	}
	src := sharedlog_stream.NewShardedSharedLogStreamSource(input_stream, inConfig)
	sink := sharedlog_stream.NewShardedSharedLogStreamSink(output_stream, outConfig)
	mp := &store.MaterializeParam{
		KeySerde:   commtypes.StringSerde{},
		ValueSerde: vtSerde,
		MsgSerde:   msgSerde,
		StoreName:  sp.OutputTopicName,
		Changelog:  output_stream,
	}

	store := store.NewInMemoryKeyValueStoreWithChangelog(mp)
	p := processor.NewMeteredProcessor(processor.NewStreamAggregateProcessor(store,
		processor.InitializerFunc(func() interface{} {
			return uint64(0)
		}),
		processor.AggregatorFunc(func(key interface{}, value interface{}, agg interface{}) interface{} {
			aggVal := agg.(uint64)
			return aggVal + 1
		})))
	srcLatencies := make([]int, 0, 128)
	sinkLatencies := make([]int, 0, 128)

	latencies := make([]int, 0, 128)
	duration := time.Duration(sp.Duration) * time.Second
	startTime := time.Now()
	for {
		if duration != 0 && time.Since(startTime) >= duration {
			break
		}
		procStart := time.Now()
		msg, err := src.Consume(ctx, sp.ParNum)
		if err != nil {
			if errors.Is(err, sharedlog_stream.ErrStreamSourceTimeout) {
				return &common.FnOutput{
					Success:   true,
					Message:   err.Error(),
					Latencies: map[string][]int{"e2e": latencies},
					Duration:  time.Since(startTime).Seconds(),
				}
			}
			return &common.FnOutput{
				Success: false,
				Message: err.Error(),
			}
		}
		srcLat := time.Since(procStart)
		srcLatencies = append(srcLatencies, int(srcLat.Microseconds()))
		ret, err := p.ProcessAndReturn(ctx, msg)
		if err != nil {
			return &common.FnOutput{
				Success: false,
				Message: err.Error(),
			}
		}
		sinkStart := time.Now()
		err = sink.Sink(ctx, ret[0], sp.ParNum)
		if err != nil {
			return &common.FnOutput{
				Success: false,
				Message: fmt.Sprintf("sink failed: %v\n", err),
			}
		}
		sinkLat := time.Since(sinkStart)
		sinkLatencies = append(sinkLatencies, int(sinkLat.Microseconds()))
		elapsed := time.Since(procStart)
		latencies = append(latencies, int(elapsed.Microseconds()))
	}
	return &common.FnOutput{
		Success:  true,
		Duration: time.Since(startTime).Seconds(),
		Latencies: map[string][]int{
			"e2e":   latencies,
			"src":   srcLatencies,
			"sink":  sinkLatencies,
			"count": p.GetLatency(),
		},
	}
}
