package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/sharedlog_stream"

	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"
	"sharedlog-stream/pkg/stream/processor"

	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/generator"

	"cs.utexas.edu/zjia/faas/types"
)

type nexmarkSourceHandler struct {
	env types.Environment
}

func NewNexmarkSource(env types.Environment) types.FuncHandler {
	return &nexmarkSourceHandler{
		env: env,
	}
}

func (h *nexmarkSourceHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	inputConfig := &ntypes.NexMarkConfigInput{}
	err := json.Unmarshal(input, inputConfig)
	if err != nil {
		return nil, err
	}
	output, err := eventGeneration(ctx, h.env, inputConfig)
	if err != nil {
		return nil, err
	}
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	return utils.CompressData(encodedOutput), nil
}

func eventGeneration(ctx context.Context, env types.Environment, inputConfig *ntypes.NexMarkConfigInput) (*ntypes.FnOutput, error) {
	stream, err := sharedlog_stream.NewSharedLogStream(ctx, env, inputConfig.TopicName)
	if err != nil {
		return &ntypes.FnOutput{
			Success: false,
			Message: fmt.Sprintf("NewSharedlogStream failed: %v", err),
		}, nil
	}
	nexmarkConfig, err := ntypes.ConvertToNexmarkConfiguration(inputConfig)
	if err != nil {
		return &ntypes.FnOutput{
			Success: false,
			Message: fmt.Sprintf("fail to convert to nexmark configuration: %v", err),
		}, nil
	}
	generatorConfig := generator.NewGeneratorConfig(nexmarkConfig, uint64(time.Now().Unix()*1000), 1, uint64(nexmarkConfig.NumEvents), 1)
	eventGenerator := generator.NewSimpleNexmarkGenerator(generatorConfig)
	latencies := make([]int, 0, 128)
	channel_url_cache := make(map[uint32]*generator.ChannelUrl)
	duration := time.Duration(inputConfig.Duration) * time.Second
	startTime := time.Now()
	var eventEncoder processor.Encoder
	var msgEncoder processor.MsgEncoder
	if inputConfig.SerdeFormat == uint8(common.JSON) {
		eventEncoder = ntypes.EventJSONSerde{}
		msgEncoder = common.MessageSerializedJSONSerde{}
	} else if inputConfig.SerdeFormat == uint8(common.MSGP) {
		eventEncoder = ntypes.EventMsgpSerde{}
		msgEncoder = common.MessageSerializedMsgpSerde{}
	} else {
		return &ntypes.FnOutput{
			Success: false,
			Message: fmt.Sprintf("serde format should be either json or msgp; but %v is given", inputConfig.SerdeFormat),
		}, nil
	}

	for {
		if !eventGenerator.HasNext() {
			break
		}
		if duration != 0 && time.Since(startTime) >= duration {
			break
		}
		now := time.Now().Unix()
		nextEvent, err := eventGenerator.NextEvent(ctx, channel_url_cache)
		if err != nil {
			return &ntypes.FnOutput{
				Success: false,
				Message: fmt.Sprintf("next event failed: %v", err),
			}, nil
		}
		wtsSec := nextEvent.WallclockTimestamp / 1000.0
		if wtsSec > uint64(now) {
			time.Sleep(time.Duration(wtsSec-uint64(now)) * time.Second)
		}
		encoded, err := eventEncoder.Encode(nextEvent.Event)
		if err != nil {
			return &ntypes.FnOutput{
				Success: false,
				Message: fmt.Sprintf("event serialization failed: %v", err),
			}, nil
		}
		msgEncoded, err := msgEncoder.Encode(nil, encoded)
		if err != nil {
			return &ntypes.FnOutput{
				Success: false,
				Message: fmt.Sprintf("msg serialization failed: %v", err),
			}, nil
		}
		pushStart := time.Now()
		_, err = stream.Push(msgEncoded)
		if err != nil {
			return &ntypes.FnOutput{
				Success: false,
				Message: fmt.Sprintf("stream push failed: %v", err),
			}, nil
		}
		elapsed := time.Since(pushStart)
		latencies = append(latencies, int(elapsed.Microseconds()))
	}
	return &ntypes.FnOutput{
		Success:   true,
		Duration:  time.Since(startTime).Seconds(),
		Latencies: latencies,
	}, nil
}