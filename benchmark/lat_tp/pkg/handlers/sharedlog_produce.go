package handlers

import (
	"context"
	"encoding/json"
	"os"
	"sharedlog-stream/benchmark/common"
	datatype "sharedlog-stream/benchmark/lat_tp/pkg/data_type"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"time"

	"cs.utexas.edu/zjia/faas/types"
)

type sharedlogProduceBenchHandler struct {
	env types.Environment
}

func NewSharedlogProduceBenchHandler(env types.Environment) types.FuncHandler {
	return &sharedlogProduceBenchHandler{
		env: env,
	}
}

func (h *sharedlogProduceBenchHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &common.SourceParam{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output := h.sharedlogProduceBench(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	return utils.CompressData(encodedOutput), nil
}

func (h *sharedlogProduceBenchHandler) sharedlogProduceBench(ctx context.Context, sp *common.SourceParam) *common.FnOutput {
	content, err := os.ReadFile(sp.FileName)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	latencies := make([]int, 0, 128)
	numEvents := sp.NumEvents
	duration := time.Duration(sp.Duration) * time.Second
	startTime := time.Now()
	nEmitEvent := uint32(0)
	stream, err := sharedlog_stream.NewShardedSharedLogStream(h.env, sp.TopicName, sp.NumOutPartition, commtypes.SerdeFormat(sp.SerdeFormat))
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	var ptSerde datatype.PayloadTsMsgpSerde
	for {
		if (duration != 0 && time.Since(startTime) >= duration) || (numEvents != 0 && nEmitEvent == numEvents) {
			break
		}
		procStart := time.Now()
		parNum := nEmitEvent % uint32(sp.NumOutPartition)
		pt := datatype.PayloadTs{
			Payload: content,
			Ts:      time.Now().UnixMicro(),
		}
		encoded, err := ptSerde.Encode(&pt)
		if err != nil {
			return &common.FnOutput{Success: false, Message: err.Error()}
		}
		_, err = stream.Push(ctx, encoded, uint8(parNum), false, false)
		if err != nil {
			return &common.FnOutput{Success: false, Message: err.Error()}
		}
		elapsed := time.Since(procStart)
		latencies = append(latencies, int(elapsed.Microseconds()))
		nEmitEvent += 1
	}
	return &common.FnOutput{
		Success:   true,
		Duration:  time.Since(startTime).Seconds(),
		Latencies: map[string][]int{"e2e": latencies},
	}
}
