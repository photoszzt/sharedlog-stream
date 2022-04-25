package handlers

import (
	"context"
	"encoding/json"
	"os"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"time"

	"cs.utexas.edu/zjia/faas/types"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type kafkaProduceBenchHandler struct {
	env types.Environment
}

func NewKafkaProduceBenchHandler(env types.Environment) types.FuncHandler {
	return &kafkaProduceBenchHandler{
		env: env,
	}
}

func (h *kafkaProduceBenchHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &common.SourceParam{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output := h.kafkaProduceBench(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	return utils.CompressData(encodedOutput), nil
}

func (h *kafkaProduceBenchHandler) kafkaProduceBench(ctx context.Context, sp *common.SourceParam) *common.FnOutput {
	content, err := os.ReadFile(sp.FileName)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	latencies := make([]int, 0, 128)
	numEvents := sp.NumEvents
	duration := time.Duration(sp.Duration) * time.Second
	startTime := time.Now()
	nEmitEvent := uint32(0)
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":                     FLAGS_broker,
		"go.produce.channel.size":               100000,
		"go.events.channel.size":                100000,
		"acks":                                  "all",
		"max.in.flight.requests.per.connection": 5,
	})
}
