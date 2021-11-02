package wordcount

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"time"

	"cs.utexas.edu/zjia/faas/types"
)

type wordCountSource struct {
	env   types.Environment
	lines []string
}

func NewWordCountSource(env types.Environment) types.FuncHandler {
	return &wordCountSource{
		env:   env,
		lines: make([]string, 0),
	}
}

func (h *wordCountSource) Call(ctx context.Context, input []byte) ([]byte, error) {
	sp := &common.SourceParam{}
	err := json.Unmarshal(input, sp)
	if err != nil {
		return nil, err
	}
	err = ParseFile(sp.FileName, &h.lines)
	if err != nil {
		return nil, err
	}
	output := h.eventGeneration(ctx, h.env, sp)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		return nil, err
	}
	return utils.CompressData(encodedOutput), nil
}

func encode_sentence_event(valSerde commtypes.Serde, msgSerde commtypes.MsgSerde, val string) ([]byte, error) {
	val_encoded, err := valSerde.Encode(val)
	if err != nil {
		return nil, fmt.Errorf("event serialization failed: %v", err)
	}
	msgEncoded, err := msgSerde.Encode(nil, val_encoded)
	if err != nil {
		return nil, fmt.Errorf("msg serialization failed: %v", err)
	}
	return msgEncoded, nil
}

func (h *wordCountSource) eventGeneration(ctx context.Context, env types.Environment, sp *common.SourceParam) *common.FnOutput {
	stream, err := sharedlog_stream.NewSharedLogStream(ctx, env, sp.TopicName)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("NewSharedlogStream failed: %v", err),
		}
	}

	msgSerde, err := commtypes.GetMsgSerde(sp.SerdeFormat)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}

	latencies := make([]int, 0, 128)
	numEvents := sp.NumEvents
	duration := time.Duration(sp.Duration) * time.Second
	startTime := time.Now()
	idx := 0

	for {
		if duration != 0 && time.Since(startTime) >= duration || duration == 0 {
			break
		}
		if numEvents != 0 && idx == int(numEvents) {
			break
		}
		procStart := time.Now()
		sentence := h.lines[idx]
		msgEncoded, err := encode_sentence_event(commtypes.StringSerde{}, msgSerde, sentence)
		if err != nil {
			return &common.FnOutput{
				Success: false,
				Message: err.Error(),
			}
		}
		_, err = stream.Push(msgEncoded, 0, nil)
		if err != nil {
			return &common.FnOutput{
				Success: false,
				Message: fmt.Sprintf("stream push failed: %v", err),
			}
		}
		elapsed := time.Since(procStart)
		latencies = append(latencies, int(elapsed.Microseconds()))
		idx += 1
		if idx >= len(h.lines) {
			idx = 0
		}
	}
	return &common.FnOutput{
		Success:   true,
		Duration:  time.Since(startTime).Seconds(),
		Latencies: map[string][]int{"e2e": latencies},
	}
}

func ParseFile(fileName string, lines *[]string) error {
	dataFile, err := os.Open(fileName)
	if err != nil {
		return fmt.Errorf("fail to open file %v: %v", fileName, err)
	}
	defer dataFile.Close()

	sc := bufio.NewScanner(dataFile)
	for sc.Scan() {
		text := sc.Text()
		*lines = append(*lines, text)
	}
	if err := sc.Err(); err != nil {
		return err
	}
	return nil
}
