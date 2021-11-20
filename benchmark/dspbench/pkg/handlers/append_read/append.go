package append_read

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/sharedlog_stream"

	"cs.utexas.edu/zjia/faas/types"
)

type AppendHandler struct {
	env types.Environment
}

func NewAppendHandler(env types.Environment) *AppendHandler {
	return &AppendHandler{
		env: env,
	}
}

func (h *AppendHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	output := h.process(ctx)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		return nil, err
	}
	return utils.CompressData(encodedOutput), nil
}

func (h *AppendHandler) process(ctx context.Context) *common.FnOutput {
	s1 := sharedlog_stream.NewSharedLogStream(h.env, "t1")
	/*
		err := s1.InitStream(ctx, 0, false)
		if err != nil {
			return &common.FnOutput{
				Success: false,
				Message: err.Error(),
			}
		}
	*/

	var pushSeqNum []uint64
	for i := 0; i < 10; i++ {
		seqNum, err := s1.Push(ctx, []byte(fmt.Sprintf("test %d\n", i)), 0, false)
		if err != nil {
			return &common.FnOutput{
				Success: false,
				Message: err.Error(),
			}
		}
		pushSeqNum = append(pushSeqNum, seqNum)
	}

	for i := 0; i < 10; i++ {
		_, rawMsgs, err := s1.ReadNext(ctx, 0)
		if err != nil {
			return &common.FnOutput{
				Success: false,
				Message: err.Error(),
			}
		}
		for _, rawMsg := range rawMsgs {
			if rawMsg.LogSeqNum != pushSeqNum[i] {
				return &common.FnOutput{
					Success: false,
					Message: fmt.Sprintf("seq num returned 0x%x, expected to be 0x%x\n", rawMsg.LogSeqNum, pushSeqNum[i]),
				}
			}

			expected_str := fmt.Sprintf("test %d\n", i)
			if !bytes.Equal(rawMsg.Payload, []byte(expected_str)) {
				return &common.FnOutput{
					Success: false,
					Message: fmt.Sprintf("expected str is \"%s\", got \"%s\"", expected_str, rawMsg.Payload),
				}
			}
		}
	}
	return &common.FnOutput{
		Success: true,
	}
}
