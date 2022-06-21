package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/benchmark/tests/pkg/tests/test_types"

	"cs.utexas.edu/zjia/faas/types"
)

type produceConsumeHandler struct {
	env types.Environment
}

func NewProducerConsumerTestHandler(env types.Environment) types.FuncHandler {
	return &produceConsumeHandler{
		env: env,
	}
}

func (h *produceConsumeHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &test_types.TestInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output := h.tests(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	return utils.CompressData(encodedOutput), nil
}

func (h *produceConsumeHandler) tests(ctx context.Context, sp *test_types.TestInput) *common.FnOutput {
	if sp.TestName == "multiProducer2pc" {
		h.testMultiProducer2pc(ctx)
	} else if sp.TestName == "singleProducerEpoch" {
		h.testSingleProduceConsumeEpoch(ctx)
	} else {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("test not recognized: %s\n", sp.TestName),
		}
	}
	return &common.FnOutput{
		Success: true,
		Message: fmt.Sprintf("%s done", sp.TestName),
	}
}
