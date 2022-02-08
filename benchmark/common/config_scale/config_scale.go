package configscale

import (
	"context"
	"encoding/json"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream/processor/commtypes"

	"cs.utexas.edu/zjia/faas/types"
)

type ConfigScaleHandler struct {
	env types.Environment
}

func NewConfigScaleHandler(env types.Environment) types.FuncHandler {
	return &ConfigScaleHandler{
		env: env,
	}
}

func (h *ConfigScaleHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &common.ConfigScaleInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output := h.ConfigScale(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	return utils.CompressData(encodedOutput), nil
}

func (h *ConfigScaleHandler) ConfigScale(ctx context.Context, input *common.ConfigScaleInput) *common.FnOutput {
	cmm, err := sharedlog_stream.NewControlChannelManager(h.env, input.AppId, commtypes.SerdeFormat(input.SerdeFormat))
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	err = cmm.AppendRescaleConfig(ctx, input.Config)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	return &common.FnOutput{
		Success: true,
	}
}
