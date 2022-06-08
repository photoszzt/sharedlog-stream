package handlers

import (
	"context"
	"encoding/json"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/commtypes"

	"cs.utexas.edu/zjia/faas/types"

	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"
)

type q5Dump struct {
	env types.Environment
}

func NewQ5Dump(env types.Environment) types.FuncHandler {
	return &q5Dump{
		env: env,
	}
}

func (h *q5Dump) Call(ctx context.Context, input []byte) ([]byte, error) {
	di := &common.DumpInput{}
	err := json.Unmarshal(input, di)
	if err != nil {
		return nil, err
	}
	output := h.process(ctx, di)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		return nil, err
	}
	return utils.CompressData(encodedOutput), nil
}

func (h *q5Dump) process(ctx context.Context, di *common.DumpInput) *common.FnOutput {
	serdeFormat := commtypes.SerdeFormat(di.SerdeFormat)
	msgSerde, err := commtypes.GetMsgSerde(serdeFormat)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	seSerde, err := ntypes.GetStartEndTimeSerde(serdeFormat)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	aicSerde, err := ntypes.GetAuctionIdCntMaxSerde(serdeFormat)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	err = benchutil.DumpOutputStream(ctx, h.env, benchutil.DumpOutputStreamConfig{
		OutputDir:     di.DumpDir,
		TopicName:     "nexmark_q5_sink",
		SerdeFormat:   serdeFormat,
		NumPartitions: 1,
		MsgSerde:      msgSerde,
		KeySerde:      seSerde,
		ValSerde:      aicSerde,
	})
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	return &common.FnOutput{
		Success: true,
	}
}
