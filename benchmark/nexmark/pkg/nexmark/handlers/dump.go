package handlers

import (
	"context"
	"encoding/json"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/commtypes"

	"cs.utexas.edu/zjia/faas/types"
)

type dump struct {
	env types.Environment
}

func NewDump(env types.Environment) types.FuncHandler {
	return &dump{
		env: env,
	}
}

func (h *dump) Call(ctx context.Context, input []byte) ([]byte, error) {
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

func (h *dump) process(ctx context.Context, di *common.DumpInput) *common.FnOutput {
	serdeFormat := commtypes.SerdeFormat(di.SerdeFormat)
	msgSerde, err := commtypes.GetMsgSerde(serdeFormat)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	keySerde, err := GetSerdeFromString(di.KeySerde, serdeFormat)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	valSerde, err := GetSerdeFromString(di.ValueSerde, serdeFormat)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	err = benchutil.DumpOutputStream(ctx, h.env, benchutil.DumpOutputStreamConfig{
		OutputDir:     di.DumpDir,
		TopicName:     di.TopicName,
		SerdeFormat:   serdeFormat,
		NumPartitions: 1,
		KVMsgSerdes: commtypes.KVMsgSerdes{
			MsgSerde: msgSerde,
			KeySerde: keySerde,
			ValSerde: valSerde,
		},
	})
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	return &common.FnOutput{
		Success: true,
	}
}
