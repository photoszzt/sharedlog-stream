package wordcount

import (
	"context"
	"encoding/json"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/commtypes"

	"cs.utexas.edu/zjia/faas/types"
	"github.com/rs/zerolog/log"
)

type wordCountDump struct {
	env types.Environment
}

func NewWordCountDump(env types.Environment) types.FuncHandler {
	return &wordCountDump{
		env: env,
	}
}

func (h *wordCountDump) process(ctx context.Context, di *common.DumpInput) *common.FnOutput {
	var msgSerde commtypes.MsgSerde
	var vtSerde commtypes.Serde
	if di.SerdeFormat == uint8(commtypes.JSON) {
		msgSerde = commtypes.MessageSerializedJSONSerde{}
		vtSerde = commtypes.ValueTimestampJSONSerde{
			ValJSONSerde: commtypes.Uint64Serde{},
		}
	} else if di.SerdeFormat == uint8(commtypes.MSGP) {
		msgSerde = commtypes.MessageSerializedMsgpSerde{}
		vtSerde = commtypes.ValueTimestampJSONSerde{
			ValJSONSerde: commtypes.Uint64Serde{},
		}
	} else {
		log.Error().Msgf("serde format is not recognized; default back to JSON")
	}

	err := benchutil.DumpOutputStream(ctx, h.env, benchutil.DumpOutputStreamConfig{
		OutputDir:     di.DumpDir,
		TopicName:     di.TopicName,
		NumPartitions: di.NumPartitions,
		MsgSerde:      msgSerde,
		KeySerde:      commtypes.StringSerde{},
		ValSerde:      vtSerde,
	})
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

func (h *wordCountDump) Call(ctx context.Context, input []byte) ([]byte, error) {
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
