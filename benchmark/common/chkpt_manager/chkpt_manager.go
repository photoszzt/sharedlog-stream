package chkpt_manager

import (
	"context"
	"encoding/json"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/exactly_once_intr"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/snapshot_store"
	"time"

	"cs.utexas.edu/zjia/faas/types"
	"github.com/go-redis/redis/v9"
)

type ChkptManagerHandler struct {
	env              types.Environment
	rdb_arr          []*redis.Client
	epochMarkerSerde commtypes.SerdeG[commtypes.EpochMarker]
}

func NewChkptManager(env types.Environment) types.FuncHandler {
	return &ChkptManagerHandler{
		env:     env,
		rdb_arr: snapshot_store.GetRedisClients(),
	}
}

func (h *ChkptManagerHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &common.ChkptMngrInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output := h.Chkpt(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	return common.CompressData(encodedOutput), nil
}

func (h *ChkptManagerHandler) genChkpt(input *common.ChkptMngrInput) *common.FnOutput {
	for i := uint8(0); i < input.SrcNumPart; i++ {
		marker := commtypes.EpochMarker{
			Mark:      commtypes.CHKPT_MARK,
			ProdIndex: i,
		}
		encoded, err := h.epochMarkerSerde.Encode(marker)
		if err != nil {
			return common.GenErrFnOutput(err)
		}
		h.streamPusher.MsgChan <- sharedlog_stream.PayloadToPush{
			Payload:    encoded,
			IsControl:  true,
			Partitions: []uint8{parNum},
			Mark:       commtypes.CHKPT_MARK,
		}
	}
	return nil
}

func (h *ChkptManagerHandler) Chkpt(ctx context.Context, input *common.ChkptMngrInput) *common.FnOutput {
	var t *time.Ticker
	var err error
	guarantee := exactly_once_intr.GuaranteeMth(input.GuaranteeMth)
	serdeFormat := commtypes.SerdeFormat(input.SerdeFormat)
	h.epochMarkerSerde, err = commtypes.GetEpochMarkerSerdeG(serdeFormat)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	if guarantee == exactly_once_intr.ALIGN_CHKPT {
		t = time.NewTicker(time.Duration(input.ChkptEveryMs) * time.Millisecond)
		for {
			select {
			case <-t.C:
				fn_out := h.genChkpt(input)
				if fn_out != nil {
					return fn_out
				}
			default:
			}
		}
	}
	return nil
}
