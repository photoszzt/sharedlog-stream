package chkpt_manager

import (
	"context"
	"encoding/json"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/exactly_once_intr"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/snapshot_store"
	"sharedlog-stream/pkg/txn_data"
	"time"

	"cs.utexas.edu/zjia/faas/types"
	"github.com/go-redis/redis/v9"
)

type ChkptManagerHandler struct {
	env              types.Environment
	rdb_arr          []*redis.Client
	epochMarkerSerde commtypes.SerdeG[commtypes.EpochMarker]
	srcStream        *sharedlog_stream.ShardedSharedLogStream
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

func (h *ChkptManagerHandler) genChkpt(ctx context.Context, input *common.ChkptMngrInput) error {
	tpNameHash := h.srcStream.TopicNameHash()
	for i := uint8(0); i < input.SrcNumPart; i++ {
		chkpt_tag := txn_data.ChkptTag(tpNameHash, i)
		nameHashTag := sharedlog_stream.NameHashWithPartition(tpNameHash, i)
		marker := commtypes.EpochMarker{
			Mark:      commtypes.CHKPT_MARK,
			ProdIndex: i,
		}
		encoded, err := h.epochMarkerSerde.Encode(marker)
		if err != nil {
			return err
		}
		_, err = h.srcStream.PushWithTag(ctx, encoded, i, []uint64{nameHashTag, chkpt_tag}, nil,
			sharedlog_stream.ControlRecordMeta, commtypes.EmptyProducerId)
		if err != nil {
			return err
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
	h.srcStream, err = sharedlog_stream.NewShardedSharedLogStream(h.env,
		input.SrcTopicName,
		input.SrcNumPart,
		serdeFormat, input.BufMaxSize)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	if guarantee == exactly_once_intr.ALIGN_CHKPT {
		t = time.NewTicker(time.Duration(input.ChkptEveryMs) * time.Millisecond)
		for {
			select {
			case <-t.C:
				err := h.genChkpt(ctx, input)
				if err != nil {
					return common.GenErrFnOutput(err)
				}
			default:
			}
		}
	}
	return nil
}
