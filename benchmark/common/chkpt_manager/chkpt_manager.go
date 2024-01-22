package chkpt_manager

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/pkg/checkpt"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/exactly_once_intr"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/txn_data"
	"time"

	"cs.utexas.edu/zjia/faas/types"
)

type ChkptManagerHandler struct {
	env              types.Environment
	rcm              checkpt.RedisChkptManager
	epochMarkerSerde commtypes.SerdeG[commtypes.EpochMarker]
	srcStream        *sharedlog_stream.ShardedSharedLogStream
}

func NewChkptManager(env types.Environment) types.FuncHandler {
	return &ChkptManagerHandler{
		env: env,
	}
}

func (h *ChkptManagerHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &common.ChkptMngrInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	h.rcm, err = checkpt.NewRedisChkptManager(ctx)
	if err != nil {
		return nil, err
	}
	PrintChkptMngrInput(parsedInput)
	output := h.Chkpt(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	return common.CompressData(encodedOutput), nil
}

func PrintChkptMngrInput(c *common.ChkptMngrInput) {
	fmt.Fprintf(os.Stderr, "ChkptMngrInput:\n")
	fmt.Fprintf(os.Stderr, "\tSrcTopicName         : %v\n", c.SrcTopicName)
	fmt.Fprintf(os.Stderr, "\tFinalOutputTopicNames: %v\n", c.FinalOutputTopicNames)
	fmt.Fprintf(os.Stderr, "\tFinalNumOutPartitions: %v\n", c.FinalNumOutPartitions)
	fmt.Fprintf(os.Stderr, "\tBufMaxSize           : %v\n", c.BufMaxSize)
	fmt.Fprintf(os.Stderr, "\tSrcNumPart           : %v\n", c.SrcNumPart)
	fmt.Fprintf(os.Stderr, "\tGuarantee            : %v\n", exactly_once_intr.GuaranteeMth(c.GuaranteeMth).String())
	fmt.Fprintf(os.Stderr, "\tSerdeFormat          : %v\n", commtypes.SerdeFormat(c.SerdeFormat).String())
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
	var err error
	guarantee := exactly_once_intr.GuaranteeMth(input.GuaranteeMth)

	fmt.Fprintf(os.Stderr, "")
	if guarantee == exactly_once_intr.ALIGN_CHKPT {
		serdeFormat := commtypes.SerdeFormat(input.SerdeFormat)
		chkptEveryMs := time.Duration(input.ChkptEveryMs) * time.Millisecond
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
		err = h.rcm.WaitForChkptFinish(ctx, input.FinalOutputTopicNames, input.FinalNumOutPartitions)
		if err != nil {
			return common.GenErrFnOutput(err)
		}
		now := time.Now()
		for {
			req, err := h.rcm.GetReqChkMngrEnd(ctx)
			if err != nil {
				return common.GenErrFnOutput(err)
			}
			if req == 1 {
				err = h.rcm.SetChkMngrEnded(ctx)
				if err != nil {
					return common.GenErrFnOutput(err)
				}
				break
			}
			elapsed := time.Since(now)
			if elapsed < chkptEveryMs {
				time.Sleep(chkptEveryMs - elapsed)
			}
			err = h.rcm.ResetCheckPointCount(ctx, input.FinalOutputTopicNames)
			if err != nil {
				return common.GenErrFnOutput(err)
			}
			err = h.genChkpt(ctx, input)
			if err != nil {
				return common.GenErrFnOutput(err)
			}
			err = h.rcm.WaitForChkptFinish(ctx, input.FinalOutputTopicNames, input.FinalNumOutPartitions)
			if err != nil {
				return common.GenErrFnOutput(err)
			}
			now = time.Now()
		}
	}
	return nil
}
