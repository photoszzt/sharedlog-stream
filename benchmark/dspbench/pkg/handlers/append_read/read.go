package append_read

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/sharedlog_stream"

	"cs.utexas.edu/zjia/faas/types"
)

type ReadHandler struct {
	env types.Environment
}

func NewReadHandler(env types.Environment) *ReadHandler {
	return &ReadHandler{
		env: env,
	}
}

func (h *ReadHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	output := h.process(ctx)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		return nil, err
	}
	return utils.CompressData(encodedOutput), nil
}

func (h *ReadHandler) process(ctx context.Context) *common.FnOutput {
	s1, err := sharedlog_stream.NewSharedLogStream(h.env, "t1", commtypes.JSON)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}

	/*
		tag := sharedlog_stream.NameHashWithPartition(s1.TopicNameHash(), 0)
		// read backward
		idx := 0
		seqNum := protocol.MaxLogSeqnum
		for {
			entry, err := h.env.SharedLogReadPrev(ctx, tag, seqNum)
			if err != nil {
				return &common.FnOutput{
					Success: false,
					Message: err.Error(),
				}
			}
			if entry == nil {
				continue
			} else {
				fmt.Fprintf(os.Stderr, "reverse read entry is %v, seqNum 0x%x\n", string(entry.Data), entry.SeqNum)
				idx += 1
				seqNum = entry.SeqNum
				if idx == 10 {
					break
				}
			}
		}

		// read forward with log api
		idx = 0
		seqNum = 0
		for {
			entry, err := h.env.SharedLogReadNext(ctx, tag, seqNum)
			if err != nil {
				return &common.FnOutput{
					Success: false,
					Message: err.Error(),
				}
			}
			if entry != nil {
				fmt.Fprintf(os.Stderr, "forward read entry is %v, seqNum 0x%x\n", string(entry.Data), entry.SeqNum)
				idx += 1
				seqNum = entry.SeqNum
				if idx == 10 {
					break
				}
			} else {
				continue
			}
		}
	*/
	// read forward
	for i := 0; i < 10; i++ {
		rawMsg, err := s1.ReadNext(ctx, 0)
		if err != nil {
			return &common.FnOutput{
				Success: false,
				Message: err.Error(),
			}
		}
		expected_str := fmt.Sprintf("test %d\n", i)
		fmt.Fprintf(os.Stderr, "read msg is %s", string(rawMsg.Payload))
		if !bytes.Equal(rawMsg.Payload, []byte(expected_str)) {
			return &common.FnOutput{
				Success: false,
				Message: fmt.Sprintf("expected str is \"%s\", got \"%s\"", expected_str, rawMsg.Payload),
			}
		}
	}

	return &common.FnOutput{
		Success: true,
	}
}
