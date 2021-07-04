package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	ntypes "cs.utexas.edu/zhitingz/sharedlog-stream/pkg/nexmark/types"
	"cs.utexas.edu/zhitingz/sharedlog-stream/pkg/nexmark/utils"
	"cs.utexas.edu/zhitingz/sharedlog-stream/pkg/operator"
	"cs.utexas.edu/zhitingz/sharedlog-stream/pkg/sharedlog_stream"
	"cs.utexas.edu/zjia/faas/types"
)

type query2Handler struct {
	env types.Environment
}

func NewQuery2(env types.Environment) types.FuncHandler {
	return &query2Handler{
		env: env,
	}
}

func (h *query2Handler) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &ntypes.QueryInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	outputCh := make(chan *ntypes.FnOutput)
	go Query2(ctx, h.env, parsedInput, outputCh)
	output := <-outputCh
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	return utils.CompressData(encodedOutput), nil
}

func filterFunc(e interface{}) bool {
	event := e.(ntypes.Event)
	return event.Bid.Auction%123 == 0
}

func Query2(ctx context.Context, env types.Environment, input *ntypes.QueryInput, output chan *ntypes.FnOutput) {
	inputStream, err := sharedlog_stream.NewSharedLogStream(ctx, env, input.InputTopicName)
	outputStream, err := sharedlog_stream.NewSharedLogStream(ctx, env, input.OutputTopicName)
	if err != nil {
		output <- &ntypes.FnOutput{
			Success: false,
			Message: fmt.Sprintf("NewSharedlogStream failed: %v", err),
		}
		return
	}
	filterOp := operator.NewFilter(filterFunc)
	for {
		val, err := inputStream.Pop()
		if err != nil {
			if sharedlog_stream.IsStreamEmptyError(err) {
				time.Sleep(time.Duration(100) * time.Microsecond)
				continue
			} else if sharedlog_stream.IsStreamTimeoutError(err) {
				continue
			} else {
				output <- &ntypes.FnOutput{
					Success: false,
					Message: fmt.Sprintf("stream pop failed: %v", err),
				}
			}
		}
		filterOp.In() <- val
		go func() {
			for elem := range filterOp.Out() {
				event := elem.(ntypes.Event)
				encoded, err := event.MarshalMsg(nil)
				if err != nil {
					panic(err)
				}
				err = outputStream.Push(encoded)
				if err != nil {
					panic(err)
				}
			}
		}()
	}
}
