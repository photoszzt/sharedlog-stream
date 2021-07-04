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

type query1Handler struct {
	env types.Environment
}

func NewQuery1(env types.Environment) types.FuncHandler {
	return &query1Handler{
		env: env,
	}
}

func (h *query1Handler) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &ntypes.QueryInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	outputCh := make(chan *ntypes.FnOutput)
	go Query1(ctx, h.env, parsedInput, outputCh)
	output := <-outputCh
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	return utils.CompressData(encodedOutput), nil
}

func mapFunc(e interface{}) interface{} {
	event := e.(ntypes.Event)
	event.Bid.Price = uint64(event.Bid.Price * 908 / 1000.0)
	return event
}

func Query1(ctx context.Context, env types.Environment, input *ntypes.QueryInput, output chan *ntypes.FnOutput) {
	inputStream, err := sharedlog_stream.NewSharedLogStream(ctx, env, input.InputTopicName)
	outputStream, err := sharedlog_stream.NewSharedLogStream(ctx, env, input.OutputTopicName)
	if err != nil {
		output <- &ntypes.FnOutput{
			Success: false,
			Message: fmt.Sprintf("NewSharedlogStream failed: %v", err),
		}
		return
	}
	mapOp := operator.NewMap(mapFunc)
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
		mapOp.In() <- val
		go func() {
			for elem := range mapOp.Out() {
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
