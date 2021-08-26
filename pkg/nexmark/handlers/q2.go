package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	ntypes "cs.utexas.edu/zhitingz/sharedlog-stream/pkg/nexmark/types"
	"cs.utexas.edu/zhitingz/sharedlog-stream/pkg/nexmark/utils"
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
	fmt.Printf("query 2 output: %v\n", encodedOutput)
	return utils.CompressData(encodedOutput), nil
}

func filterFunc(e interface{}) bool {
	event := e.(*ntypes.Event)
	return event.Bid.Auction%123 == 0
}

func Query2(ctx context.Context, env types.Environment, input *ntypes.QueryInput, output chan *ntypes.FnOutput) {
	inputStream, err := sharedlog_stream.NewSharedLogStream(ctx, env, input.InputTopicName)
	if err != nil {
		output <- &ntypes.FnOutput{
			Success: false,
			Message: fmt.Sprintf("NewSharedlogStream for input stream failed: %v", err),
		}
		return
	}
	/*
		outputStream, err := sharedlog_stream.NewSharedLogStream(ctx, env, input.OutputTopicName)
		if err != nil {
			output <- &ntypes.FnOutput{
				Success: false,
				Message: fmt.Sprintf("NewSharedlogStream for output stream failed: %v", err),
			}
			return
		}
	*/
	duration := time.Duration(input.Duration) * time.Second
	// filterOp := operator.NewFilter(filterFunc)
	latencies := make([]int, 0, 128)
	startTime := time.Now()
	for {
		if duration != 0 && time.Since(startTime) >= duration {
			break
		}
		// procStart := time.Now()
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
		event := &ntypes.Event{}
		_, err = event.UnmarshalMsg(val)
		if err != nil {
			output <- &ntypes.FnOutput{
				Success: false,
				Message: fmt.Sprintf("fail to unmarshal stream item to Event: %v", err),
			}
		}
		/*
			if event.Etype == ntypes.BID && filterOp.FilterF(event) {
				seqNum, err := outputStream.Push(val)
				if err != nil {
					panic(err)
				}
				// fmt.Printf("Push result to queue: %v\n", seqNum)
				elapsed := time.Since(procStart)
				latencies = append(latencies, int(elapsed.Microseconds()))
			}
		*/
	}
	output <- &ntypes.FnOutput{
		Success:   true,
		Duration:  time.Since(startTime).Seconds(),
		Latencies: latencies,
	}
}
