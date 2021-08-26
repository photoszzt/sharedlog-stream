package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	ntypes "cs.utexas.edu/zhitingz/sharedlog-stream/pkg/nexmark/types"
	"cs.utexas.edu/zhitingz/sharedlog-stream/pkg/nexmark/utils"
	"cs.utexas.edu/zhitingz/sharedlog-stream/pkg/sharedlog_stream"
	"cs.utexas.edu/zhitingz/sharedlog-stream/pkg/stream"
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
	fmt.Printf("query 1 outputs %s\n", encodedOutput)
	return utils.CompressData(encodedOutput), nil
}

func mapFunc(msg stream.Message) (stream.Message, error) {
	event := msg.Value.(*ntypes.Event)
	if event.Etype == ntypes.BID {
		event.Bid.Price = uint64(event.Bid.Price * 908 / 1000.0)
		return stream.Message{Value: event}, nil
	} else {
		return stream.EmptyMessage, nil
	}
}

func Query1(ctx context.Context, env types.Environment, input *ntypes.QueryInput, output chan *ntypes.FnOutput) {
	inputStream, err := sharedlog_stream.NewSharedLogStream(ctx, env, input.InputTopicName)
	if err != nil {
		output <- &ntypes.FnOutput{
			Success: false,
			Message: fmt.Sprintf("NewSharedlogStream for input stream failed: %v", err),
		}
		return
	}
	outputStream, err := sharedlog_stream.NewSharedLogStream(ctx, env, input.OutputTopicName)
	if err != nil {
		output <- &ntypes.FnOutput{
			Success: false,
			Message: fmt.Sprintf("NewSharedlogStream for output stream failed: %v", err),
		}
		return
	}
	builder := stream.NewStreamBuilder()
	builder.Source("nexmark-src", sharedlog_stream.NewSharedLogStreamSource(inputStream, int(input.Duration))).
		MapFunc("q1_map", stream.MapperFunc(mapFunc)).
		Process("sink", sharedlog_stream.NewSharedLogStreamSink(outputStream))
	_, err_arrs := builder.Build()
	if err_arrs != nil {
		output <- &ntypes.FnOutput{
			Success: false,
			Message: fmt.Sprintf("build stream failed: %v", err_arrs),
		}
	}
	// nodes := stream.FlattenNodeTree
	duration := time.Duration(input.Duration) * time.Second
	// mapOp := operator.NewMap(mapFunc)
	latencies := make([]int, 0, 128)
	startTime := time.Now()
	for {
		if duration != 0 && time.Since(startTime) >= duration {
			break
		}
		procStart := time.Now()
		val, err := inputStream.Pop()
		if err != nil {
			if sharedlog_stream.IsStreamEmptyError(err) {
				time.Sleep(time.Duration(100) * time.Microsecond)
				// fmt.Println("No stream")
				continue
			} else if sharedlog_stream.IsStreamTimeoutError(err) {
				// fmt.Println("pop timeout")
				continue
			} else {
				// fmt.Println("pop failed")
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
			trans := mapOp.MapF(event)
			if trans != nil {
				trans_event := trans.(*ntypes.Event)
				encoded, err := trans_event.MarshalMsg()
				if err != nil {
					panic(err)
				}
				_, err = outputStream.Push(encoded)
				if err != nil {
					panic(err)
				}
			}
		*/
		elapsed := time.Since(procStart)
		latencies = append(latencies, int(elapsed.Microseconds()))
	}
	output <- &ntypes.FnOutput{
		Success:   true,
		Duration:  time.Since(startTime).Seconds(),
		Latencies: latencies,
	}
}
