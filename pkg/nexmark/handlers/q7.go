package handlers

import (
	"context"
	"encoding/json"
	"fmt"

	ntypes "cs.utexas.edu/zhitingz/sharedlog-stream/pkg/nexmark/types"
	"cs.utexas.edu/zhitingz/sharedlog-stream/pkg/nexmark/utils"
	"cs.utexas.edu/zhitingz/sharedlog-stream/pkg/sharedlog_stream"
	"cs.utexas.edu/zhitingz/sharedlog-stream/pkg/stream"
	"cs.utexas.edu/zhitingz/sharedlog-stream/pkg/stream/processor"
	"cs.utexas.edu/zjia/faas/types"
)

type query7Handler struct {
	env types.Environment
}

func NewQuery7(env types.Environment) types.FuncHandler {
	return &query7Handler{
		env: env,
	}
}

func (h *query7Handler) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &ntypes.QueryInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	outputCh := make(chan *ntypes.FnOutput)
	go Query7(ctx, h.env, parsedInput, outputCh)
	output := <-outputCh
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	fmt.Printf("query 2 output: %v\n", encodedOutput)
	return utils.CompressData(encodedOutput), nil
}

func Query7(ctx context.Context, env types.Environment, input *ntypes.QueryInput, output chan *ntypes.FnOutput) {
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

	// var msgEncoder processor.MsgEncoder
	var eventDecoder processor.Decoder
	var msgDecoder processor.MsgDecoder

	if input.SerdeFormat == uint8(ntypes.JSON) {
		// msgEncoder = ntypes.MessageSerializedJSONEncoder{}
		eventDecoder = ntypes.EventJSONDecoder{}
		msgDecoder = ntypes.MessageSerializedJSONDecoder{}
	} else if input.SerdeFormat == uint8(ntypes.MSGP) {
		// msgEncoder = ntypes.MessageSerializedMsgpEncoder{}
		eventDecoder = ntypes.EventMsgpDecoder{}
		msgDecoder = ntypes.MessageSerializedMsgpDecoder{}
	} else {
		output <- &ntypes.FnOutput{
			Success: false,
			Message: fmt.Sprintf("serde format should be either json or msgp; but %v is given", input.SerdeFormat),
		}
	}

	builder := stream.NewStreamBuilder()
	inputs := builder.Source("nexmark-src", sharedlog_stream.NewSharedLogStreamSource(inputStream, int(input.Duration),
		processor.StringDecoder{}, eventDecoder, msgDecoder))
	// bid :=
	inputs.Filter("filter-bid", processor.PredicateFunc(func(msg processor.Message) (bool, error) {
		event := msg.Value.(*ntypes.Event)
		return event.Etype == ntypes.BID, nil
	})).Map("select-key", processor.MapperFunc(func(msg processor.Message) (processor.Message, error) {
		event := msg.Value.(*ntypes.Event)
		return processor.Message{Key: event.Bid.Auction, Value: msg.Value, Timestamp: msg.Timestamp}, nil
	}))
}
