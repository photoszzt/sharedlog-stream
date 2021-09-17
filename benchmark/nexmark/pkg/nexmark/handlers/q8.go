package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream"
	"sharedlog-stream/pkg/stream/processor"

	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"

	"cs.utexas.edu/zjia/faas/types"
)

type query8Handler struct {
	env types.Environment
}

func NewQuery8(env types.Environment) types.FuncHandler {
	return &query8Handler{
		env: env,
	}
}

func (h *query8Handler) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &ntypes.QueryInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	outputCh := make(chan *ntypes.FnOutput)
	go Query8(ctx, h.env, parsedInput, outputCh)
	output := <-outputCh
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	fmt.Printf("query 2 output: %v\n", encodedOutput)
	return utils.CompressData(encodedOutput), nil
}

func Query8(ctx context.Context, env types.Environment, input *ntypes.QueryInput, output chan *ntypes.FnOutput) {
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

	var msgEncoder processor.MsgEncoder
	var eventDecoder processor.Decoder
	var msgDecoder processor.MsgDecoder
	var ptEncoder processor.Encoder

	if input.SerdeFormat == uint8(common.JSON) {
		msgEncoder = common.MessageSerializedJSONEncoder{}
		eventDecoder = ntypes.EventJSONDecoder{}
		msgDecoder = common.MessageSerializedJSONDecoder{}
		ptEncoder = ntypes.PersonTimeJSONEncoder{}
	} else if input.SerdeFormat == uint8(common.MSGP) {
		msgEncoder = common.MessageSerializedMsgpEncoder{}
		eventDecoder = ntypes.EventMsgpDecoder{}
		msgDecoder = common.MessageSerializedMsgpDecoder{}
		ptEncoder = ntypes.PersonTimeMsgpEncoder{}
	} else {
		output <- &ntypes.FnOutput{
			Success: false,
			Message: fmt.Sprintf("serde format should be either json or msgp; but %v is given", input.SerdeFormat),
		}
	}
	builder := stream.NewStreamBuilder()
	inputs := builder.Source("nexmark-src", sharedlog_stream.NewSharedLogStreamSource(inputStream, int(input.Duration),
		processor.StringDecoder{}, eventDecoder, msgDecoder))
	person := inputs.Filter("filter-person",
		processor.PredicateFunc(func(msg processor.Message) (bool, error) {
			event := msg.Value.(*ntypes.Event)
			return event.Etype == ntypes.PERSON, nil
		})).
		Map("select-key",
			processor.MapperFunc(func(msg processor.Message) (processor.Message, error) {
				event := msg.Value.(*ntypes.Event)
				return processor.Message{Key: event.NewPerson.ID, Value: msg.Value, Timestamp: msg.Timestamp}, nil
			}))
	auction := inputs.Filter("filter-auction",
		processor.PredicateFunc(func(msg processor.Message) (bool, error) {
			event := msg.Value.(*ntypes.Event)
			return event.Etype == ntypes.AUCTION, nil
		})).
		Map("select-key",
			processor.MapperFunc(func(msg processor.Message) (processor.Message, error) {
				event := msg.Value.(*ntypes.Event)
				return processor.Message{Key: event.NewAuction.Seller, Value: msg.Value, Timestamp: msg.Timestamp}, nil
			}))
	auction.StreamStreamJoin(person,
		processor.ValueJoinerWithKeyFunc(func(readOnlyKey interface{}, leftValue interface{}, rightValue interface{}) interface{} {
			key := readOnlyKey.(stream.WindowedKey)
			rv := rightValue.(*ntypes.Event)
			return &ntypes.PersonTime{
				ID:        rv.NewPerson.ID,
				Name:      rv.NewPerson.Name,
				StartTime: key.Window.Start(),
			}

		}), *processor.NewJoinWindows(time.Duration(10) * time.Second)).
		Process("sink", sharedlog_stream.NewSharedLogStreamSink(outputStream, processor.Uint64Encoder{}, ptEncoder, msgEncoder))
	tp, err_arrs := builder.Build()
	if err_arrs != nil {
		output <- &ntypes.FnOutput{
			Success: false,
			Message: fmt.Sprintf("build stream failed: %v", err_arrs),
		}
	}
	pumps := make(map[processor.Node]processor.Pump)
	var srcPumps []processor.SourcePump
	nodes := processor.FlattenNodeTree(tp.Sources())
	processor.ReverseNodes(nodes)
	for _, node := range nodes {
		pipe := processor.NewPipe(processor.ResolvePumps(pumps, node.Children()))
		node.Processor().WithPipe(pipe)

		pump := processor.NewSyncPump(node, pipe)
		pumps[node] = pump
	}
	for source, node := range tp.Sources() {
		srcPump := processor.NewSourcePump(node.Name(), source,
			processor.ResolvePumps(pumps, node.Children()), func(err error) {
				log.Fatal(err.Error())
			})
		srcPumps = append(srcPumps, srcPump)
	}

	duration := time.Duration(input.Duration) * time.Second
	latencies := make([]int, 0, 128)
	startTime := time.Now()
	select {
	case <-time.After(duration):
		for _, srcPump := range srcPumps {
			srcPump.Stop()
			srcPump.Close()
		}
	}
	output <- &ntypes.FnOutput{
		Success:   true,
		Duration:  time.Since(startTime).Seconds(),
		Latencies: latencies,
	}

}
