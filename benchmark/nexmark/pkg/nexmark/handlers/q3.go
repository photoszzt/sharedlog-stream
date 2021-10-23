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
	"sharedlog-stream/pkg/stream/processor/commtypes"

	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"

	"cs.utexas.edu/zjia/faas/types"
)

type query3Handler struct {
	env types.Environment
}

func NewQuery3(env types.Environment) types.FuncHandler {
	return &query3Handler{
		env: env,
	}
}

func (h *query3Handler) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &ntypes.QueryInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	outputCh := make(chan *common.FnOutput)
	go Query3(ctx, h.env, parsedInput, outputCh)
	output := <-outputCh
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	fmt.Printf("query 3 output: %v\n", encodedOutput)
	return utils.CompressData(encodedOutput), nil
}

func Query3(ctx context.Context, env types.Environment, input *ntypes.QueryInput, output chan *common.FnOutput) {
	inputStream, err := sharedlog_stream.NewSharedLogStream(ctx, env, input.InputTopicName)
	if err != nil {
		output <- &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("NewSharedlogStream for input stream failed: %v", err),
		}
		return
	}

	outputStream, err := sharedlog_stream.NewSharedLogStream(ctx, env, input.OutputTopicName)
	if err != nil {
		output <- &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("NewSharedlogStream for output stream failed: %v", err),
		}
		return
	}

	msgSerde, err := commtypes.GetMsgSerde(input.SerdeFormat)
	if err != nil {
		output <- &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	eventSerde, err := getEventSerde(input.SerdeFormat)
	if err != nil {
		output <- &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}

	inConfig := &sharedlog_stream.SharedLogStreamConfig{
		Timeout:      time.Duration(input.Duration) * time.Second,
		KeyDecoder:   commtypes.StringDecoder{},
		ValueDecoder: eventSerde,
		MsgDecoder:   msgSerde,
	}
	outConfig := &sharedlog_stream.StreamSinkConfig{
		KeyEncoder: commtypes.Uint64Encoder{},
		ValueEncoder: commtypes.EncoderFunc(func(val interface{}) ([]byte, error) {
			ret := val.(*ntypes.NameCityStateId)
			if input.SerdeFormat == uint8(commtypes.JSON) {
				return json.Marshal(ret)
			} else {
				return ret.MarshalMsg(nil)
			}
		}),
		MsgEncoder: msgSerde,
	}
	builder := stream.NewStreamBuilder()
	inputs := builder.Source("nexmark-src", sharedlog_stream.NewSharedLogStreamSource(inputStream, inConfig))
	auctionsBySellerId := inputs.Filter("filter-auction",
		processor.PredicateFunc(func(msg *commtypes.Message) (bool, error) {
			event := msg.Value.(ntypes.Event)
			return event.Etype == ntypes.AUCTION, nil
		})).
		Filter("filter-category",
			processor.PredicateFunc(func(msg *commtypes.Message) (bool, error) {
				event := msg.Value.(*ntypes.Event)
				return event.NewAuction.Category == 10, nil
			})).
		Map("update-key",
			processor.MapperFunc(func(msg commtypes.Message) (commtypes.Message, error) {
				event := msg.Value.(*ntypes.Event)
				return commtypes.Message{Key: event.NewAuction.Seller, Value: msg.Value, Timestamp: msg.Timestamp}, nil
			})).
		ToTable("convert-to-table")

	personsById := inputs.Filter("filter-person",
		processor.PredicateFunc(func(msg *commtypes.Message) (bool, error) {
			event := msg.Value.(ntypes.Event)
			return event.Etype == ntypes.PERSON, nil
		})).
		Filter("filter-state", processor.PredicateFunc(func(msg *commtypes.Message) (bool, error) {
			event := msg.Value.(ntypes.Event)
			state := event.NewPerson.State
			return state == "OR" || state == "ID" || state == "CA", nil
		})).
		Map("update-key", processor.MapperFunc(func(msg commtypes.Message) (commtypes.Message, error) {
			event := msg.Value.(*ntypes.Event)
			return commtypes.Message{Key: event.NewPerson.ID, Value: msg.Value, Timestamp: msg.Timestamp}, nil
		})).
		ToTable("convert-to-table")

	auctionsBySellerId.
		Join("join", personsById, processor.ValueJoinerFunc(func(leftVal interface{}, rightVal interface{}) interface{} {
			event := rightVal.(*ntypes.Event)
			return &ntypes.NameCityStateId{
				Name:  event.NewPerson.Name,
				City:  event.NewPerson.City,
				State: event.NewPerson.State,
				ID:    event.NewPerson.ID,
			}
		})).
		ToStream().
		Process("sink", sharedlog_stream.NewSharedLogStreamSink(outputStream, outConfig))
	tp, err_arrs := builder.Build()
	if err_arrs != nil {
		output <- &common.FnOutput{
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
	output <- &common.FnOutput{
		Success:   true,
		Duration:  time.Since(startTime).Seconds(),
		Latencies: map[string][]int{"e2e": latencies},
	}
}
