package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"sharedlog-stream/benchmark/common"
	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"

	"cs.utexas.edu/zjia/faas/types"
)

type query3ProcessorHandler struct {
	env types.Environment
}

func NewQuery3Processor(env types.Environment) types.FuncHandler {
	return &query3ProcessorHandler{
		env: env,
	}
}

func (h *query3ProcessorHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &common.QueryInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output := h.Query3(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	fmt.Printf("query 3 output: %v\n", encodedOutput)
	return utils.CompressData(encodedOutput), nil
}

func (h *query3ProcessorHandler) getSrcSink(ctx context.Context, sp *common.QueryInput,
	input_stream *sharedlog_stream.ShardedSharedLogStream,
	output_stream *sharedlog_stream.ShardedSharedLogStream,
) (*processor.MeteredSource, *processor.MeteredSink, error) {
	msgSerde, err := commtypes.GetMsgSerde(sp.SerdeFormat)
	if err != nil {
		return nil, nil, err
	}
	eventSerde, err := getEventSerde(sp.SerdeFormat)
	if err != nil {
		return nil, nil, err
	}

	inConfig := &sharedlog_stream.SharedLogStreamConfig{
		Timeout:      common.SrcConsumeTimeout,
		KeyDecoder:   commtypes.StringDecoder{},
		ValueDecoder: eventSerde,
		MsgDecoder:   msgSerde,
	}
	outConfig := &sharedlog_stream.StreamSinkConfig{
		KeyEncoder: commtypes.Uint64Encoder{},
		ValueEncoder: commtypes.EncoderFunc(func(val interface{}) ([]byte, error) {
			ret := val.(*ntypes.NameCityStateId)
			if sp.SerdeFormat == uint8(commtypes.JSON) {
				return json.Marshal(ret)
			} else {
				return ret.MarshalMsg(nil)
			}
		}),
		MsgEncoder: msgSerde,
	}
	src := processor.NewMeteredSource(sharedlog_stream.NewShardedSharedLogStreamSource(input_stream, inConfig))
	sink := processor.NewMeteredSink(sharedlog_stream.NewShardedSharedLogStreamSink(output_stream, outConfig))
	return src, sink, nil
}

type query3ProcessArgs struct {
	src           *processor.MeteredSource
	sink          *processor.MeteredSink
	output_stream *sharedlog_stream.ShardedSharedLogStream

	filterAuctions        *processor.MeteredProcessor
	auctionsBySellerIDMap *processor.MeteredProcessor
	filterPerson          *processor.MeteredProcessor
	personsByIDMap        *processor.MeteredProcessor
	parNum                uint8
}

func (h *query3ProcessorHandler) Query3(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	/*
		input_stream, output_stream, err := benchutil.GetShardedInputOutputStreams(ctx, h.env, sp)
		if err != nil {
			return &common.FnOutput{
				Success: false,
				Message: fmt.Sprintf("get input output stream failed: %v", err),
			}
		}
		src, sink, err := h.getSrcSink(ctx, sp, input_stream, output_stream)
		if err != nil {
			return &common.FnOutput{
				Success: false,
				Message: err.Error(),
			}
		}

		filterAuctions := processor.NewMeteredProcessor(processor.NewStreamFilterProcessor(processor.PredicateFunc(
			func(m *commtypes.Message) (bool, error) {
				event := m.Value.(ntypes.Event)
				return event.Etype == ntypes.AUCTION && event.NewAuction.Category == 10, nil
			})))

		auctionsBySellerIDMap := processor.NewMeteredProcessor(processor.NewStreamMapProcessor(
			processor.MapperFunc(func(msg commtypes.Message) (commtypes.Message, error) {
				event := msg.Value.(*ntypes.Event)
				return commtypes.Message{Key: event.NewAuction.Seller, Value: msg.Value, Timestamp: msg.Timestamp}, nil
			})))

		filterPerson := processor.NewMeteredProcessor(processor.NewStreamFilterProcessor(processor.PredicateFunc(
			func(msg *commtypes.Message) (bool, error) {
				event := msg.Value.(ntypes.Event)
				return event.Etype == ntypes.PERSON && event.NewPerson.State == "OR" &&
					event.NewPerson.State == "ID" && event.NewPerson.State == "CA", nil
			})))
		personsByIDMap := processor.NewMeteredProcessor(processor.NewStreamMapProcessor(
			processor.MapperFunc(func(msg commtypes.Message) (commtypes.Message, error) {
				event := msg.Value.(*ntypes.Event)
				return commtypes.Message{Key: event.NewPerson.ID, Value: msg.Value, Timestamp: msg.Timestamp}, nil
			})))

		joiner := processor.ValueJoinerFunc(func(leftVal interface{}, rightVal interface{}) interface{} {
			event := rightVal.(*ntypes.Event)
			return &ntypes.NameCityStateId{
				Name:  event.NewPerson.Name,
				City:  event.NewPerson.City,
				State: event.NewPerson.State,
				ID:    event.NewPerson.ID,
			}
		})
	*/
	return nil
}
