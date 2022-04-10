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
	"sharedlog-stream/pkg/stream/processor/store"

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
	parsedInput := &common.QueryInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output := Query8(ctx, h.env, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	fmt.Printf("query 8 output: %v\n", encodedOutput)
	return utils.CompressData(encodedOutput), nil
}

func Query8(ctx context.Context, env types.Environment, input *common.QueryInput) *common.FnOutput {
	inputStream, err := sharedlog_stream.NewSharedLogStream(env, input.InputTopicNames[0],
		commtypes.SerdeFormat(input.SerdeFormat))
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	outputStream, err := sharedlog_stream.NewSharedLogStream(env, input.OutputTopicName,
		commtypes.SerdeFormat(input.SerdeFormat))
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	msgSerde, err := commtypes.GetMsgSerde(input.SerdeFormat)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	eventSerde, err := getEventSerde(input.SerdeFormat)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}

	ptSerde, err := getPersonTimeSerde(input.SerdeFormat)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}

	inConfig := &sharedlog_stream.StreamSourceConfig{
		Timeout:      time.Duration(input.Duration) * time.Second,
		KeyDecoder:   commtypes.StringDecoder{},
		ValueDecoder: eventSerde,
		MsgDecoder:   msgSerde,
	}
	outConfig := &sharedlog_stream.StreamSinkConfig{
		KeySerde:   commtypes.Uint64Serde{},
		ValueSerde: ptSerde,
		MsgSerde:   msgSerde,
	}
	builder := stream.NewStreamBuilder()
	inputs := builder.Source("nexmark-src", sharedlog_stream.NewSharedLogStreamSource(inputStream, inConfig))
	person := inputs.Filter("filter-person",
		processor.PredicateFunc(func(msg *commtypes.Message) (bool, error) {
			event := msg.Value.(*ntypes.Event)
			return event.Etype == ntypes.PERSON, nil
		})).
		Map("select-key",
			processor.MapperFunc(func(msg commtypes.Message) (commtypes.Message, error) {
				event := msg.Value.(*ntypes.Event)
				return commtypes.Message{Key: event.NewPerson.ID, Value: msg.Value, Timestamp: msg.Timestamp}, nil
			}))
	auction := inputs.Filter("filter-auction",
		processor.PredicateFunc(func(msg *commtypes.Message) (bool, error) {
			event := msg.Value.(ntypes.Event)
			return event.Etype == ntypes.AUCTION, nil
		})).
		Map("select-key",
			processor.MapperFunc(func(msg commtypes.Message) (commtypes.Message, error) {
				event := msg.Value.(*ntypes.Event)
				return commtypes.Message{Key: event.NewAuction.Seller, Value: msg.Value, Timestamp: msg.Timestamp}, nil
			}))
	joinW, err := processor.NewJoinWindowsNoGrace(time.Duration(10) * time.Second)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	auction.StreamStreamJoin("join-auction-person", person,
		processor.ValueJoinerWithKeyFunc(func(readOnlyKey interface{}, leftValue interface{}, rightValue interface{}) interface{} {
			rv := rightValue.(*ntypes.Event)
			return &ntypes.PersonTime{
				ID:        rv.NewPerson.ID,
				Name:      rv.NewPerson.Name,
				StartTime: 0,
			}
		}), joinW,
		&store.JoinParam{
			LeftWindowStoreName:  "auction-window-store",
			RightWindowStoreName: "person-window-store",
		}).
		Process("sink", sharedlog_stream.NewSharedLogStreamSink(outputStream, outConfig))
	tp, err_arrs := builder.Build()
	if err_arrs != nil {
		return &common.FnOutput{
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
		srcPump := processor.NewSourcePump(ctx, node.Name(), source, 0,
			processor.ResolvePumps(pumps, node.Children()), func(err error) {
				log.Fatal(err.Error())
			})
		srcPumps = append(srcPumps, srcPump)
	}

	duration := time.Duration(input.Duration) * time.Second
	latencies := make([]int, 0, 128)
	startTime := time.Now()

	time.After(duration)
	for _, srcPump := range srcPumps {
		srcPump.Stop()
		srcPump.Close()
	}

	return &common.FnOutput{
		Success:   true,
		Duration:  time.Since(startTime).Seconds(),
		Latencies: map[string][]int{"e2e": latencies},
	}

}
