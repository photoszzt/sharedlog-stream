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

type query2Handler struct {
	env types.Environment
}

func NewQuery2(env types.Environment) types.FuncHandler {
	return &query2Handler{
		env: env,
	}
}

func (h *query2Handler) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &common.QueryInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	outputCh := make(chan *common.FnOutput)
	go Query2(ctx, h.env, parsedInput, outputCh)
	output := <-outputCh
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	fmt.Printf("query 2 output: %v\n", encodedOutput)
	return utils.CompressData(encodedOutput), nil
}

func filterFunc(msg processor.Message) (bool, error) {
	event := msg.Value.(*ntypes.Event)
	return event.Bid.Auction%123 == 0, nil
}

func Query2(ctx context.Context, env types.Environment, input *common.QueryInput, output chan *common.FnOutput) {
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
	msgSerde, err := common.GetMsgSerde(input.SerdeFormat)
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

	builder := stream.NewStreamBuilder()
	builder.Source("nexmark-src", sharedlog_stream.NewSharedLogStreamSource(inputStream,
		int(input.Duration), processor.StringDecoder{}, eventSerde, msgSerde)).
		Filter("only_bid", processor.PredicateFunc(only_bid)).
		Filter("q2_filter", processor.PredicateFunc(filterFunc)).
		Process("sink", sharedlog_stream.NewSharedLogStreamSink(outputStream, processor.StringEncoder{}, eventSerde, msgSerde))
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
	// filterOp := operator.NewFilter(filterFunc)
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
		Latencies: latencies,
	}
}
