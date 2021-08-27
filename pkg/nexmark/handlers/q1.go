package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
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
	event.Bid.Price = uint64(event.Bid.Price * 908 / 1000.0)
	return stream.Message{Value: event}, nil

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
		FilterFunc("only_bid", only_bid).
		MapFunc("q1_map", stream.MapperFunc(mapFunc)).
		Process("sink", sharedlog_stream.NewSharedLogStreamSink(outputStream))
	tp, err_arrs := builder.Build()
	if err_arrs != nil {
		output <- &ntypes.FnOutput{
			Success: false,
			Message: fmt.Sprintf("build stream failed: %v", err_arrs),
		}
	}
	pumps := make(map[stream.Node]stream.Pump)
	var srcPumps []stream.SourcePump
	nodes := stream.FlattenNodeTree(tp.Sources())
	stream.ReverseNodes(nodes)
	for _, node := range nodes {
		pipe := stream.NewPipe(node.Processor(), stream.ResolvePumps(pumps, node.Children()))
		node.Processor().WithPipe(pipe)

		pump := stream.NewSyncPump(node, pipe)
		pumps[node] = pump
	}
	for source, node := range tp.Sources() {
		srcPump := stream.NewSourcePump(node.Name(), source,
			stream.ResolvePumps(pumps, node.Children()), func(err error) {
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
