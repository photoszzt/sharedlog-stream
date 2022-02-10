package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"

	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"

	"cs.utexas.edu/zjia/faas/types"
)

type query1Handler struct {
	env           types.Environment
	currentOffset map[string]uint64
}

func NewQuery1(env types.Environment) types.FuncHandler {
	return &query1Handler{
		env:           env,
		currentOffset: make(map[string]uint64),
	}
}

func (h *query1Handler) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &common.QueryInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output := h.Query1(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	return utils.CompressData(encodedOutput), nil
}

func q1mapFunc(msg commtypes.Message) (commtypes.Message, error) {
	event := msg.Value.(*ntypes.Event)
	event.Bid.Price = uint64(event.Bid.Price * 908 / 1000.0)
	return commtypes.Message{Value: event}, nil
}

func (h *query1Handler) Query1(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	input_stream, output_stream, err := benchutil.GetShardedInputOutputStreams(ctx, h.env, sp)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("get input output stream failed: %v", err),
		}
	}
	src, sink, msgSerde, err := getSrcSink(ctx, sp, input_stream, output_stream)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}

	filterBid := processor.NewMeteredProcessor(processor.NewStreamFilterProcessor(processor.PredicateFunc(
		only_bid)))
	q1Map := processor.NewMeteredProcessor(processor.NewStreamMapValuesWithKeyProcessor(processor.MapperFunc(q1mapFunc)))

	procArgs := &query1ProcessArgs{
		sink:          sink,
		trackParFunc:  sharedlog_stream.DefaultTrackParFunc,
		src:           src,
		filterBid:     filterBid,
		q1Map:         q1Map,
		output_stream: output_stream,
		parNum:        sp.ParNum,
	}
	task := sharedlog_stream.StreamTask{
		ProcessFunc: h.process,
	}
	if sp.EnableTransaction {
		srcs := make(map[string]processor.Source)
		srcs[sp.InputTopicNames[0]] = src
		streamTaskArgs := sharedlog_stream.StreamTaskArgsTransaction{
			ProcArgs:              procArgs,
			Env:                   h.env,
			MsgSerde:              msgSerde,
			Srcs:                  srcs,
			OutputStream:          output_stream,
			QueryInput:            sp,
			TransactionalId:       fmt.Sprintf("q1Query-%s-%d-%s", sp.InputTopicNames[0], sp.ParNum, sp.OutputTopicName),
			KVChangelogs:          nil,
			WindowStoreChangelogs: nil,
			FixedOutParNum:        sp.ParNum,
			CHash:                 nil,
			CHashMu:               nil,
		}
		ret := sharedlog_stream.SetupManagersAndProcessTransactional(ctx, h.env, &streamTaskArgs,
			func(procArgs interface{}, trackParFunc func([]uint8) error) {
				procArgs.(*query1ProcessArgs).trackParFunc = trackParFunc
			}, &task)
		if ret != nil && ret.Success {
			ret.Latencies["src"] = src.GetLatency()
			ret.Latencies["sink"] = sink.GetLatency()
			ret.Latencies["filterBids"] = filterBid.GetLatency()
			ret.Latencies["q1Map"] = q1Map.GetLatency()
		}
		return ret
	}
	streamTaskArgs := sharedlog_stream.StreamTaskArgs{
		ProcArgs: procArgs,
		Duration: time.Duration(sp.Duration) * time.Second,
	}
	ret := task.Process(ctx, &streamTaskArgs)
	if ret != nil && ret.Success {
		ret.Latencies["src"] = src.GetLatency()
		ret.Latencies["sink"] = sink.GetLatency()
		ret.Latencies["filterBids"] = filterBid.GetLatency()
		ret.Latencies["q1Map"] = q1Map.GetLatency()
	}
	return ret
}

type query1ProcessArgs struct {
	src           *processor.MeteredSource
	sink          *processor.MeteredSink
	filterBid     *processor.MeteredProcessor
	q1Map         *processor.MeteredProcessor
	output_stream *sharedlog_stream.ShardedSharedLogStream
	trackParFunc  func([]uint8) error
	parNum        uint8
}

func (h *query1Handler) process(ctx context.Context, argsTmp interface{}) (map[string]uint64, *common.FnOutput) {
	args := argsTmp.(*query1ProcessArgs)
	gotMsgs, err := args.src.Consume(ctx, args.parNum)
	if err != nil {
		if errors.Is(err, sharedlog_stream.ErrStreamSourceTimeout) {
			return h.currentOffset, &common.FnOutput{
				Success: true,
				Message: err.Error(),
			}
		}
		return h.currentOffset, &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}

	for _, msg := range gotMsgs {
		if msg.Msg.Value == nil {
			continue
		}
		h.currentOffset[args.src.TopicName()] = msg.LogSeqNum
		bidMsg, err := args.filterBid.ProcessAndReturn(ctx, msg.Msg)
		if err != nil {
			return h.currentOffset, &common.FnOutput{
				Success: false,
				Message: err.Error(),
			}
		}
		if bidMsg != nil {
			filtered, err := args.q1Map.ProcessAndReturn(ctx, bidMsg[0])
			if err != nil {
				return h.currentOffset, &common.FnOutput{
					Success: false,
					Message: err.Error(),
				}
			}
			err = args.sink.Sink(ctx, filtered[0], args.parNum, false)
			if err != nil {
				return h.currentOffset, &common.FnOutput{
					Success: false,
					Message: err.Error(),
				}
			}
		}
	}
	return h.currentOffset, nil
}

/*
func Query1(ctx context.Context, env types.Environment, input *common.QueryInput, output chan *common.FnOutput) {
	// fmt.Fprintf(os.Stderr, "input topic name: %v, output topic name: %v\n", input.InputTopicName, input.OutputTopicName)
	inputStream := sharedlog_stream.NewSharedLogStream(env, input.InputTopicName)
	outputStream := sharedlog_stream.NewSharedLogStream(env, input.OutputTopicName)
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
		KeyEncoder:   commtypes.StringEncoder{},
		ValueEncoder: eventSerde,
		MsgEncoder:   msgSerde,
	}
	builder := stream.NewStreamBuilder()
	builder.Source("nexmark_src", sharedlog_stream.NewSharedLogStreamSource(inputStream, inConfig)).
		Filter("only_bid", processor.PredicateFunc(only_bid)).
		Map("q1_map", processor.MapperFunc(q1mapFunc)).
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
		srcPump := processor.NewSourcePump(ctx, node.Name(), source, 0, processor.ResolvePumps(pumps, node.Children()), func(err error) {
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

	output <- &common.FnOutput{
		Success:   true,
		Duration:  time.Since(startTime).Seconds(),
		Latencies: map[string][]int{"e2e": latencies},
	}
}
*/
