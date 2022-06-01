package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/execution"
	"sharedlog-stream/pkg/proc_interface"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/source_sink"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/transaction"
	"sharedlog-stream/pkg/transaction/tran_interface"

	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"

	"cs.utexas.edu/zjia/faas/types"
)

type query2Handler struct {
	env      types.Environment
	funcName string
}

func NewQuery2(env types.Environment, funcName string) types.FuncHandler {
	return &query2Handler{
		env:      env,
		funcName: funcName,
	}
}

func (h *query2Handler) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &common.QueryInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output := h.Query2(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	return utils.CompressData(encodedOutput), nil
}

func filterFunc(msg *commtypes.Message) (bool, error) {
	event := msg.Value.(*ntypes.Event)
	return event.Etype == ntypes.BID && event.Bid.Auction%123 == 0, nil
}

type query2ProcessArgs struct {
	src           *source_sink.MeteredSource
	q2Filter      *processor.MeteredProcessor
	output_stream *sharedlog_stream.ShardedSharedLogStream
	trackParFunc  tran_interface.TrackKeySubStreamFunc
	proc_interface.BaseProcArgsWithSink
}

func (a *query2ProcessArgs) Source() source_sink.Source { return a.src }

func (h *query2Handler) Query2(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	input_stream, output_streams, err := benchutil.GetShardedInputOutputStreams(ctx, h.env, sp)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("get input output stream failed: %v", err),
		}
	}
	src, sink, err := getSrcSink(ctx, sp, input_stream, output_streams[0])
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	src.SetInitialSource(true)
	sink.MarkFinalOutput()
	q2Filter := processor.NewMeteredProcessor(processor.NewStreamFilterProcessor(processor.PredicateFunc(filterFunc)), time.Duration(sp.WarmupS)*time.Second)
	procArgs := &query2ProcessArgs{
		src:           src,
		q2Filter:      q2Filter,
		output_stream: output_streams[0],
		trackParFunc:  tran_interface.DefaultTrackSubstreamFunc,
		BaseProcArgsWithSink: proc_interface.NewBaseProcArgsWithSink([]source_sink.Sink{sink}, h.funcName,
			sp.ScaleEpoch, sp.ParNum),
	}
	task := transaction.StreamTask{
		ProcessFunc: func(ctx context.Context, task *transaction.StreamTask, argsTmp interface{}) *common.FnOutput {
			args := argsTmp.(proc_interface.ProcArgsWithSrcSink)
			return execution.CommonProcess(ctx, task, args, h.procMsg)
		},
		CurrentOffset:             make(map[string]uint64),
		CommitEveryForAtLeastOnce: common.CommitDuration,
		PauseFunc:                 nil,
		ResumeFunc:                nil,
		InitFunc: func(progArgs interface{}) {
			src.StartWarmup()
			sink.StartWarmup()
			q2Filter.StartWarmup()
		},
	}
	srcs := []source_sink.Source{src}
	sinks := []source_sink.Sink{sink}

	update_stats := func(ret *common.FnOutput) {
		ret.Latencies["q2Filter"] = q2Filter.GetLatency()
		ret.Latencies["eventTimeLatency"] = sink.GetEventTimeLatency()
		ret.Counts["src"] = src.GetCount()
		ret.Counts["sink"] = sink.GetCount()
	}
	if sp.EnableTransaction {
		transactionalID := fmt.Sprintf("%s-%s-%d-%s", h.funcName, sp.InputTopicNames[0], sp.ParNum, sp.OutputTopicNames[0])
		streamTaskArgs := transaction.NewStreamTaskArgsTransaction(h.env, transactionalID, procArgs, srcs, sinks).
			WithFixedOutParNum(sp.ParNum)
		benchutil.UpdateStreamTaskArgsTransaction(sp, streamTaskArgs)
		ret := transaction.SetupManagersAndProcessTransactional(ctx, h.env, streamTaskArgs, &task)
		if ret != nil && ret.Success {
			update_stats(ret)
		}
		return ret
	}
	streamTaskArgs := transaction.NewStreamTaskArgs(h.env, procArgs, srcs, sinks)
	benchutil.UpdateStreamTaskArgs(sp, streamTaskArgs)
	ret := task.Process(ctx, streamTaskArgs)
	if ret != nil && ret.Success {
		update_stats(ret)
	}
	return ret
}

func (h *query2Handler) procMsg(ctx context.Context, msg commtypes.Message, argsTmp interface{}) error {
	args := argsTmp.(*query2ProcessArgs)
	outMsg, err := args.q2Filter.ProcessAndReturn(ctx, msg)
	if err != nil {
		return err
	}
	if outMsg != nil {
		err = args.Sinks()[0].Produce(ctx, outMsg[0], args.ParNum(), false)
		if err != nil {
			return err
		}
	}
	return nil
}

/*
func Query2(ctx context.Context, env types.Environment, input *common.QueryInput, output chan *common.FnOutput) {
	// fmt.Fprintf(os.Stderr, "input topic name is %v\n", input.InputTopicName)
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
		Filter("q2_filter", processor.PredicateFunc(filterFunc)).
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
	output <- &common.FnOutput{
		Success:   true,
		Duration:  time.Since(startTime).Seconds(),
		Latencies: map[string][]int{"e2e": latencies},
	}
}
*/
