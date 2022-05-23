package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/execution"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/source_sink"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/proc_interface"
	"sharedlog-stream/pkg/transaction"
	"sharedlog-stream/pkg/transaction/tran_interface"

	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"

	"cs.utexas.edu/zjia/faas/types"
)

type query1Handler struct {
	env      types.Environment
	funcName string
}

func NewQuery1(env types.Environment, funcName string) types.FuncHandler {
	return &query1Handler{
		env:      env,
		funcName: funcName,
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
	input_stream, output_streams, err := benchutil.GetShardedInputOutputStreams(ctx, h.env, sp)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("get input output stream failed: %v", err),
		}
	}
	debug.Assert(len(output_streams) == 1, "expected only one output stream")
	src, sink, err := getSrcSink(ctx, sp, input_stream, output_streams[0])
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	src.SetInitialSource(true)
	sink.MarkFinalOutput()

	filterBid := processor.NewMeteredProcessor(processor.NewStreamFilterProcessor(processor.PredicateFunc(
		only_bid)), time.Duration(sp.WarmupS)*time.Second)
	q1Map := processor.NewMeteredProcessor(processor.NewStreamMapValuesWithKeyProcessor(processor.MapperFunc(q1mapFunc)),
		time.Duration(sp.WarmupS)*time.Second)
	procArgs := &query1ProcessArgs{
		trackParFunc:         tran_interface.DefaultTrackSubstreamFunc,
		src:                  src,
		filterBid:            filterBid,
		q1Map:                q1Map,
		output_stream:        output_streams[0],
		BaseProcArgsWithSink: proc_interface.NewBaseProcArgsWithSink([]source_sink.Sink{sink}, h.funcName, sp.ScaleEpoch, sp.ParNum),
	}
	task := transaction.StreamTask{
		ProcessFunc:               h.process,
		CurrentOffset:             make(map[string]uint64),
		CommitEveryForAtLeastOnce: common.CommitDuration,
		PauseFunc:                 nil,
		ResumeFunc:                nil,
		InitFunc: func(progArgs interface{}) {
			filterBid.StartWarmup()
			q1Map.StartWarmup()
			src.StartWarmup()
			sink.StartWarmup()
		},
	}
	srcs := []source_sink.Source{src}
	sinks := []source_sink.Sink{sink}
	if sp.EnableTransaction {
		transactionalID := fmt.Sprintf("%s-%s-%d-%s",
			h.funcName, sp.InputTopicNames[0], sp.ParNum, sp.OutputTopicNames[0])
		streamTaskArgs := transaction.NewStreamTaskArgsTransaction(h.env, transactionalID, procArgs, srcs, sinks).
			WithFixedOutParNum(sp.ParNum)
		benchutil.UpdateStreamTaskArgsTransaction(sp, streamTaskArgs)
		ret := transaction.SetupManagersAndProcessTransactional(ctx, h.env, streamTaskArgs,
			func(procArgs interface{}, trackParFunc tran_interface.TrackKeySubStreamFunc,
				recordFinish tran_interface.RecordPrevInstanceFinishFunc) {
				procArgs.(*query1ProcessArgs).trackParFunc = trackParFunc
				procArgs.(*query1ProcessArgs).SetRecordFinishFunc(recordFinish)
			}, &task)
		if ret != nil && ret.Success {
			ret.Latencies["src"] = src.GetLatency()
			ret.Latencies["sink"] = sink.GetLatency()
			ret.Latencies["filterBids"] = filterBid.GetLatency()
			ret.Latencies["q1Map"] = q1Map.GetLatency()
			ret.Latencies["eventTimeLatency"] = sink.GetEventTimeLatency()
			ret.Consumed["src"] = src.GetCount()
		}
		return ret
	}
	streamTaskArgs := transaction.NewStreamTaskArgs(h.env, procArgs, srcs, sinks)
	benchutil.UpdateStreamTaskArgs(sp, streamTaskArgs)
	ret := task.Process(ctx, streamTaskArgs)
	if ret != nil && ret.Success {
		ret.Latencies["src"] = src.GetLatency()
		ret.Latencies["sink"] = sink.GetLatency()
		ret.Latencies["filterBids"] = filterBid.GetLatency()
		ret.Latencies["q1Map"] = q1Map.GetLatency()
		ret.Latencies["eventTimeLatency"] = sink.GetEventTimeLatency()
		ret.Consumed["src"] = src.GetCount()
	}
	return ret
}

type query1ProcessArgs struct {
	src           *source_sink.MeteredSource
	filterBid     *processor.MeteredProcessor
	q1Map         *processor.MeteredProcessor
	output_stream *sharedlog_stream.ShardedSharedLogStream
	trackParFunc  tran_interface.TrackKeySubStreamFunc
	proc_interface.BaseProcArgsWithSink
}

func (a *query1ProcessArgs) Source() source_sink.Source { return a.src }

func (h *query1Handler) process(ctx context.Context, t *transaction.StreamTask, argsTmp interface{}) *common.FnOutput {
	args := argsTmp.(*query1ProcessArgs)
	return execution.CommonProcess(ctx, t, args, func(t *transaction.StreamTask, msg commtypes.MsgAndSeq) error {
		t.CurrentOffset[args.src.TopicName()] = msg.LogSeqNum
		if msg.MsgArr != nil {
			for _, subMsg := range msg.MsgArr {
				if subMsg.Value == nil {
					continue
				}
				bidMsg, err := args.filterBid.ProcessAndReturn(ctx, subMsg)
				if err != nil {
					return err
				}
				if bidMsg != nil {
					filtered, err := args.q1Map.ProcessAndReturn(ctx, bidMsg[0])
					if err != nil {
						return err
					}
					err = args.Sinks()[0].Produce(ctx, filtered[0], args.ParNum(), false)
					if err != nil {
						return err
					}
				}
			}
		} else {
			bidMsg, err := args.filterBid.ProcessAndReturn(ctx, msg.Msg)
			if err != nil {
				return err
			}
			if bidMsg != nil {
				filtered, err := args.q1Map.ProcessAndReturn(ctx, bidMsg[0])
				if err != nil {
					return err
				}
				err = args.Sinks()[0].Produce(ctx, filtered[0], args.ParNum(), false)
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
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
