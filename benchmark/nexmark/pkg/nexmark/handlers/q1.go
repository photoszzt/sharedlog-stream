package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/execution"
	"sharedlog-stream/pkg/proc_interface"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/source_sink"
	"sharedlog-stream/pkg/stream_task"

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
		filterBid:     filterBid,
		q1Map:         q1Map,
		output_stream: output_streams[0],
		BaseProcArgsWithSrcSink: proc_interface.NewBaseProcArgsWithSrcSink(src,
			[]source_sink.Sink{sink}, h.funcName, sp.ScaleEpoch, sp.ParNum),
	}
	task := stream_task.NewStreamTaskBuilder().
		AppProcessFunc(func(ctx context.Context, task *stream_task.StreamTask, argsTmp interface{}) *common.FnOutput {
			args := argsTmp.(proc_interface.ProcArgsWithSrcSink)
			return execution.CommonProcess(ctx, task, args, h.procMsg)
		}).
		InitFunc(func(progArgs interface{}) {
			filterBid.StartWarmup()
			q1Map.StartWarmup()
			src.StartWarmup()
			sink.StartWarmup()
		}).Build()
	srcs := []source_sink.Source{src}
	sinks := []source_sink.Sink{sink}

	update_stats := func(ret *common.FnOutput) {
		ret.Latencies["filterBids"] = filterBid.GetLatency()
		ret.Latencies["q1Map"] = q1Map.GetLatency()
		ret.Latencies["eventTimeLatency"] = sink.GetEventTimeLatency()
		ret.Counts["src"] = src.GetCount()
		ret.Counts["sink"] = sink.GetCount()
	}
	if sp.EnableTransaction {
		transactionalID := fmt.Sprintf("%s-%s-%d-%s",
			h.funcName, sp.InputTopicNames[0], sp.ParNum, sp.OutputTopicNames[0])
		builder := stream_task.NewStreamTaskArgsTransactionBuilder().
			ProcArgs(procArgs).
			Env(h.env).
			Srcs(srcs).
			Sinks(sinks).
			TransactionalID(transactionalID)
		streamTaskArgs := benchutil.UpdateStreamTaskArgsTransaction(sp, builder).
			FixedOutParNum(sp.ParNum).Build()
		ret := stream_task.SetupManagersAndProcessTransactional(ctx, h.env, streamTaskArgs, task)
		if ret != nil && ret.Success {
			update_stats(ret)
		}
		return ret
	} else {
		return ExecuteAppNoTransaction(ctx, h.env, sp, task, srcs, sinks, procArgs, update_stats)
	}
}

type query1ProcessArgs struct {
	filterBid     *processor.MeteredProcessor
	q1Map         *processor.MeteredProcessor
	output_stream *sharedlog_stream.ShardedSharedLogStream
	proc_interface.BaseProcArgsWithSrcSink
}

func (h *query1Handler) procMsg(ctx context.Context, msg commtypes.Message, argsTmp interface{}) error {
	args := argsTmp.(*query1ProcessArgs)
	bidMsg, err := args.filterBid.ProcessAndReturn(ctx, msg)
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
	return nil
}
