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
	"sharedlog-stream/pkg/execution"
	"sharedlog-stream/pkg/proc_interface"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/source_sink"
	"sharedlog-stream/pkg/stream_task"
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
	task := stream_task.NewStreamTaskBuilder().
		AppProcessFunc(func(ctx context.Context, task *stream_task.StreamTask, argsTmp interface{}) *common.FnOutput {
			args := argsTmp.(proc_interface.ProcArgsWithSrcSink)
			return execution.CommonProcess(ctx, task, args, h.procMsg)
		}).
		InitFunc(func(progArgs interface{}) {
			src.StartWarmup()
			sink.StartWarmup()
			q2Filter.StartWarmup()
		}).Build()
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
