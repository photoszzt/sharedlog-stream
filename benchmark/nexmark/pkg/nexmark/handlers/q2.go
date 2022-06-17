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
	"sharedlog-stream/pkg/stream_task"

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
	q2Filter *processor.MeteredProcessor
	proc_interface.BaseExecutionContext
}

func (h *query2Handler) Query2(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	srcs, sinks, err := getSrcSink(ctx, h.env, sp)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	srcs[0].SetInitialSource(true)
	sinks[0].MarkFinalOutput()
	srcsSinks := proc_interface.NewBaseSrcsSinks(srcs, sinks)

	q2Filter := processor.NewMeteredProcessor(processor.NewStreamFilterProcessor(processor.PredicateFunc(filterFunc)), time.Duration(sp.WarmupS)*time.Second)
	procArgs := &query2ProcessArgs{
		q2Filter: q2Filter,
		BaseExecutionContext: proc_interface.NewExecutionContextFromComponents(srcsSinks, proc_interface.NewBaseProcArgs(h.funcName,
			sp.ScaleEpoch, sp.ParNum)),
	}
	task := stream_task.NewStreamTaskBuilder().
		AppProcessFunc(func(ctx context.Context, task *stream_task.StreamTask, argsTmp interface{}) *common.FnOutput {
			args := argsTmp.(proc_interface.ExecutionContext)
			return execution.CommonProcess(ctx, task, args, h.procMsg)
		}).
		InitFunc(func(progArgs interface{}) {
			q2Filter.StartWarmup()
		}).Build()

	update_stats := func(ret *common.FnOutput) {
		ret.Latencies["q2Filter"] = q2Filter.GetLatency()
		ret.Latencies["eventTimeLatency"] = sinks[0].GetEventTimeLatency()
	}
	transactionalID := fmt.Sprintf("%s-%s-%d-%s", h.funcName, sp.InputTopicNames[0], sp.ParNum, sp.OutputTopicNames[0])
	streamTaskArgs := benchutil.UpdateStreamTaskArgs(sp,
		stream_task.NewStreamTaskArgsBuilder(h.env, procArgs, transactionalID)).
		FixedOutParNum(sp.ParNum).
		Build()
	return task.ExecuteApp(ctx, streamTaskArgs, update_stats)
}

func (h *query2Handler) procMsg(ctx context.Context, msg commtypes.Message, argsTmp interface{}) error {
	args := argsTmp.(*query2ProcessArgs)
	outMsg, err := args.q2Filter.ProcessAndReturn(ctx, msg)
	if err != nil {
		return err
	}
	if outMsg != nil {
		err = args.Sinks()[0].Produce(ctx, outMsg[0], args.SubstreamNum(), false)
		if err != nil {
			return err
		}
	}
	return nil
}
