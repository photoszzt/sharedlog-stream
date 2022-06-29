package handlers

import (
	"context"
	"encoding/json"
	"fmt"

	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
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

func filterFunc(key interface{}, value interface{}) (bool, error) {
	event := value.(*ntypes.Event)
	return event.Etype == ntypes.BID && event.Bid.Auction%123 == 0, nil
}

/*
type query2ProcessArgs struct {
	q2Filter *processor.MeteredProcessor
	processor.BaseExecutionContext
}
*/

func (h *query2Handler) Query2(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	srcs, sinks, err := getSrcSink(ctx, h.env, sp)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	srcs[0].SetInitialSource(true)
	sinks[0].MarkFinalOutput()
	srcsSinks := proc_interface.NewBaseSrcsSinks(srcs, sinks)
	ectx := processor.NewExecutionContextFromComponents(srcsSinks, proc_interface.NewBaseProcArgs(h.funcName,
		sp.ScaleEpoch, sp.ParNum))
	ectx.Via(processor.NewMeteredProcessor(
		processor.NewStreamFilterProcessor("q2Filter",
			processor.PredicateFunc(filterFunc)))).
		Via(processor.NewMeteredProcessor(
			processor.NewFixedSubstreamOutputProcessor(sinks[0], sp.ParNum)))
	task := stream_task.NewStreamTaskBuilder().
		AppProcessFunc(func(ctx context.Context, task *stream_task.StreamTask, argsTmp interface{}) *common.FnOutput {
			args := argsTmp.(processor.ExecutionContext)
			return execution.CommonProcess(ctx, task, args, processor.ProcessMsg)
		}).Build()
	transactionalID := fmt.Sprintf("%s-%s-%d-%s", h.funcName, sp.InputTopicNames[0], sp.ParNum, sp.OutputTopicNames[0])
	streamTaskArgs := benchutil.UpdateStreamTaskArgs(sp,
		stream_task.NewStreamTaskArgsBuilder(h.env, &ectx, transactionalID)).
		FixedOutParNum(sp.ParNum).
		Build()
	return task.ExecuteApp(ctx, streamTaskArgs)
}

/*
func (h *query2Handler) procMsg(ctx context.Context, msg commtypes.Message, argsTmp interface{}) error {
	args := argsTmp.(*query2ProcessArgs)
	outMsg, err := args.q2Filter.ProcessAndReturn(ctx, msg)
	if err != nil {
		return err
	}
	if outMsg != nil {
		err = args.Producers()[0].Produce(ctx, outMsg[0], args.SubstreamNum(), false)
		if err != nil {
			return err
		}
	}
	return nil
}
*/
