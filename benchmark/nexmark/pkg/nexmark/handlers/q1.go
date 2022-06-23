package handlers

import (
	"context"
	"encoding/json"
	"fmt"

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
	srcs, sinks, err := getSrcSink(ctx, h.env, sp)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	srcs[0].SetInitialSource(true)
	sinks[0].MarkFinalOutput()
	ectx := processor.NewExecutionContextFromComponents(proc_interface.NewBaseSrcsSinks(srcs, sinks),
		proc_interface.NewBaseProcArgs(h.funcName, sp.ScaleEpoch, sp.ParNum))
	ectx.
		Via(processor.NewMeteredProcessor(
			processor.NewStreamFilterProcessor("filterBid", processor.PredicateFunc(only_bid)))).
		Via(processor.NewMeteredProcessor(
			processor.NewStreamMapValuesWithKeyProcessor(processor.MapperFunc(q1mapFunc)))).
		Via(processor.NewMeteredProcessor(
			processor.NewFixedSubstreamOutputProcessor(sinks[0], sp.ParNum)))
	task := stream_task.NewStreamTaskBuilder().
		AppProcessFunc(func(ctx context.Context, task *stream_task.StreamTask, argsTmp interface{}) *common.FnOutput {
			args := argsTmp.(processor.ExecutionContext)
			return execution.CommonProcess(ctx, task, args, processor.ProcessMsg)
		}).
		InitFunc(func(progArgs interface{}) {
		}).Build()
	streamTaskArgs := benchutil.UpdateStreamTaskArgs(sp,
		stream_task.NewStreamTaskArgsBuilder(h.env, &ectx,
			fmt.Sprintf("%s-%s-%d-%s", h.funcName, sp.InputTopicNames[0],
				sp.ParNum, sp.OutputTopicNames[0]))).
		FixedOutParNum(sp.ParNum).
		Build()
	return task.ExecuteApp(ctx, streamTaskArgs)
}

/*
type query1ProcessArgs struct {
	filterBid *processor.MeteredProcessor
	q1Map     *processor.MeteredProcessor
	processor.BaseExecutionContext
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
		if filtered != nil {
			err = args.Producers()[0].Produce(ctx, filtered[0], args.SubstreamNum(), false)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
*/
