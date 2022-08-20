package handlers

import (
	"context"
	"encoding/json"
	"fmt"

	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	"sharedlog-stream/pkg/proc_interface"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/stream_task"

	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/ntypes"

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
	return common.CompressData(encodedOutput), nil
}

func q1mapFunc(_ string, value *ntypes.Event) (*ntypes.Event, error) {
	value.Bid.Price = uint64(value.Bid.Price * 908 / 1000.0)
	return value, nil
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
			processor.NewStreamFilterProcessorG[string, *ntypes.Event]("filterBid",
				processor.PredicateFuncG[string, *ntypes.Event](only_bid)))).
		Via(processor.NewMeteredProcessor(
			processor.NewStreamMapValuesProcessorG[string, *ntypes.Event, *ntypes.Event](
				"mapBid", processor.ValueMapperWithKeyFuncG[string, *ntypes.Event, *ntypes.Event](q1mapFunc)))).
		Via(processor.NewMeteredProcessor(
			processor.NewFixedSubstreamOutputProcessor(sinks[0], sp.ParNum)))
	task := stream_task.NewStreamTaskBuilder().MarkFinalStage().Build()
	streamTaskArgs := benchutil.UpdateStreamTaskArgs(sp,
		stream_task.NewStreamTaskArgsBuilder(h.env, &ectx,
			fmt.Sprintf("%s-%s-%d-%s", h.funcName, sp.InputTopicNames[0],
				sp.ParNum, sp.OutputTopicNames[0]))).
		FixedOutParNum(sp.ParNum).
		Build()
	return stream_task.ExecuteApp(ctx, task, streamTaskArgs)
}
