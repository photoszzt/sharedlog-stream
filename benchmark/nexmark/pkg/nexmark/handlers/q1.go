package handlers

import (
	"context"
	"encoding/json"
	"fmt"

	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/optional"
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

func q1mapFunc(_ optional.Option[string], value optional.Option[*ntypes.Event]) (*ntypes.Event, error) {
	v := value.Unwrap()
	v.Bid.Price = uint64(v.Bid.Price * 908 / 1000.0)
	return v, nil
}

func (h *query1Handler) Query1(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	srcs, sinks, err := getSrcSink(ctx, h.env, sp)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	eventSerde, err := ntypes.GetEventSerdeG(serdeFormat)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	msgSerde, err := commtypes.GetMsgGSerdeG[string](serdeFormat, commtypes.StringSerdeG{}, eventSerde)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	srcs[0].SetInitialSource(true)
	sinks[0].MarkFinalOutput()
	ectx := processor.NewExecutionContextFromComponents(proc_interface.NewBaseSrcsSinks(srcs, sinks),
		proc_interface.NewBaseProcArgs(h.funcName, sp.ScaleEpoch, sp.ParNum))

	filterProc :=
		processor.NewStreamFilterProcessorG[string, *ntypes.Event]("filterBid",
			processor.PredicateFuncG[string, *ntypes.Event](only_bid))
	mapProc :=
		processor.NewStreamMapValuesProcessorG[string, *ntypes.Event, *ntypes.Event](
			"mapBid", processor.ValueMapperWithKeyFuncG[string, *ntypes.Event, *ntypes.Event](q1mapFunc))
	outProc :=
		processor.NewFixedSubstreamOutputProcessorG(sinks[0], sp.ParNum, msgSerde)
	filterProc.NextProcessor(mapProc)
	mapProc.NextProcessor(outProc)
	task := stream_task.NewStreamTaskBuilder().MarkFinalStage().
		AppProcessFunc(func(ctx context.Context, task *stream_task.StreamTask, args processor.ExecutionContext) (*common.FnOutput, optional.Option[commtypes.RawMsgAndSeq]) {
			return stream_task.CommonProcess(ctx, task, args.(*processor.BaseExecutionContext),
				func(ctx context.Context, msg commtypes.MessageG[string, *ntypes.Event], argsTmp interface{}) error {
					return filterProc.Process(ctx, msg)
				}, msgSerde)
		}).
		Build()
	streamTaskArgs := benchutil.UpdateStreamTaskArgs(sp,
		stream_task.NewStreamTaskArgsBuilder(h.env, &ectx,
			fmt.Sprintf("%s-%s-%d-%s", h.funcName, sp.InputTopicNames[0],
				sp.ParNum, sp.OutputTopicNames[0]))).
		FixedOutParNum(sp.ParNum).
		Build()
	return stream_task.ExecuteApp(ctx, task, streamTaskArgs, stream_task.EmptySetupSnapshotCallback)
}
