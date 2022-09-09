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
	return common.CompressData(encodedOutput), nil
}

func filterFunc(_ optional.Option[string], value optional.Option[*ntypes.Event]) (bool, error) {
	v := value.Unwrap()
	return v.Etype == ntypes.BID && v.Bid.Auction%123 == 0, nil
}

func (h *query2Handler) Query2(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	srcs, sinks, err := getSrcSink(ctx, h.env, sp)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	srcs[0].SetInitialSource(true)
	sinks[0].MarkFinalOutput()
	srcsSinks := proc_interface.NewBaseSrcsSinks(srcs, sinks)
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	eventSerde, err := ntypes.GetEventSerdeG(serdeFormat)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	msgSerde, err := commtypes.GetMsgGSerdeG[string](serdeFormat, commtypes.StringSerdeG{}, eventSerde)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	ectx := processor.NewExecutionContextFromComponents(srcsSinks, proc_interface.NewBaseProcArgs(h.funcName,
		sp.ScaleEpoch, sp.ParNum))
	filterProc := processor.NewStreamFilterProcessorG[string, *ntypes.Event]("q2Filter",
		processor.PredicateFuncG[string, *ntypes.Event](filterFunc))
	outProc := processor.NewFixedSubstreamOutputProcessorG(sinks[0], sp.ParNum, msgSerde)
	filterProc.NextProcessor(outProc)
	task := stream_task.NewStreamTaskBuilder().MarkFinalStage().
		AppProcessFunc(func(ctx context.Context, task *stream_task.StreamTask, args processor.ExecutionContext) (*common.FnOutput, optional.Option[commtypes.RawMsgAndSeq]) {
			return stream_task.CommonProcess(ctx, task, args.(*processor.BaseExecutionContext),
				func(ctx context.Context, msg commtypes.MessageG[string, *ntypes.Event], argsTmp interface{}) error {
					return filterProc.Process(ctx, msg)
				}, msgSerde)
		}).
		Build()
	transactionalID := fmt.Sprintf("%s-%s-%d-%s", h.funcName, sp.InputTopicNames[0], sp.ParNum, sp.OutputTopicNames[0])
	streamTaskArgs := benchutil.UpdateStreamTaskArgs(sp,
		stream_task.NewStreamTaskArgsBuilder(h.env, &ectx, transactionalID)).
		FixedOutParNum(sp.ParNum).
		Build()
	return stream_task.ExecuteApp(ctx, task, streamTaskArgs, stream_task.EmptySetupSnapshotCallback)
}
