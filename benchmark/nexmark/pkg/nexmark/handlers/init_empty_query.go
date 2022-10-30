package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/ntypes"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/optional"
	"sharedlog-stream/pkg/proc_interface"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/stream_task"

	"cs.utexas.edu/zjia/faas/types"
)

type initQueryEmpty struct {
	env      types.Environment
	funcName string
}

func NewInitEmptyQuery(env types.Environment, funcName string) types.FuncHandler {
	return &initQueryEmpty{
		env:      env,
		funcName: funcName,
	}
}

func (h *initQueryEmpty) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &common.QueryInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output := h.queryEmpty(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	return common.CompressData(encodedOutput), nil
}

func emptyFunc(ctx context.Context, sp *common.QueryInput,
	funcName string, env types.Environment, subGraphTag string,
	initial bool, last bool,
) *common.FnOutput {
	srcs, sinks, err := getSrcSink(ctx, env, sp)
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
	if initial {
		srcs[0].SetInitialSource(true)
	}
	if last {
		sinks[0].MarkFinalOutput()
	}
	ectx := processor.NewExecutionContextFromComponents(proc_interface.NewBaseSrcsSinks(srcs, sinks),
		proc_interface.NewBaseProcArgs(funcName, sp.ScaleEpoch, sp.ParNum))
	outProc := processor.NewFixedSubstreamOutputProcessorG(subGraphTag, sinks[0], sp.ParNum, msgSerde)
	task := stream_task.NewStreamTaskBuilder().
		AppProcessFunc(func(ctx context.Context, task *stream_task.StreamTask, args processor.ExecutionContext) (*common.FnOutput, optional.Option[commtypes.RawMsgAndSeq]) {
			return stream_task.CommonProcess(ctx, task, args.(*processor.BaseExecutionContext),
				func(ctx context.Context, msg commtypes.MessageG[string, *ntypes.Event], argsTmp interface{}) error {
					return outProc.Process(ctx, msg)
				}, msgSerde)
		}).
		Build()
	streamTaskArgs := benchutil.UpdateStreamTaskArgs(sp,
		stream_task.NewStreamTaskArgsBuilder(env, &ectx,
			fmt.Sprintf("%s-%s-%d-%s", funcName, sp.InputTopicNames[0],
				sp.ParNum, sp.OutputTopicNames[0]))).
		FixedOutParNum(sp.ParNum).
		Build()
	return stream_task.ExecuteApp(ctx, task, streamTaskArgs, stream_task.EmptySetupSnapshotCallback,
		func() {
			outProc.OutputRemainingStats()
		})
}

func (h *initQueryEmpty) queryEmpty(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	return emptyFunc(ctx, sp, h.funcName, h.env, "subG1", true, false)
}
