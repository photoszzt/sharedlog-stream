package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/ntypes"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/optional"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/stream_task"

	"cs.utexas.edu/zjia/faas/types"
)

type q7BidByPrice struct {
	env      types.Environment
	funcName string
}

func NewQ7BidByPriceHandler(env types.Environment, funcName string) types.FuncHandler {
	return &q7BidByPrice{
		env:      env,
		funcName: funcName,
	}
}

func (h *q7BidByPrice) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &common.QueryInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output := h.q7BidByPrice(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	return common.CompressData(encodedOutput), nil
}

func (h *q7BidByPrice) q7BidByPrice(ctx context.Context, input *common.QueryInput) *common.FnOutput {
	serdeFormat := commtypes.SerdeFormat(input.SerdeFormat)
	eventSerde, err := ntypes.GetEventSerdeG(serdeFormat)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	inMsgSerde, err := commtypes.GetMsgGSerdeG[string](serdeFormat, commtypes.StringSerdeG{}, eventSerde)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	outMsgSerde, err := commtypes.GetMsgGSerdeG[uint64](serdeFormat, commtypes.Uint64SerdeG{}, eventSerde)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	srcs, sinks_arr, err := getSrcSinkUint64Key(ctx, h.env, input)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	srcs[0].SetInitialSource(true)
	ectx := processor.NewExecutionContext(srcs, sinks_arr, h.funcName,
		input.ScaleEpoch, input.ParNum)
	filterProc := processor.NewStreamFilterProcessorG[string, *ntypes.Event]("filterBids",
		processor.PredicateFuncG[string, *ntypes.Event](
			func(key optional.Option[string], value optional.Option[*ntypes.Event]) (bool, error) {
				event := value.Unwrap()
				return event.Etype == ntypes.BID, nil
			}))
	selectKey := processor.NewStreamSelectKeyProcessorG[string, *ntypes.Event, uint64]("bidKeyedByPrice",
		processor.SelectKeyFuncG[string, *ntypes.Event, uint64](
			func(key optional.Option[string], value optional.Option[*ntypes.Event]) (uint64, error) {
				event := value.Unwrap()
				return event.Bid.Price, nil
			}))
	outProc := processor.NewGroupByOutputProcessorG(sinks_arr[0], &ectx, outMsgSerde)
	filterProc.NextProcessor(selectKey)
	selectKey.NextProcessor(outProc)
	task := stream_task.NewStreamTaskBuilder().
		AppProcessFunc(func(ctx context.Context, task *stream_task.StreamTask, args processor.ExecutionContext) (
			*common.FnOutput, optional.Option[commtypes.RawMsgAndSeq],
		) {
			return stream_task.CommonProcess(ctx, task, args.(*processor.BaseExecutionContext),
				func(ctx context.Context, msg commtypes.MessageG[string, *ntypes.Event], argsTmp interface{}) error {
					return filterProc.Process(ctx, msg)
				}, inMsgSerde)
		}).
		Build()
	transactionalID := fmt.Sprintf("%s-%s-%d-%s", h.funcName, input.InputTopicNames[0],
		input.ParNum, input.OutputTopicNames[0])
	streamTaskArgs := benchutil.UpdateStreamTaskArgs(input,
		stream_task.NewStreamTaskArgsBuilder(h.env, &ectx, transactionalID)).Build()
	return stream_task.ExecuteApp(ctx, task, streamTaskArgs, stream_task.EmptySetupSnapshotCallback)
}
