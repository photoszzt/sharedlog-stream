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

type q46GroupByHandler struct {
	env      types.Environment
	funcName string
}

func NewQ46GroupByHandler(env types.Environment, funcName string) types.FuncHandler {
	return &q46GroupByHandler{
		env:      env,
		funcName: funcName,
	}
}

func (h *q46GroupByHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &common.QueryInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output := h.Q46GroupBy(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	return common.CompressData(encodedOutput), nil
}

func (h *q46GroupByHandler) Q46GroupBy(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	eventSerde, err := ntypes.GetEventSerdeG(serdeFormat)
	if err != nil {
		return &common.FnOutput{Success: false, Message: fmt.Sprintf("get event serde err: %v", err)}
	}
	inMsgSerde, err := commtypes.GetMsgGSerdeG[string](serdeFormat, commtypes.StringSerdeG{}, eventSerde)
	if err != nil {
		return &common.FnOutput{Success: false, Message: fmt.Sprintf("get msg serde err: %v", err)}
	}
	outMsgSerde, err := commtypes.GetMsgGSerdeG[uint64](serdeFormat, commtypes.Uint64SerdeG{}, eventSerde)
	if err != nil {
		return &common.FnOutput{Success: false, Message: fmt.Sprintf("get msg serde err: %v", err)}
	}
	ectx, err := getExecutionCtx(ctx, h.env, sp, h.funcName)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	ectx.Consumers()[0].SetInitialSource(true)
	ectx.Producers()[0].SetName("aucsByIDSink")
	ectx.Producers()[1].SetName("bidsByAucIDSink")
	aucByIDProc := processor.NewStreamSelectKeyProcessorG[string, *ntypes.Event, uint64]("auctionsByIDMap",
		processor.SelectKeyFuncG[string, *ntypes.Event, uint64](
			func(key optional.Option[string], value optional.Option[*ntypes.Event]) (uint64, error) {
				event := value.Unwrap()
				return event.NewAuction.ID, nil
			}))
	groupByAucIDProc := processor.NewGroupByOutputProcessorG(ectx.Producers()[0], &ectx, outMsgSerde)
	aucByIDProc.NextProcessor(groupByAucIDProc)

	bidsByAucIDProc := processor.NewStreamSelectKeyProcessorG[string, *ntypes.Event, uint64]("bidsByAuctionIDMap",
		processor.SelectKeyFuncG[string, *ntypes.Event, uint64](func(_ optional.Option[string], value optional.Option[*ntypes.Event]) (uint64, error) {
			event := value.Unwrap()
			return event.Bid.Auction, nil
		}))
	grouByAucIDProc := processor.NewGroupByOutputProcessorG(ectx.Producers()[1], &ectx, outMsgSerde)
	bidsByAucIDProc.NextProcessor(grouByAucIDProc)

	task := stream_task.NewStreamTaskBuilder().AppProcessFunc(
		func(ctx context.Context, task *stream_task.StreamTask,
			argsTmp processor.ExecutionContext,
		) (*common.FnOutput, optional.Option[commtypes.RawMsgAndSeq]) {
			args := argsTmp.(*processor.BaseExecutionContext)
			return stream_task.CommonProcess(ctx, task, args,
				func(ctx context.Context, msg commtypes.MessageG[string, *ntypes.Event], argsTmp interface{}) error {
					event := msg.Value.Unwrap()
					if event.Etype == ntypes.AUCTION {
						err := aucByIDProc.Process(ctx, msg)
						if err != nil {
							return err
						}
					} else if event.Etype == ntypes.BID {
						err := bidsByAucIDProc.Process(ctx, msg)
						if err != nil {
							return err
						}
					}
					return nil
				}, inMsgSerde)
		}).Build()
	transactionalID := fmt.Sprintf("%s-%s-%d", h.funcName, sp.InputTopicNames[0], sp.ParNum)
	streamTaskArgs := benchutil.UpdateStreamTaskArgs(sp,
		stream_task.NewStreamTaskArgsBuilder(h.env, &ectx, transactionalID)).Build()
	return stream_task.ExecuteApp(ctx, task, streamTaskArgs, stream_task.EmptySetupSnapshotCallback)
}
