package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/ntypes"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/optional"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/producer_consumer"
	"sharedlog-stream/pkg/stream_task"
	"time"

	"cs.utexas.edu/zjia/faas/types"
)

type q8GroupByHandler struct {
	env      types.Environment
	funcName string
}

func NewQ8GroupByHandler(env types.Environment, funcName string) types.FuncHandler {
	return &q8GroupByHandler{
		env:      env,
		funcName: funcName,
	}
}

func (h *q8GroupByHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &common.QueryInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	fmt.Fprintf(os.Stderr, "inputParam: %+v\n", parsedInput)
	output := h.Q8GroupBy(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	return common.CompressData(encodedOutput), nil
}

func (h *q8GroupByHandler) getExecutionCtx(ctx context.Context, sp *common.QueryInput,
) (processor.BaseExecutionContext, error) {
	var sinks []producer_consumer.MeteredProducerIntr
	input_stream, output_streams, err := benchutil.GetShardedInputOutputStreams(ctx, h.env, sp)
	if err != nil {
		return processor.BaseExecutionContext{}, err
	}
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	inConfig := &producer_consumer.StreamConsumerConfig{
		Timeout:     common.SrcConsumeTimeout,
		SerdeFormat: serdeFormat,
	}
	outConfig := &producer_consumer.StreamSinkConfig{
		FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
		Format:        serdeFormat,
	}
	// fmt.Fprintf(os.Stderr, "output to %v\n", output_stream.TopicName())
	consumer, err := producer_consumer.NewShardedSharedLogStreamConsumer(input_stream, inConfig, sp.NumSubstreamProducer[0], sp.ParNum)
	if err != nil {
		return processor.BaseExecutionContext{}, err
	}
	src := producer_consumer.NewMeteredConsumer(consumer, time.Duration(sp.WarmupS)*time.Second)
	src.SetInitialSource(true)
	for _, output_stream := range output_streams {
		sink, err := producer_consumer.NewMeteredProducer(producer_consumer.NewShardedSharedLogStreamProducer(output_stream, outConfig), time.Duration(sp.WarmupS)*time.Second)
		if err != nil {
			return processor.BaseExecutionContext{}, err
		}
		sinks = append(sinks, sink)
	}
	sinks[0].SetName("auctionsBySellerIDSink")
	sinks[1].SetName("personsByIDSink")
	ectx, err := processor.NewExecutionContext([]*producer_consumer.MeteredConsumer{src}, sinks,
		h.funcName, sp.ScaleEpoch, sp.ParNum), nil
	debug.Assert(ectx.SubstreamNum() == sp.ParNum, "substream num mismatch")
	return ectx, err
}

func (h *q8GroupByHandler) Q8GroupBy(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	eventSerde, err := ntypes.GetEventSerdeG(serdeFormat)
	if err != nil {
		return &common.FnOutput{Message: fmt.Sprintf("get event serde err: %v", err), Success: false}
	}
	msgSerde, err := commtypes.GetMsgGSerdeG[string](serdeFormat, commtypes.StringSerdeG{}, eventSerde)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	ectx, err := h.getExecutionCtx(ctx, sp)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	outMsgSerde, err := commtypes.GetMsgGSerdeG[uint64](serdeFormat, commtypes.Uint64SerdeG{}, eventSerde)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	aucBySellerIdProc := processor.NewStreamSelectKeyProcessorG[string, *ntypes.Event, uint64]("auctionsBySellerIDMap",
		processor.SelectKeyFuncG[string, *ntypes.Event, uint64](
			func(key optional.Option[string], value optional.Option[*ntypes.Event]) (uint64, error) {
				v := value.Unwrap()
				return v.NewAuction.Seller, nil
			}))
	groupBySellerIDProc := processor.NewGroupByOutputProcessorG("aucProc", ectx.Producers()[0], &ectx, outMsgSerde)
	aucBySellerIdProc.NextProcessor(groupBySellerIDProc)

	perByIDProc := processor.NewStreamSelectKeyProcessorG[string, *ntypes.Event, uint64]("personsByIDMap",
		processor.SelectKeyFuncG[string, *ntypes.Event, uint64](
			func(key optional.Option[string], value optional.Option[*ntypes.Event]) (uint64, error) {
				v := value.Unwrap()
				return v.NewPerson.ID, nil
			}))
	groupByPerIDProc := processor.NewGroupByOutputProcessorG("perProc", ectx.Producers()[1], &ectx, outMsgSerde)
	perByIDProc.NextProcessor(groupByPerIDProc)

	task := stream_task.NewStreamTaskBuilder().
		AppProcessFunc(func(ctx context.Context, task *stream_task.StreamTask,
			argsTmp processor.ExecutionContext,
		) (*common.FnOutput, optional.Option[commtypes.RawMsgAndSeq]) {
			args := argsTmp.(*processor.BaseExecutionContext)
			return stream_task.CommonProcess(ctx, task, args,
				func(ctx context.Context, msg commtypes.MessageG[string, *ntypes.Event], argsTmp interface{}) error {
					event := msg.Value.Unwrap()
					if event.Etype == ntypes.AUCTION {
						err := aucBySellerIdProc.Process(ctx, msg)
						if err != nil {
							return err
						}
					} else if event.Etype == ntypes.PERSON {
						err := perByIDProc.Process(ctx, msg)
						if err != nil {
							return err
						}
					}
					return nil
				}, msgSerde)
		}).Build()
	transactionalID := fmt.Sprintf("%s-%s-%d",
		h.funcName, sp.InputTopicNames[0], sp.ParNum)
	streamTaskArgs := benchutil.UpdateStreamTaskArgs(sp,
		stream_task.NewStreamTaskArgsBuilder(h.env, &ectx, transactionalID)).
		Build()
	return stream_task.ExecuteApp(ctx, task, streamTaskArgs, stream_task.EmptySetupSnapshotCallback,
		func() {
			groupByPerIDProc.OutputRemainingStats()
			groupBySellerIDProc.OutputRemainingStats()
		})
}
