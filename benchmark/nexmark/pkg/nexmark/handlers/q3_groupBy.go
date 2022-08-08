package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/producer_consumer"
	"sharedlog-stream/pkg/stream_task"
	"time"

	"cs.utexas.edu/zjia/faas/types"
)

type q3GroupByHandler struct {
	env types.Environment

	funcName string
}

func NewQ3GroupByHandler(env types.Environment, funcName string) types.FuncHandler {
	return &q3GroupByHandler{
		env:      env,
		funcName: funcName,
	}
}

func (h *q3GroupByHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &common.QueryInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output := h.Q3GroupBy(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	return utils.CompressData(encodedOutput), nil
}

func getExecutionCtx(ctx context.Context, env types.Environment, sp *common.QueryInput, funcName string,
) (processor.BaseExecutionContext, error) {
	input_stream, output_streams, err := benchutil.GetShardedInputOutputStreams(ctx, env, sp)
	if err != nil {
		return processor.BaseExecutionContext{}, err
	}
	debug.Assert(len(output_streams) == 2, "expected 2 output streams")
	var sinks []producer_consumer.MeteredProducerIntr
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	eventSerde, err := ntypes.GetEventSerdeG(serdeFormat)
	if err != nil {
		return processor.BaseExecutionContext{}, fmt.Errorf("get event serde err: %v", err)
	}
	inMsgSerde, err := commtypes.GetMsgSerdeG[string](serdeFormat, commtypes.StringSerdeG{}, eventSerde)
	if err != nil {
		return processor.BaseExecutionContext{}, fmt.Errorf("get msg serde err: %v", err)
	}
	outMsgSerde, err := commtypes.GetMsgSerdeG[uint64](serdeFormat, commtypes.Uint64SerdeG{}, eventSerde)
	if err != nil {
		return processor.BaseExecutionContext{}, fmt.Errorf("get msg serde err: %v", err)
	}
	inConfig := &producer_consumer.StreamConsumerConfigG[string, *ntypes.Event]{
		Timeout:     common.SrcConsumeTimeout,
		MsgSerde:    inMsgSerde,
		SerdeFormat: serdeFormat,
	}
	outConfig := &producer_consumer.StreamSinkConfig[uint64, *ntypes.Event]{
		MsgSerde:      outMsgSerde,
		FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
	}
	// fmt.Fprintf(os.Stderr, "output to %v\n", output_stream.TopicName())
	warmup := time.Duration(sp.WarmupS) * time.Second
	consumer, err := producer_consumer.NewShardedSharedLogStreamConsumerG(input_stream, inConfig, sp.NumSubstreamProducer[0])
	if err != nil {
		return processor.BaseExecutionContext{}, fmt.Errorf("get msg serde err: %v", err)
	}
	src := producer_consumer.NewMeteredConsumer(consumer, warmup)
	for _, output_stream := range output_streams {
		sink := producer_consumer.NewMeteredProducer(producer_consumer.NewShardedSharedLogStreamProducer(output_stream, outConfig),
			warmup)
		sinks = append(sinks, sink)
	}
	return processor.NewExecutionContext([]producer_consumer.MeteredConsumerIntr{src}, sinks,
		funcName, sp.ScaleEpoch, sp.ParNum), nil
}

func (h *q3GroupByHandler) Q3GroupBy(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	ectx, err := getExecutionCtx(ctx, h.env, sp, h.funcName)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	ectx.Consumers()[0].SetInitialSource(true)
	ectx.Producers()[0].SetName("aucSink")
	ectx.Producers()[1].SetName("perSink")
	aucBySellerChain := processor.NewProcessorChains()
	aucBySellerChain.
		Via(processor.NewMeteredProcessor(processor.NewStreamSelectKeyProcessor(
			"auctionsBySellerIDMap",
			processor.SelectKeyFunc(func(_, value interface{}) (interface{}, error) {
				event := value.(*ntypes.Event)
				return event.NewAuction.Seller, nil
			})))).
		Via(processor.NewGroupByOutputProcessor(ectx.Producers()[0], &ectx))
	personsByIDChain := processor.NewProcessorChains()
	personsByIDChain.
		Via(processor.NewMeteredProcessor(processor.NewStreamSelectKeyProcessor(
			"personsByIDMap",
			processor.SelectKeyFunc(func(_, value interface{}) (interface{}, error) {
				event := value.(*ntypes.Event)
				return event.NewPerson.ID, nil
			})))).
		Via(processor.NewGroupByOutputProcessor(ectx.Producers()[1], &ectx))

	task := stream_task.NewStreamTaskBuilder().
		AppProcessFunc(func(ctx context.Context, task *stream_task.StreamTask,
			argsTmp processor.ExecutionContext,
		) (*common.FnOutput, *commtypes.MsgAndSeq) {
			args := argsTmp.(*processor.BaseExecutionContext)
			return stream_task.CommonProcess(ctx, task, args,
				func(ctx context.Context, msg commtypes.Message, _ interface{}) error {
					event := msg.Value.(*ntypes.Event)
					if event.Etype == ntypes.PERSON && ((event.NewPerson.State == "OR") ||
						event.NewPerson.State == "ID" || event.NewPerson.State == "CA") {
						_, err := personsByIDChain.RunChains(ctx, msg)
						if err != nil {
							return err
						}
					} else if event.Etype == ntypes.AUCTION && event.NewAuction.Category == 10 {
						_, err := aucBySellerChain.RunChains(ctx, msg)
						if err != nil {
							return err
						}
					}
					return nil
				})
		}).Build()
	streamTaskArgs := benchutil.UpdateStreamTaskArgs(sp,
		stream_task.NewStreamTaskArgsBuilder(h.env, &ectx,
			fmt.Sprintf("%s-%s-%d", h.funcName, sp.InputTopicNames[0], sp.ParNum))).Build()
	return stream_task.ExecuteApp(ctx, task, streamTaskArgs)
}
