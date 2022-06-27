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
	"sharedlog-stream/pkg/execution"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/producer_consumer"
	"sharedlog-stream/pkg/stream_task"
	"sync"
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

type TwoMsgChanProcArgs struct {
	msgChan1 chan commtypes.Message
	msgChan2 chan commtypes.Message
	processor.BaseExecutionContext
}

func getExecutionCtx(ctx context.Context, env types.Environment, sp *common.QueryInput, funcName string,
) (processor.BaseExecutionContext, error) {
	input_stream, output_streams, err := benchutil.GetShardedInputOutputStreams(ctx, env, sp)
	if err != nil {
		return processor.EmptyBaseExecutionContext, err
	}
	debug.Assert(len(output_streams) == 2, "expected 2 output streams")
	var sinks []producer_consumer.MeteredProducerIntr
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	eventSerde, err := ntypes.GetEventSerde(serdeFormat)
	if err != nil {
		return processor.EmptyBaseExecutionContext, fmt.Errorf("get event serde err: %v", err)
	}
	inMsgSerde, err := commtypes.GetMsgSerde(serdeFormat, commtypes.StringSerde{}, eventSerde)
	if err != nil {
		return processor.EmptyBaseExecutionContext, fmt.Errorf("get msg serde err: %v", err)
	}
	outMsgSerde, err := commtypes.GetMsgSerde(serdeFormat, commtypes.Uint64Serde{}, eventSerde)
	if err != nil {
		return processor.EmptyBaseExecutionContext, fmt.Errorf("get msg serde err: %v", err)
	}
	inConfig := &producer_consumer.StreamConsumerConfig{
		Timeout:  common.SrcConsumeTimeout,
		MsgSerde: inMsgSerde,
	}
	outConfig := &producer_consumer.StreamSinkConfig{
		MsgSerde:      outMsgSerde,
		FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
	}
	// fmt.Fprintf(os.Stderr, "output to %v\n", output_stream.TopicName())
	warmup := time.Duration(sp.WarmupS) * time.Second
	src := producer_consumer.NewMeteredConsumer(producer_consumer.NewShardedSharedLogStreamConsumer(input_stream, inConfig), warmup)
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
	personsByIDFunc := h.getPersonsByID()
	auctionsBySellerIDFunc := h.getAucBySellerID()
	dctx, dcancel := context.WithCancel(ctx)
	defer dcancel()
	task, procArgs := PrepareProcessByTwoGeneralProc(dctx, auctionsBySellerIDFunc, personsByIDFunc,
		ectx, procMsgWithTwoMsgChan)
	streamTaskArgs := benchutil.UpdateStreamTaskArgs(sp,
		stream_task.NewStreamTaskArgsBuilder(h.env, procArgs,
			fmt.Sprintf("%s-%s-%d",
				h.funcName, sp.InputTopicNames[0], sp.ParNum))).Build()
	return task.ExecuteApp(dctx, streamTaskArgs)
}

func (h *q3GroupByHandler) getPersonsByID() execution.GeneralProcFunc {
	gpCtx := execution.NewGeneralProcCtx()
	gpCtx.AppendProcessor(processor.NewMeteredProcessor(processor.NewStreamFilterProcessor("filterPerson", processor.PredicateFunc(
		func(msg *commtypes.Message) (bool, error) {
			event := msg.Value.(*ntypes.Event)
			return event.Etype == ntypes.PERSON && ((event.NewPerson.State == "OR") ||
				event.NewPerson.State == "ID" || event.NewPerson.State == "CA"), nil
		}))))
	gpCtx.AppendProcessor(processor.NewMeteredProcessor(processor.NewStreamMapProcessor(
		"personsByIDMap",
		processor.MapperFunc(func(msg commtypes.Message) (commtypes.Message, error) {
			event := msg.Value.(*ntypes.Event)
			return commtypes.Message{
				Key:       event.NewPerson.ID,
				Value:     msg.Value,
				Timestamp: msg.Timestamp,
			}, nil
		}))))

	return func(ctx context.Context, argsTmp interface{}, wg *sync.WaitGroup, msgChan chan commtypes.Message, errChan chan error) {
		args := argsTmp.(*TwoMsgChanProcArgs)
		defer wg.Done()
		gpCtx.AppendProcessor(processor.NewGroupByOutputProcessor(args.Producers()[1], args))
		gpCtx.GeneralProc(ctx, args.Producers()[1], msgChan, errChan)
	}
}

func (h *q3GroupByHandler) getAucBySellerID() execution.GeneralProcFunc {
	gpCtx := execution.NewGeneralProcCtx()
	gpCtx.AppendProcessor(processor.NewMeteredProcessor(processor.NewStreamFilterProcessor("filterAuctions", processor.PredicateFunc(
		func(m *commtypes.Message) (bool, error) {
			event := m.Value.(*ntypes.Event)
			return event.Etype == ntypes.AUCTION && event.NewAuction.Category == 10, nil
		}))))

	gpCtx.AppendProcessor(processor.NewMeteredProcessor(processor.NewStreamMapProcessor(
		"auctionsBySellerIDMap", processor.MapperFunc(func(msg commtypes.Message) (commtypes.Message, error) {
			event := msg.Value.(*ntypes.Event)
			return commtypes.Message{Key: event.NewAuction.Seller, Value: msg.Value, Timestamp: msg.Timestamp}, nil
		}))))

	return func(ctx context.Context, argsTmp interface{}, wg *sync.WaitGroup,
		msgChan chan commtypes.Message, errChan chan error,
	) {
		args := argsTmp.(*TwoMsgChanProcArgs)
		gpCtx.AppendProcessor(processor.NewGroupByOutputProcessor(args.Producers()[0], args))
		defer wg.Done()
		gpCtx.GeneralProc(ctx, args.Producers()[0], msgChan, errChan)
	}
}
