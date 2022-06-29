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
	"sharedlog-stream/pkg/execution"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/producer_consumer"
	"sharedlog-stream/pkg/stream_task"
	"sync"
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
	output := h.Q8GroupBy(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	return utils.CompressData(encodedOutput), nil
}

func (h *q8GroupByHandler) getExecutionCtx(ctx context.Context, sp *common.QueryInput,
) (processor.BaseExecutionContext, error) {
	var sinks []producer_consumer.MeteredProducerIntr
	input_stream, output_streams, err := benchutil.GetShardedInputOutputStreams(ctx, h.env, sp)
	if err != nil {
		return processor.EmptyBaseExecutionContext, err
	}
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	eventSerde, err := ntypes.GetEventSerde(serdeFormat)
	if err != nil {
		return processor.EmptyBaseExecutionContext, fmt.Errorf("get event serde err: %v", err)
	}
	msgSerde, err := commtypes.GetMsgSerde(serdeFormat, commtypes.StringSerde{}, eventSerde)
	if err != nil {
		return processor.EmptyBaseExecutionContext, fmt.Errorf("get msg serde err: %v", err)
	}
	outMsgSerde, err := commtypes.GetMsgSerde(serdeFormat, commtypes.Uint64Serde{}, eventSerde)
	if err != nil {
		return processor.EmptyBaseExecutionContext, fmt.Errorf("get msg serde err: %v", err)
	}
	inConfig := &producer_consumer.StreamConsumerConfig{
		Timeout:  common.SrcConsumeTimeout,
		MsgSerde: msgSerde,
	}
	outConfig := &producer_consumer.StreamSinkConfig{
		MsgSerde:      outMsgSerde,
		FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
	}
	// fmt.Fprintf(os.Stderr, "output to %v\n", output_stream.TopicName())
	src := producer_consumer.NewMeteredConsumer(producer_consumer.NewShardedSharedLogStreamConsumer(input_stream, inConfig), time.Duration(sp.WarmupS)*time.Second)
	src.SetInitialSource(true)
	for _, output_stream := range output_streams {
		sink := producer_consumer.NewMeteredProducer(producer_consumer.NewShardedSharedLogStreamProducer(output_stream, outConfig), time.Duration(sp.WarmupS)*time.Second)
		sinks = append(sinks, sink)
	}
	sinks[0].SetName("auctionsBySellerIDSink")
	sinks[1].SetName("personsByIDSink")
	return processor.NewExecutionContext([]producer_consumer.MeteredConsumerIntr{src}, sinks,
		h.funcName, sp.ScaleEpoch, sp.ParNum), nil
}

func (h *q8GroupByHandler) Q8GroupBy(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	ectx, err := h.getExecutionCtx(ctx, sp)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	personsByIDFunc := h.getPersonsByID()
	auctionsBySellerIDFunc := h.getAucBySellerID()
	dctx, dcancel := context.WithCancel(ctx)
	defer dcancel()
	task, procArgs := PrepareProcessByTwoGeneralProc(dctx, auctionsBySellerIDFunc,
		personsByIDFunc, ectx, procMsgWithTwoMsgChan)
	transactionalID := fmt.Sprintf("%s-%s-%d",
		h.funcName, sp.InputTopicNames[0], sp.ParNum)
	streamTaskArgs := benchutil.UpdateStreamTaskArgs(sp,
		stream_task.NewStreamTaskArgsBuilder(h.env, procArgs, transactionalID)).
		Build()
	return task.ExecuteApp(dctx, streamTaskArgs)
}

func (h *q8GroupByHandler) getPersonsByID() execution.GeneralProcFunc {
	gpCtx := execution.NewGeneralProcCtx()
	gpCtx.AppendProcessor(processor.NewMeteredProcessor(processor.NewStreamFilterProcessor("filterPerson",
		processor.PredicateFunc(
			func(key, value interface{}) (bool, error) {
				event := value.(*ntypes.Event)
				return event.Etype == ntypes.PERSON, nil
			}))))
	gpCtx.AppendProcessor(processor.NewMeteredProcessor(processor.NewStreamSelectKeyProcessor("personsByIDMap",
		processor.SelectKeyFunc(func(key, value interface{}) (interface{}, error) {
			event := value.(*ntypes.Event)
			return event.NewPerson.ID, nil
		}))))
	return func(ctx context.Context, argsTmp interface{}, wg *sync.WaitGroup,
		msgChan chan commtypes.Message, errChan chan error,
	) {
		args := argsTmp.(*TwoMsgChanProcArgs)
		gpCtx.AppendProcessor(processor.NewGroupByOutputProcessor(args.Producers()[1], args))
		defer wg.Done()
		gpCtx.GeneralProc(ctx, args.Producers()[1], msgChan, errChan)
	}
}

func (h *q8GroupByHandler) getAucBySellerID() execution.GeneralProcFunc {
	gpCtx := execution.NewGeneralProcCtx()
	gpCtx.AppendProcessor(processor.NewMeteredProcessor(processor.NewStreamFilterProcessor(
		"filterAuctions", processor.PredicateFunc(
			func(key, value interface{}) (bool, error) {
				event := value.(*ntypes.Event)
				return event.Etype == ntypes.AUCTION, nil
			}))))

	gpCtx.AppendProcessor(processor.NewMeteredProcessor(processor.NewStreamSelectKeyProcessor(
		"auctionsBySellerIDMap", processor.SelectKeyFunc(
			func(key, value interface{}) (interface{}, error) {
				event := value.(*ntypes.Event)
				return event.NewAuction.Seller, nil
			}))))

	return func(ctx context.Context, argsTmp interface{}, wg *sync.WaitGroup, msgChan chan commtypes.Message, errChan chan error) {
		args := argsTmp.(*TwoMsgChanProcArgs)
		gpCtx.AppendProcessor(processor.NewGroupByOutputProcessor(args.Producers()[0], args))
		defer wg.Done()
		gpCtx.GeneralProc(ctx, args.Producers()[0], msgChan, errChan)
	}
}
