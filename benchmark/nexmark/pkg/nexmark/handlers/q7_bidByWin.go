package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/ntypes"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/optional"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/producer_consumer"
	"sharedlog-stream/pkg/stream_task"
	"sharedlog-stream/pkg/utils"
	"time"

	"cs.utexas.edu/zjia/faas/types"
)

type q7BidByWin struct {
	env      types.Environment
	funcName string
}

func NewQ7BidByWin(env types.Environment, funcName string) types.FuncHandler {
	return &q7BidByWin{
		env:      env,
		funcName: funcName,
	}
}

func (h *q7BidByWin) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &common.QueryInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output := h.q7BidByWin(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	return common.CompressData(encodedOutput), nil
}

/*
type q7BidByWinProcessArgs struct {
	bid      *processor.MeteredProcessor
	bidByWin *processor.MeteredProcessor
	groupBy  *processor.GroupBy
	processor.BaseExecutionContext
}

func (h *q7BidByWin) procMsg(ctx context.Context, msg commtypes.Message, argsTmp interface{}) error {
	args := argsTmp.(*q7BidByWinProcessArgs)
	bidMsg, err := args.bid.ProcessAndReturn(ctx, msg)
	if err != nil {
		return fmt.Errorf("filter bid err: %v", err)
	}
	if bidMsg != nil {
		mappedKey, err := args.bidByWin.ProcessAndReturn(ctx, bidMsg[0])
		if err != nil {
			return fmt.Errorf("bid keyed by price error: %v", err)
		}
		err = args.groupBy.GroupByAndProduce(ctx, mappedKey[0], args.TrackParFunc())
		if err != nil {
			return err
		}
	}
	return nil
}
*/

func (h *q7BidByWin) getSrcSink(ctx context.Context, sp *common.QueryInput,
) ([]*producer_consumer.MeteredConsumer, []producer_consumer.MeteredProducerIntr, error) {
	input_stream, output_streams, err := benchutil.GetShardedInputOutputStreams(ctx, h.env, sp)
	if err != nil {
		return nil, nil, err
	}
	debug.Assert(len(output_streams) == 1, "expected only one output stream")

	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	inConfig := &producer_consumer.StreamConsumerConfig{
		Timeout:     common.SrcConsumeTimeout,
		SerdeFormat: serdeFormat,
	}
	outConfig := &producer_consumer.StreamSinkConfig{
		FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
		Format:        serdeFormat,
	}
	warmup := time.Duration(sp.WarmupS) * time.Second
	consumer, err := producer_consumer.NewShardedSharedLogStreamConsumer(input_stream, inConfig, sp.NumSubstreamProducer[0], sp.ParNum)
	if err != nil {
		return nil, nil, err
	}
	src := producer_consumer.NewMeteredConsumer(consumer, warmup)
	sink, err := producer_consumer.NewMeteredProducer(producer_consumer.NewShardedSharedLogStreamProducer(output_streams[0], outConfig), warmup)
	if err != nil {
		return nil, nil, err
	}
	src.SetInitialSource(true)
	return []*producer_consumer.MeteredConsumer{src}, []producer_consumer.MeteredProducerIntr{sink}, nil
}

func (h *q7BidByWin) q7BidByWin(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	eventSerde, err := ntypes.GetEventSerdeG(serdeFormat)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	inMsgSerde, err := commtypes.GetMsgGSerdeG[string](serdeFormat, commtypes.StringSerdeG{}, eventSerde)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	seSerde, err := ntypes.GetStartEndTimeSerdeG(serdeFormat)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	outMsgSerde, err := commtypes.GetMsgGSerdeG(serdeFormat, seSerde, eventSerde)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	srcs, sinks_arr, err := h.getSrcSink(ctx, sp)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	ectx := processor.NewExecutionContext(srcs, sinks_arr,
		h.funcName, sp.ScaleEpoch, sp.ParNum)
	tw, err := commtypes.NewTimeWindowsWithGrace(time.Duration(10)*time.Second, time.Duration(2)*time.Second)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	filterProc := processor.NewStreamFilterProcessorG[string, *ntypes.Event]("filterBids",
		processor.PredicateFuncG[string, *ntypes.Event](
			func(key optional.Option[string], value optional.Option[*ntypes.Event]) (bool, error) {
				event := value.Unwrap()
				return event.Etype == ntypes.BID, nil
			}))
	selectProc := processor.NewStreamSelectKeyProcessorG[string, *ntypes.Event, ntypes.StartEndTime]("bidByWin",
		processor.SelectKeyFuncG[string, *ntypes.Event, ntypes.StartEndTime](
			func(key optional.Option[string], value optional.Option[*ntypes.Event]) (ntypes.StartEndTime, error) {
				event := value.Unwrap()
				ts := event.Bid.DateTime
				windowStart := utils.MaxInt64(0, ts-tw.SizeMs+tw.AdvanceMs) / tw.AdvanceMs * tw.AdvanceMs
				wEnd := windowStart + tw.SizeMs
				debug.Assert(windowStart >= 0, "window start should be >= 0")
				debug.Assert(wEnd > 0, "window end should be > 0")
				win := ntypes.StartEndTime{StartTimeMs: windowStart, EndTimeMs: wEnd}
				return win, nil
			}))
	outProc := processor.NewGroupByOutputProcessorG("bidByWinProc", sinks_arr[0], &ectx, outMsgSerde)
	filterProc.NextProcessor(selectProc)
	selectProc.NextProcessor(outProc)
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
	transactionalID := fmt.Sprintf("%s-%s-%d-%s", h.funcName, sp.InputTopicNames[0],
		sp.ParNum, sp.OutputTopicNames[0])
	streamTaskArgs := benchutil.UpdateStreamTaskArgs(sp,
		stream_task.NewStreamTaskArgsBuilder(h.env, &ectx, transactionalID)).Build()
	return stream_task.ExecuteApp(ctx, task, streamTaskArgs, stream_task.EmptySetupSnapshotCallback,
		func() { outProc.OutputRemainingStats() })
}
