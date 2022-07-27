package handlers

import (
	"context"
	"fmt"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"
	"sync"
	"time"

	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/execution"
	"sharedlog-stream/pkg/proc_interface"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/producer_consumer"
	"sharedlog-stream/pkg/stats"
	"sharedlog-stream/pkg/stream_task"

	"cs.utexas.edu/zjia/faas/types"
)

func only_bid(key interface{}, value interface{}) (bool, error) {
	event := value.(*ntypes.Event)
	return event.Etype == ntypes.BID, nil
}

func getSrcSink(ctx context.Context, env types.Environment, sp *common.QueryInput,
) ([]producer_consumer.MeteredConsumerIntr, []producer_consumer.MeteredProducerIntr, error) {
	input_stream, output_streams, err := benchutil.GetShardedInputOutputStreams(ctx, env, sp)
	if err != nil {
		return nil, nil, err
	}
	debug.Assert(len(output_streams) == 1, "expected only one output stream")
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	eventSerde, err := ntypes.GetEventSerdeG(serdeFormat)
	if err != nil {
		return nil, nil, err
	}
	msgSerde, err := commtypes.GetMsgSerdeG[string](serdeFormat, commtypes.StringSerdeG{}, eventSerde)
	if err != nil {
		return nil, nil, fmt.Errorf("get msg serde failed: %v", err)
	}
	inConfig := &producer_consumer.StreamConsumerConfigG[string, *ntypes.Event]{
		Timeout:  common.SrcConsumeTimeout,
		MsgSerde: msgSerde,
	}
	outConfig := &producer_consumer.StreamSinkConfig[string, *ntypes.Event]{
		MsgSerde:      msgSerde,
		FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
	}
	warmup := time.Duration(sp.WarmupS) * time.Second
	src := producer_consumer.NewMeteredConsumer(producer_consumer.NewShardedSharedLogStreamConsumerG(input_stream, inConfig), warmup)
	sink := producer_consumer.NewMeteredProducer(producer_consumer.NewShardedSharedLogStreamProducer(output_streams[0], outConfig), warmup)
	return []producer_consumer.MeteredConsumerIntr{src}, []producer_consumer.MeteredProducerIntr{sink}, nil
}

func getSrcSinkUint64Key(
	ctx context.Context,
	env types.Environment,
	sp *common.QueryInput,
) ([]producer_consumer.MeteredConsumerIntr, []producer_consumer.MeteredProducerIntr, error) {
	input_stream, output_streams, err := benchutil.GetShardedInputOutputStreams(ctx, env, sp)
	if err != nil {
		return nil, nil, err
	}
	debug.Assert(len(output_streams) == 1, "expected only one output stream")
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	eventSerde, err := ntypes.GetEventSerdeG(serdeFormat)
	if err != nil {
		return nil, nil, err
	}
	inMsgSerde, err := commtypes.GetMsgSerdeG[string](serdeFormat, commtypes.StringSerdeG{}, eventSerde)
	if err != nil {
		return nil, nil, err
	}
	outMsgSerde, err := commtypes.GetMsgSerdeG[uint64](serdeFormat, commtypes.Uint64SerdeG{}, eventSerde)
	if err != nil {
		return nil, nil, err
	}
	warmup := time.Duration(sp.WarmupS) * time.Second
	inConfig := &producer_consumer.StreamConsumerConfigG[string, *ntypes.Event]{
		Timeout:  common.SrcConsumeTimeout,
		MsgSerde: inMsgSerde,
	}
	outConfig := &producer_consumer.StreamSinkConfig[uint64, *ntypes.Event]{
		MsgSerde:      outMsgSerde,
		FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
	}
	src := producer_consumer.NewMeteredConsumer(producer_consumer.NewShardedSharedLogStreamConsumerG(input_stream, inConfig), warmup)
	sink := producer_consumer.NewMeteredProducer(producer_consumer.NewShardedSharedLogStreamProducer(output_streams[0], outConfig), warmup)
	return []producer_consumer.MeteredConsumerIntr{src}, []producer_consumer.MeteredProducerIntr{sink}, nil
}

func GetSerdeFromString(serdeStr string, serdeFormat commtypes.SerdeFormat) (commtypes.Serde, error) {
	switch serdeStr {
	case "StartEndTime":
		return ntypes.GetStartEndTimeSerde(serdeFormat)
	case "AuctionIdCntMax":
		return ntypes.GetAuctionIdCntMaxSerde(serdeFormat)
	case "BidAndMax":
		return ntypes.GetBidAndMaxSerde(serdeFormat)
	case "Event":
		return ntypes.GetEventSerde(serdeFormat)
	case "Uint64":
		return commtypes.Uint64Serde{}, nil
	case "Float64":
		return commtypes.Float64Serde{}, nil
	case "String":
		return commtypes.StringSerde{}, nil
	case "PersonTime":
		return ntypes.GetPersonTimeSerde(serdeFormat)
	case "NameCityStateId":
		return ntypes.GetNameCityStateIdSerde(serdeFormat)
	default:
		return nil, fmt.Errorf("Unrecognized serde string %s", serdeStr)
	}
}

func PrepareProcessByTwoGeneralProc(
	ctx context.Context,
	func1 execution.GeneralProcFunc,
	func2 execution.GeneralProcFunc,
	ectx *processor.BaseExecutionContext,
	procMsg proc_interface.ProcessMsgFunc,
) *stream_task.StreamTask {
	var wg sync.WaitGroup
	func1Manager := execution.NewGeneralProcManager(func1)
	func2Manager := execution.NewGeneralProcManager(func2)
	handleErrFunc := func() error {
		select {
		case aucErr := <-func1Manager.ErrChan():
			return aucErr
		case bidErr := <-func2Manager.ErrChan():
			return bidErr
		default:
		}
		return nil
	}

	pauseTime := stats.NewInt64Collector("2proc_pause_us", stats.DEFAULT_COLLECT_DURATION)

	task := stream_task.NewStreamTaskBuilder().
		AppProcessFunc(func(ctx context.Context, task *stream_task.StreamTask, argsTmp interface{}) *common.FnOutput {
			args := argsTmp.(*processor.BaseExecutionContext)
			return execution.CommonProcess(ctx, task, args, func(ctx context.Context, msg commtypes.Message, argsTmp interface{}) error {
				func1Manager.MsgChan() <- msg
				func2Manager.MsgChan() <- msg
				return nil
			})
		}).
		InitFunc(func(task *stream_task.StreamTask) {
			func1Manager.LaunchProc(ctx, ectx, &wg)
			func2Manager.LaunchProc(ctx, ectx, &wg)
		}).
		PauseFunc(func() *common.FnOutput {
			// debug.Fprintf(os.Stderr, "begin pause\n")
			if err := handleErrFunc(); err != nil {
				return &common.FnOutput{Success: false, Message: err.Error()}
			}

			pStart := stats.TimerBegin()
			func1Manager.MsgChan() <- commtypes.Message{Key: commtypes.Punctuate{}}
			func2Manager.MsgChan() <- commtypes.Message{Key: commtypes.Punctuate{}}

			<-func1Manager.PauseChan()
			<-func2Manager.PauseChan()
			elapsed := stats.Elapsed(pStart)
			pauseTime.AddSample(elapsed.Microseconds())

			// sargs.LockProducer()
			// debug.Fprintf(os.Stderr, "done pause\n")
			return nil
		}).HandleErrFunc(handleErrFunc).Build()
	return task
}
