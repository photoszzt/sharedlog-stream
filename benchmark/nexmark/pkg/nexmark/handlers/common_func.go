package handlers

import (
	"context"
	"fmt"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"
	"time"

	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/proc_interface"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/source_sink"
	"sharedlog-stream/pkg/stream_task"

	"cs.utexas.edu/zjia/faas/types"
)

func only_bid(msg *commtypes.Message) (bool, error) {
	event := msg.Value.(*ntypes.Event)
	return event.Etype == ntypes.BID, nil
}

func getSrcSink(ctx context.Context, sp *common.QueryInput,
	input_stream *sharedlog_stream.ShardedSharedLogStream,
	output_stream *sharedlog_stream.ShardedSharedLogStream,
) (*source_sink.MeteredSource, *source_sink.MeteredSyncSink, error) {
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	msgSerde, err := commtypes.GetMsgSerde(serdeFormat)
	if err != nil {
		return nil, nil, fmt.Errorf("get msg serde failed: %v", err)
	}
	eventSerde, err := ntypes.GetEventSerde(serdeFormat)
	if err != nil {
		return nil, nil, err
	}
	inConfig := &source_sink.StreamSourceConfig{
		Timeout: common.SrcConsumeTimeout,
		KVMsgSerdes: commtypes.KVMsgSerdes{
			KeySerde: commtypes.StringSerde{},
			ValSerde: eventSerde,
			MsgSerde: msgSerde,
		},
	}
	outConfig := &source_sink.StreamSinkConfig{
		KVMsgSerdes: commtypes.KVMsgSerdes{
			KeySerde: commtypes.StringSerde{},
			ValSerde: eventSerde,
			MsgSerde: msgSerde,
		},
		FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
	}
	src := source_sink.NewMeteredSource(source_sink.NewShardedSharedLogStreamSource(input_stream, inConfig), time.Duration(sp.WarmupS)*time.Second)
	sink := source_sink.NewMeteredSyncSink(source_sink.NewShardedSharedLogStreamSyncSink(output_stream, outConfig), time.Duration(sp.WarmupS)*time.Second)
	return src, sink, nil
}

func getSrcSinkUint64Key(
	ctx context.Context,
	sp *common.QueryInput,
	input_stream *sharedlog_stream.ShardedSharedLogStream,
	output_stream *sharedlog_stream.ShardedSharedLogStream,
) (*source_sink.MeteredSource, *source_sink.MeteredSyncSink, error) {
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	msgSerde, err := commtypes.GetMsgSerde(serdeFormat)
	if err != nil {
		return nil, nil, err
	}
	eventSerde, err := ntypes.GetEventSerde(serdeFormat)
	if err != nil {
		return nil, nil, err
	}
	inConfig := &source_sink.StreamSourceConfig{
		Timeout: common.SrcConsumeTimeout,
		KVMsgSerdes: commtypes.KVMsgSerdes{
			KeySerde: commtypes.StringSerde{},
			ValSerde: eventSerde,
			MsgSerde: msgSerde,
		},
	}
	outConfig := &source_sink.StreamSinkConfig{
		KVMsgSerdes: commtypes.KVMsgSerdes{
			MsgSerde: msgSerde,
			ValSerde: eventSerde,
			KeySerde: commtypes.Uint64Serde{},
		},
		FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
	}

	src := source_sink.NewMeteredSource(source_sink.NewShardedSharedLogStreamSource(input_stream, inConfig),
		time.Duration(sp.WarmupS)*time.Second)
	sink := source_sink.NewMeteredSyncSink(source_sink.NewShardedSharedLogStreamSyncSink(output_stream, outConfig),
		time.Duration(sp.WarmupS)*time.Second)
	return src, sink, nil
}

/*
func CommonGetSrcSink(ctx context.Context, sp *common.QueryInput,
	input_stream *sharedlog_stream.ShardedSharedLogStream,
	output_stream *sharedlog_stream.ShardedSharedLogStream,
) (*source_sink.MeteredSource, *source_sink.MeteredSink, error) {
	msgSerde, err := commtypes.GetMsgSerde(sp.SerdeFormat)
	if err != nil {
		return nil, nil, fmt.Errorf("get msg serde err: %v", err)
	}
	eventSerde, err := getEventSerde(sp.SerdeFormat)
	if err != nil {
		return nil, nil, fmt.Errorf("get event serde err: %v", err)
	}
	inConfig := &source_sink.StreamSourceConfig{
		Timeout: common.SrcConsumeTimeout,
		KVMsgSerdes: commtypes.KVMsgSerdes{
			KeySerde: commtypes.StringSerde{},
			ValSerde: eventSerde,
			MsgSerde: msgSerde,
		},
	}
	outConfig := &source_sink.StreamSinkConfig{
		KVMsgSerdes: commtypes.KVMsgSerdes{
			KeySerde: commtypes.Uint64Serde{},
			ValSerde: eventSerde,
			MsgSerde: msgSerde,
		},
		FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
	}
	// fmt.Fprintf(os.Stderr, "output to %v\n", output_stream.TopicName())
	src := source_sink.NewMeteredSource(source_sink.NewShardedSharedLogStreamSource(input_stream, inConfig),
		time.Duration(sp.WarmupS)*time.Second)
	sink := source_sink.NewMeteredSink(source_sink.NewShardedSharedLogStreamSink(output_stream, outConfig),
		time.Duration(sp.WarmupS)*time.Second)
	return src, sink, nil
}
*/

func ExecuteApp(
	ctx context.Context,
	env types.Environment,
	transactionalID string,
	sp *common.QueryInput,
	task *stream_task.StreamTask,
	srcs []source_sink.Source,
	sinks_arr []source_sink.Sink,
	procArgs proc_interface.ProcArgs,
	update_stats func(ret *common.FnOutput),
) *common.FnOutput {
	if sp.EnableTransaction {
		streamTaskArgs := benchutil.UpdateStreamTaskArgsTransaction(sp,
			stream_task.NewStreamTaskArgsTransactionBuilder().
				ProcArgs(procArgs).
				Env(env).
				Srcs(srcs).
				Sinks(sinks_arr).
				TransactionalID(transactionalID)).
			Build()
		ret := stream_task.SetupManagersAndProcessTransactional(ctx, env, streamTaskArgs, task)
		if ret != nil && ret.Success {
			update_stats(ret)
		}
		return ret
	} else {
		streamTaskArgs := stream_task.NewStreamTaskArgs(env, procArgs, srcs, sinks_arr)
		benchutil.UpdateStreamTaskArgs(sp, streamTaskArgs)
		ret := task.Process(ctx, streamTaskArgs)
		if ret != nil && ret.Success {
			update_stats(ret)
		}
		return ret
	}
}

func ExecuteAppNoTransaction(
	ctx context.Context,
	env types.Environment,
	sp *common.QueryInput,
	task *stream_task.StreamTask,
	srcs []source_sink.Source,
	sinks_arr []source_sink.Sink,
	procArgs proc_interface.ProcArgs,
	update_stats func(ret *common.FnOutput),
) *common.FnOutput {
	streamTaskArgs := stream_task.NewStreamTaskArgs(env, procArgs, srcs, sinks_arr)
	benchutil.UpdateStreamTaskArgs(sp, streamTaskArgs)
	ret := task.Process(ctx, streamTaskArgs)
	if ret != nil && ret.Success {
		update_stats(ret)
	}
	return ret
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
