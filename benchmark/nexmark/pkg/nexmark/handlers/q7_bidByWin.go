package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"
	nutils "sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/control_channel"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/execution"
	"sharedlog-stream/pkg/hash"
	"sharedlog-stream/pkg/proc_interface"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/source_sink"
	"sharedlog-stream/pkg/transaction"
	"sharedlog-stream/pkg/utils"
	"sync"
	"time"

	"cs.utexas.edu/zjia/faas/types"
	"golang.org/x/xerrors"
)

type q7BidByWin struct {
	env      types.Environment
	cHashMu  sync.RWMutex
	cHash    *hash.ConsistentHash
	funcName string
}

func NewQ7BidByWin(env types.Environment, funcName string) types.FuncHandler {
	return &q7BidByWin{
		env:      env,
		cHash:    hash.NewConsistentHash(),
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
	return nutils.CompressData(encodedOutput), nil
}

type q7BidByWinProcessArgs struct {
	bid      *processor.MeteredProcessor
	bidByWin *processor.MeteredProcessor
	proc_interface.BaseProcArgsWithSrcSink
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
		key := mappedKey[0].Key.(uint64)
		h.cHashMu.RLock()
		parTmp, ok := h.cHash.Get(key)
		h.cHashMu.RUnlock()
		if !ok {
			return xerrors.New("fail to get output substream number")
		}
		par := parTmp.(uint8)
		err = args.TrackParFunc()(ctx, key, args.Sinks()[0].KeySerde(), args.Sinks()[0].TopicName(), par)
		if err != nil {
			return fmt.Errorf("track par err: %v", err)
		}
		err = args.Sinks()[0].Produce(ctx, mappedKey[0], par, false)
		if err != nil {
			return fmt.Errorf("sink err: %v", err)
		}
	}
	return nil
}

func (h *q7BidByWin) getSrcSink(
	ctx context.Context,
	sp *common.QueryInput,
	input_stream *sharedlog_stream.ShardedSharedLogStream,
	output_stream *sharedlog_stream.ShardedSharedLogStream,
) (*source_sink.MeteredSource, *source_sink.MeteredSyncSink, error) {
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	eventSerde, err := getEventSerde(serdeFormat)
	if err != nil {
		return nil, nil, err
	}
	msgSerde, err := commtypes.GetMsgSerde(serdeFormat)
	if err != nil {
		return nil, nil, err
	}
	seSerde, err := ntypes.GetStartEndTimeSerde(serdeFormat)
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
			KeySerde: seSerde,
		},
		FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
	}
	src := source_sink.NewMeteredSource(source_sink.NewShardedSharedLogStreamSource(input_stream, inConfig), time.Duration(sp.WarmupS)*time.Second)
	sink := source_sink.NewMeteredSyncSink(source_sink.NewShardedSharedLogStreamSyncSink(output_stream, outConfig), time.Duration(sp.WarmupS)*time.Second)
	src.SetInitialSource(true)
	return src, sink, nil
}

func (h *q7BidByWin) q7BidByWin(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	input_stream, output_streams, err := benchutil.GetShardedInputOutputStreams(ctx, h.env, sp)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	debug.Assert(len(output_streams) == 1, "expected only one output stream")
	src, sink, err := h.getSrcSink(ctx, sp, input_stream, output_streams[0])
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	warmup := time.Duration(sp.WarmupS) * time.Second
	tw, err := processor.NewTimeWindowsWithGrace(time.Duration(10)*time.Second, time.Duration(2)*time.Second)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	bid := processor.NewMeteredProcessor(processor.NewStreamFilterProcessor(processor.PredicateFunc(func(msg *commtypes.Message) (bool, error) {
		event := msg.Value.(*ntypes.Event)
		return event.Etype == ntypes.BID, nil
	})), warmup)
	bidByWin := processor.NewMeteredProcessor(processor.NewStreamMapProcessor(processor.MapperFunc(func(msg commtypes.Message) (commtypes.Message, error) {
		event := msg.Value.(*ntypes.Event)
		ts := event.Bid.DateTime
		windowStart := utils.MaxInt64(0, ts-tw.SizeMs+tw.AdvanceMs) / tw.AdvanceMs * tw.AdvanceMs
		wEnd := windowStart + tw.SizeMs
		debug.Assert(windowStart >= 0, "window start should be >= 0")
		debug.Assert(wEnd > 0, "window end should be > 0")
		win := ntypes.StartEndTime{StartTimeMs: windowStart, EndTimeMs: wEnd}
		return commtypes.Message{Key: win, Value: msg.Value, Timestamp: msg.Timestamp}, nil
	})), warmup)

	procArgs := &q7BidByWinProcessArgs{
		bid:      bid,
		bidByWin: bidByWin,
		BaseProcArgsWithSrcSink: proc_interface.NewBaseProcArgsWithSrcSink(src, []source_sink.Sink{sink},
			h.funcName, sp.ScaleEpoch, sp.ParNum),
	}
	task := transaction.NewStreamTaskBuilder().
		AppProcessFunc(func(ctx context.Context, task *transaction.StreamTask, argsTmp interface{}) *common.FnOutput {
			args := argsTmp.(proc_interface.ProcArgsWithSrcSink)
			return execution.CommonProcess(ctx, task, args, h.procMsg)
		}).
		InitFunc(func(progArgs interface{}) {
			bid.StartWarmup()
			bidByWin.StartWarmup()
			src.StartWarmup()
			sink.StartWarmup()
		}).Build()
	control_channel.SetupConsistentHash(&h.cHashMu, h.cHash, sp.NumOutPartitions[0])

	srcs := []source_sink.Source{src}
	sinks_arr := []source_sink.Sink{sink}

	update_stats := func(ret *common.FnOutput) {
		ret.Latencies["bid"] = bid.GetLatency()
		ret.Latencies["bidKeyedByPrice"] = bidByWin.GetLatency()
		ret.Counts["src"] = src.GetCount()
		ret.Counts["sink"] = sink.GetCount()
	}
	transactionalID := fmt.Sprintf("%s-%s-%d-%s", h.funcName, sp.InputTopicNames[0], sp.ParNum, sp.OutputTopicNames[0])
	return ExecuteApp(ctx, h.env, transactionalID, sp, task, srcs, sinks_arr, procArgs, update_stats)
}
