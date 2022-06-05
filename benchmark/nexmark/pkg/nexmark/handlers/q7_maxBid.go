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
	"sharedlog-stream/pkg/store_with_changelog"
	"sharedlog-stream/pkg/transaction"
	"sharedlog-stream/pkg/treemap"
	"sync"
	"time"

	"cs.utexas.edu/zjia/faas/types"
)

type q7MaxBid struct {
	env      types.Environment
	cHashMu  sync.RWMutex
	cHash    *hash.ConsistentHash
	funcName string
}

func NewQ7MaxBid(env types.Environment, funcName string) types.FuncHandler {
	return &q7MaxBid{
		env:      env,
		cHash:    hash.NewConsistentHash(),
		funcName: funcName,
	}
}

func (h *q7MaxBid) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &common.QueryInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output := h.q7MaxBidByPrice(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	return nutils.CompressData(encodedOutput), nil
}

type q7MaxBidByPriceProcessArgs struct {
	maxBid  *processor.MeteredProcessor
	remapKV *processor.MeteredProcessor
	proc_interface.BaseProcArgsWithSrcSink
}

func (h *q7MaxBid) procMsg(ctx context.Context, msg commtypes.Message, argsTmp interface{}) error {
	return nil
}

func (h *q7MaxBid) getSrcSink(
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
	seSerde, err := ntypes.GetStartEndTimeSerde(serdeFormat)
	if err != nil {
		return nil, nil, err
	}
	inKVMsgSerdes := commtypes.KVMsgSerdes{
		MsgSerde: msgSerde,
		ValSerde: eventSerde,
		KeySerde: seSerde,
	}
	inConfig := &source_sink.StreamSourceConfig{
		KVMsgSerdes: inKVMsgSerdes,
		Timeout:     common.SrcConsumeTimeout,
	}
	outConfig := &source_sink.StreamSinkConfig{
		KVMsgSerdes: commtypes.KVMsgSerdes{
			MsgSerde: msgSerde,
			KeySerde: commtypes.Uint64Serde{},
			ValSerde: seSerde,
		},
		FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
	}
	warmup := time.Duration(sp.WarmupS) * time.Second
	src := source_sink.NewMeteredSource(
		source_sink.NewShardedSharedLogStreamSource(input_stream, inConfig),
		warmup)
	sink := source_sink.NewMeteredSyncSink(
		source_sink.NewShardedSharedLogStreamSyncSink(output_stream, outConfig),
		warmup)
	src.SetInitialSource(false)
	return src, sink, nil
}

func (h *q7MaxBid) q7MaxBidByPrice(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
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
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)

	seSerde, err := ntypes.GetStartEndTimeSerde(serdeFormat)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	msgSerde, err := commtypes.GetMsgSerde(serdeFormat)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	vtSerde, err := commtypes.GetValueTsSerde(serdeFormat, commtypes.Uint64Serde{}, commtypes.Uint64Serde{})
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}

	maxBidByWinStoreName := "maxBidByWinKVStore"
	mp, err := store_with_changelog.NewMaterializeParamBuilder().
		KVMsgSerdes(commtypes.KVMsgSerdes{
			KeySerde: seSerde,
			ValSerde: vtSerde,
			MsgSerde: msgSerde,
		}).
		StoreName(maxBidByWinStoreName).
		ParNum(sp.ParNum).
		SerdeFormat(serdeFormat).
		StreamParam(commtypes.CreateStreamParam{
			Env:          h.env,
			NumPartition: sp.NumInPartition,
		}).Build()
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	compare := func(a, b treemap.Key) int {
		ka := a.(*ntypes.StartEndTime)
		kb := b.(*ntypes.StartEndTime)
		return ntypes.CompareStartEndTime(ka, kb)
	}
	kvstore := store_with_changelog.CreateInMemKVTableWithChangelog(mp, compare, warmup)
	maxBid := processor.NewMeteredProcessor(processor.NewStreamAggregateProcessor(kvstore,
		processor.InitializerFunc(func() interface{} {
			return uint64(0)
		}),
		processor.AggregatorFunc(func(key, value, aggregate interface{}) interface{} {
			v := value.(*ntypes.Event)
			agg := aggregate.(uint64)
			if v.Bid.Price > agg {
				return v.Bid.Price
			}
			return agg
		})), warmup)

	remapKV := processor.NewMeteredProcessor(processor.NewStreamMapProcessor(
		processor.MapperFunc(func(m commtypes.Message) (commtypes.Message, error) {
			return commtypes.Message{Key: m.Value, Value: m.Key, Timestamp: m.Timestamp}, nil
		})), warmup)
	sinks_arr := []source_sink.Sink{sink}
	procArgs := &q7MaxBidByPriceProcessArgs{
		maxBid:  maxBid,
		remapKV: remapKV,
		BaseProcArgsWithSrcSink: proc_interface.NewBaseProcArgsWithSrcSink(
			src, sinks_arr, h.funcName, sp.ScaleEpoch, sp.ParNum,
		),
	}
	task := transaction.NewStreamTaskBuilder().
		AppProcessFunc(func(ctx context.Context, task *transaction.StreamTask, argsTmp interface{}) *common.FnOutput {
			args := argsTmp.(proc_interface.ProcArgsWithSrcSink)
			return execution.CommonProcess(ctx, task, args, h.procMsg)
		}).
		InitFunc(func(progArgs interface{}) {
			src.StartWarmup()
			sink.StartWarmup()
			maxBid.StartWarmup()
			remapKV.StartWarmup()
		}).Build()
	control_channel.SetupConsistentHash(&h.cHashMu, h.cHash, sp.NumOutPartitions[0])
	srcs := []source_sink.Source{src}

	var kvc []*transaction.KVStoreChangelog
	kvc = []*transaction.KVStoreChangelog{
		transaction.NewKVStoreChangelog(
			kvstore, kvstore.MaterializeParam().ChangelogManager(),
			kvstore.MaterializeParam().KVMsgSerdes(),
			kvstore.MaterializeParam().ParNum(),
		),
	}
	update_stats := func(ret *common.FnOutput) {
		ret.Latencies["maxBid"] = maxBid.GetLatency()
		ret.Latencies["remapKV"] = remapKV.GetLatency()
		ret.Counts["src"] = src.GetCount()
		ret.Counts["sink"] = sink.GetCount()
	}
	if sp.EnableTransaction {
		transactionalID := fmt.Sprintf("%s-%s-%d-%s", h.funcName,
			sp.InputTopicNames[0], sp.ParNum, sp.OutputTopicNames[0])
		streamTaskArgs := transaction.NewStreamTaskArgsTransaction(h.env, transactionalID, procArgs, srcs, sinks_arr).
			WithKVChangelogs(kvc)
		benchutil.UpdateStreamTaskArgsTransaction(sp, streamTaskArgs)
		ret := transaction.SetupManagersAndProcessTransactional(ctx, h.env, streamTaskArgs, task)
		if ret != nil && ret.Success {
			update_stats(ret)
		}
		return ret
	} else {
		streamTaskArgs := transaction.NewStreamTaskArgs(h.env, procArgs, srcs, sinks_arr).
			WithKVChangelogs(kvc)
		benchutil.UpdateStreamTaskArgs(sp, streamTaskArgs)
		ret := task.Process(ctx, streamTaskArgs)
		if ret != nil && ret.Success {
			update_stats(ret)
		}
		return ret
	}
}
