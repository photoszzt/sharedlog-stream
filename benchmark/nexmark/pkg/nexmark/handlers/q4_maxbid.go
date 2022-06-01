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

type q4MaxBid struct {
	env types.Environment

	cHashMu sync.RWMutex
	cHash   *hash.ConsistentHash

	funcName string
}

func NewQ4MaxBid(env types.Environment, funcName string) *q4MaxBid {
	return &q4MaxBid{
		env:      env,
		funcName: funcName,
		cHash:    hash.NewConsistentHash(),
	}
}

func (h *q4MaxBid) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &common.QueryInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output := h.Q4MaxBid(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	return utils.CompressData(encodedOutput), nil
}

func (h *q4MaxBid) getSrcSink(ctx context.Context, sp *common.QueryInput,
	inputStream *sharedlog_stream.ShardedSharedLogStream,
	outputStream *sharedlog_stream.ShardedSharedLogStream,
) (*source_sink.MeteredSource, *source_sink.MeteredSyncSink, commtypes.KVMsgSerdes, error) {
	var abSerde commtypes.Serde
	var aicSerde commtypes.Serde
	if sp.SerdeFormat == uint8(commtypes.JSON) {
		abSerde = ntypes.AuctionBidJSONSerde{}
		aicSerde = ntypes.AuctionIdCategoryJSONSerde{}
	} else {
		abSerde = ntypes.AuctionBidMsgpSerde{}
		aicSerde = ntypes.AuctionIdCategoryMsgpSerde{}
	}
	msgSerde, err := commtypes.GetMsgSerde(sp.SerdeFormat)
	if err != nil {
		return nil, nil, commtypes.KVMsgSerdes{}, fmt.Errorf("get msg serde err: %v", err)
	}
	kvmsgSerdes := commtypes.KVMsgSerdes{
		KeySerde: aicSerde,
		ValSerde: abSerde,
		MsgSerde: msgSerde,
	}
	inputConfig := &source_sink.StreamSourceConfig{
		Timeout:     common.SrcConsumeTimeout,
		KVMsgSerdes: kvmsgSerdes,
	}
	outConfig := &source_sink.StreamSinkConfig{
		KVMsgSerdes: commtypes.KVMsgSerdes{
			KeySerde: commtypes.Uint64Serde{},
			ValSerde: commtypes.Uint64Serde{},
			MsgSerde: msgSerde,
		},
		FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
	}
	warmup := time.Duration(sp.WarmupS) * time.Second
	src := source_sink.NewMeteredSource(
		source_sink.NewShardedSharedLogStreamSource(inputStream, inputConfig),
		warmup)
	sink := source_sink.NewMeteredSyncSink(
		source_sink.NewShardedSharedLogStreamSyncSink(outputStream, outConfig),
		warmup)
	src.SetInitialSource(false)
	return src, sink, kvmsgSerdes, nil
}

type q4MaxBidProcessArgs struct {
	maxBid  *processor.MeteredProcessor
	groupBy *processor.MeteredProcessor
	proc_interface.BaseProcArgsWithSrcSink
}

func (h *q4MaxBid) procMsg(ctx context.Context, msg commtypes.Message, argsTmp interface{}) error {
	args := argsTmp.(*q4MaxBidProcessArgs)
	ab := msg.Value.(commtypes.EventTimeExtractor)
	ts, err := ab.ExtractEventTime()
	if err != nil {
		return err
	}
	msg.Timestamp = ts
	aggs, err := args.maxBid.ProcessAndReturn(ctx, msg)
	if err != nil {
		return err
	}
	remapped, err := args.groupBy.ProcessAndReturn(ctx, aggs[0])
	if err != nil {
		return err
	}
	sink := args.Sinks()[0]
	for _, msg := range remapped {
		k := msg.Key.(uint64)
		h.cHashMu.RLock()
		parTmp, ok := h.cHash.Get(k)
		h.cHashMu.RUnlock()
		if !ok {
			return fmt.Errorf("fail to get output partition")
		}
		par := parTmp.(uint8)
		err = args.TrackParFunc()(ctx, k, sink.KeySerde(), sink.TopicName(), par)
		if err != nil {
			return err
		}
		err = sink.Produce(ctx, msg, par, false)
		if err != nil {
			return fmt.Errorf("sink err: %v", err)
		}
	}
	return nil
}

func (h *q4MaxBid) Q4MaxBid(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	input_stream, output_streams, err := benchutil.GetShardedInputOutputStreams(ctx, h.env, sp)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	src, sink, inKVMsgSerdes, err := h.getSrcSink(ctx, sp, input_stream, output_streams[0])
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	maxBidStoreName := "q4MaxBidKVStore"
	warmup := time.Duration(sp.WarmupS) * time.Second
	mp, err := store_with_changelog.NewMaterializeParamBuilder().
		KVMsgSerdes(inKVMsgSerdes).
		StoreName(maxBidStoreName).
		ParNum(sp.ParNum).
		SerdeFormat(commtypes.SerdeFormat(sp.SerdeFormat)).
		StreamParam(commtypes.CreateStreamParam{
			Env:          h.env,
			NumPartition: sp.NumInPartition,
		}).Build()
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	compare := func(a, b treemap.Key) int {
		ka := a.(*ntypes.AuctionIdCategory)
		kb := b.(*ntypes.AuctionIdCategory)
		return ntypes.CompareAuctionIdCategory(ka, kb)
	}
	kvstore := store_with_changelog.CreateInMemKVTableWithChangelog(mp, compare, warmup)
	maxBid := processor.NewMeteredProcessor(processor.NewStreamAggregateProcessor(kvstore,
		processor.InitializerFunc(func() interface{} { return uint64(0) }),
		processor.AggregatorFunc(func(key, value, aggregate interface{}) interface{} {
			v := value.(*ntypes.AuctionBid)
			agg := aggregate.(uint64)
			if v.BidPrice > agg {
				return v.BidPrice
			} else {
				return agg
			}
		})), warmup)
	groupBy := processor.NewMeteredProcessor(processor.NewStreamMapProcessor(
		processor.MapperFunc(func(m commtypes.Message) (commtypes.Message, error) {
			return commtypes.Message{
				Key:   m.Key.(*ntypes.AuctionIdCategory).Category,
				Value: m.Value,
			}, nil
		})), warmup)
	sinks_arr := []source_sink.Sink{sink}
	procArgs := &q4MaxBidProcessArgs{
		maxBid:  maxBid,
		groupBy: groupBy,
		BaseProcArgsWithSrcSink: proc_interface.NewBaseProcArgsWithSrcSink(src, sinks_arr, h.funcName,
			sp.ScaleEpoch, sp.ParNum),
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
			groupBy.StartWarmup()
		}).Build()

	srcs := []source_sink.Source{src}
	var kvc []*transaction.KVStoreChangelog
	kvc = []*transaction.KVStoreChangelog{
		transaction.NewKVStoreChangelog(kvstore, mp.ChangelogManager(), mp.KVMsgSerdes(), sp.ParNum),
	}

	update_stats := func(ret *common.FnOutput) {
		ret.Latencies["maxBid"] = maxBid.GetLatency()
		ret.Latencies["groupBy"] = groupBy.GetLatency()
		ret.Latencies["eventTimeLatency"] = sink.GetEventTimeLatency()
		ret.Counts["src"] = src.GetCount()
		ret.Counts["sink"] = sink.GetCount()
	}

	if sp.EnableTransaction {
		transactionalID := fmt.Sprintf("%s-%s-%d-%s", h.funcName, sp.InputTopicNames[0],
			sp.ParNum, sp.OutputTopicNames[0])
		streamTaskArgs := transaction.NewStreamTaskArgsTransaction(h.env, transactionalID,
			procArgs, srcs, sinks_arr).WithKVChangelogs(kvc)
		benchutil.UpdateStreamTaskArgsTransaction(sp, streamTaskArgs)
		ret := transaction.SetupManagersAndProcessTransactional(ctx, h.env, streamTaskArgs, task)
		if ret != nil && ret.Success {
			update_stats(ret)
		}
		return ret
	} else {
		streamTaskArgs := transaction.NewStreamTaskArgs(h.env, procArgs, srcs, sinks_arr).WithKVChangelogs(kvc)
		benchutil.UpdateStreamTaskArgs(sp, streamTaskArgs)
		ret := task.Process(ctx, streamTaskArgs)
		if ret != nil && ret.Success {
			update_stats(ret)
		}
		return ret
	}
}
