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
	"sharedlog-stream/pkg/proc_interface"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/source_sink"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/store_with_changelog"
	"sharedlog-stream/pkg/transaction"
	"sharedlog-stream/pkg/transaction/tran_interface"
	"time"

	"cs.utexas.edu/zjia/faas/types"
)

type q4Avg struct {
	env types.Environment

	funcName string
}

func NewQ4Avg(env types.Environment, funcName string) *q4Avg {
	return &q4Avg{
		env:      env,
		funcName: funcName,
	}
}

func (h *q4Avg) getSrcSink(ctx context.Context, sp *common.QueryInput,
	inputStream *sharedlog_stream.ShardedSharedLogStream,
	outputStream *sharedlog_stream.ShardedSharedLogStream,
) (*source_sink.MeteredSource, *source_sink.MeteredSyncSink, commtypes.KVMsgSerdes, error) {
	msgSerde, err := commtypes.GetMsgSerde(sp.SerdeFormat)
	if err != nil {
		return nil, nil, commtypes.KVMsgSerdes{}, fmt.Errorf("get msg serde err: %v", err)
	}
	kvmsgSerdes := commtypes.KVMsgSerdes{
		KeySerde: commtypes.Uint64Serde{},
		ValSerde: commtypes.Int64Serde{},
		MsgSerde: msgSerde,
	}
	inputConfig := &source_sink.StreamSourceConfig{
		Timeout:     common.SrcConsumeTimeout,
		KVMsgSerdes: kvmsgSerdes,
	}
	outputConfig := &source_sink.StreamSinkConfig{
		KVMsgSerdes: commtypes.KVMsgSerdes{
			KeySerde: commtypes.Uint64Serde{},
			ValSerde: commtypes.Float64Serde{},
			MsgSerde: msgSerde,
		},
		FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
	}
	warmup := time.Duration(sp.WarmupS) * time.Second
	src := source_sink.NewMeteredSource(
		source_sink.NewShardedSharedLogStreamSource(inputStream, inputConfig), warmup)
	sink := source_sink.NewMeteredSyncSink(
		source_sink.NewShardedSharedLogStreamSyncSink(outputStream, outputConfig), warmup)
	return src, sink, kvmsgSerdes, nil
}

func (h *q4Avg) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &common.QueryInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output := h.Q4Avg(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	return utils.CompressData(encodedOutput), nil
}

type Q4AvgProcessArgs struct {
	sumCount     *processor.MeteredProcessor
	calcAvg      *processor.MeteredProcessor
	trackParFunc tran_interface.TrackKeySubStreamFunc
	proc_interface.BaseProcArgsWithSrcSink
}

func (h *q4Avg) procMsg(ctx context.Context, msg commtypes.Message, argsTmp interface{}) error {
	args := argsTmp.(*Q4AvgProcessArgs)
	val := msg.Value.(commtypes.EventTimeExtractor)
	ts, err := val.ExtractEventTime()
	if err != nil {
		return err
	}
	msg.Timestamp = ts
	sumCounts, err := args.sumCount.ProcessAndReturn(ctx, msg)
	if err != nil {
		return err
	}
	avg, err := args.sumCount.ProcessAndReturn(ctx, sumCounts[0])
	if err != nil {
		return err
	}
	err = args.Sinks()[0].Produce(ctx, avg[0], args.ParNum(), false)
	if err != nil {
		return err
	}
	return nil
}

func (h *q4Avg) Q4Avg(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	input_stream, output_streams, err := benchutil.GetShardedInputOutputStreams(ctx, h.env, sp)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	src, sink, inKVMsgSerdes, err := h.getSrcSink(ctx, sp, input_stream, output_streams[0])
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	sumCountStoreName := "q4SumCountKVStore"
	warmup := time.Duration(sp.WarmupS) * time.Second
	mp, err := store_with_changelog.NewMaterializeParamBuilder().
		KVMsgSerdes(inKVMsgSerdes).
		StoreName(sumCountStoreName).
		ParNum(sp.ParNum).
		SerdeFormat(commtypes.SerdeFormat(sp.SerdeFormat)).
		StreamParam(commtypes.CreateStreamParam{
			Env:          h.env,
			NumPartition: sp.NumInPartition,
		}).Build()
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	kvstore := store_with_changelog.CreateInMemKVTableWithChangelog(mp, store.Uint64KeyKVStoreCompare, warmup)
	sumCount := processor.NewMeteredProcessor(processor.NewStreamAggregateProcessor(
		kvstore,
		processor.InitializerFunc(func() interface{} {
			return &ntypes.SumAndCount{
				Sum:   0,
				Count: 0,
			}
		}),
		processor.AggregatorFunc(func(key, value, aggregate interface{}) interface{} {
			val := value.(uint64)
			agg := aggregate.(*ntypes.SumAndCount)
			return &ntypes.SumAndCount{
				Sum:   agg.Sum + val,
				Count: agg.Count + 1,
			}
		}),
	), warmup)
	calcAvg := processor.NewMeteredProcessor(processor.NewStreamMapValuesProcessor(
		processor.ValueMapperFunc(func(value interface{}) (interface{}, error) {
			val := value.(*ntypes.SumAndCount)
			return float64(val.Sum) / float64(val.Count), nil
		}),
	), warmup)
	sinks_arr := []source_sink.Sink{sink}
	procArgs := &Q4AvgProcessArgs{
		sumCount: sumCount,
		calcAvg:  calcAvg,
		BaseProcArgsWithSrcSink: proc_interface.NewBaseProcArgsWithSrcSink(src, sinks_arr,
			h.funcName, sp.ScaleEpoch, sp.ParNum),
	}
	task := transaction.StreamTask{
		ProcessFunc: func(ctx context.Context, task *transaction.StreamTask, argsTmp interface{}) *common.FnOutput {
			args := argsTmp.(proc_interface.ProcArgsWithSrcSink)
			return execution.CommonProcess(ctx, task, args, h.procMsg)
		},
		CurrentOffset:             make(map[string]uint64),
		CommitEveryForAtLeastOnce: common.CommitDuration,
		PauseFunc:                 nil,
		ResumeFunc:                nil,
		InitFunc: func(progArgs interface{}) {
			src.StartWarmup()
			sink.StartWarmup()
			sumCount.StartWarmup()
			calcAvg.StartWarmup()
		},
	}
	srcs := []source_sink.Source{src}
	var kvc []*transaction.KVStoreChangelog
	kvc = []*transaction.KVStoreChangelog{
		transaction.NewKVStoreChangelog(kvstore, mp.ChangelogManager(), mp.KVMsgSerdes(), sp.ParNum),
	}
	update_stats := func(ret *common.FnOutput) {
		ret.Latencies["sumCount"] = sumCount.GetLatency()
		ret.Latencies["calcAvg"] = calcAvg.GetLatency()
		ret.Latencies["eventTimeLatency"] = sink.GetEventTimeLatency()
		ret.Counts["src"] = src.GetCount()
		ret.Counts["sink"] = sink.GetCount()
	}

	if sp.EnableTransaction {
		transactionalID := fmt.Sprintf("%s-%s-%d-%s", h.funcName, sp.InputTopicNames[0],
			sp.ParNum, sp.OutputTopicNames[0])
		streamTaskArgs := transaction.NewStreamTaskArgsTransaction(h.env, transactionalID,
			procArgs, srcs, sinks_arr).WithKVChangelogs(kvc).WithFixedOutParNum(sp.ParNum)
		benchutil.UpdateStreamTaskArgsTransaction(sp, streamTaskArgs)
		ret := transaction.SetupManagersAndProcessTransactional(ctx, h.env, streamTaskArgs, &task)
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
