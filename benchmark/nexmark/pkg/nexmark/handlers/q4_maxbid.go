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
	"sharedlog-stream/pkg/store_restore"
	"sharedlog-stream/pkg/store_with_changelog"
	"sharedlog-stream/pkg/stream_task"
	"sharedlog-stream/pkg/treemap"
	"time"

	"cs.utexas.edu/zjia/faas/types"
)

type q4MaxBid struct {
	env      types.Environment
	funcName string
}

func NewQ4MaxBid(env types.Environment, funcName string) *q4MaxBid {
	return &q4MaxBid{
		env:      env,
		funcName: funcName,
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
) ([]producer_consumer.MeteredConsumerIntr, []producer_consumer.MeteredProducerIntr, error) {
	inputStream, outputStreams, err := benchutil.GetShardedInputOutputStreams(ctx, h.env, sp)
	if err != nil {
		return nil, nil, err
	}
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	var abSerde commtypes.Serde
	var aicSerde commtypes.Serde
	if serdeFormat == commtypes.JSON {
		abSerde = ntypes.AuctionBidJSONSerde{}
		aicSerde = ntypes.AuctionIdCategoryJSONSerde{}
	} else if serdeFormat == commtypes.MSGP {
		abSerde = ntypes.AuctionBidMsgpSerde{}
		aicSerde = ntypes.AuctionIdCategoryMsgpSerde{}
	} else {
		return nil, nil, fmt.Errorf("unrecognized format: %v", serdeFormat)
	}
	msgSerde, err := commtypes.GetMsgSerde(serdeFormat)
	if err != nil {
		return nil, nil, fmt.Errorf("get msg serde err: %v", err)
	}
	inputConfig := &producer_consumer.StreamConsumerConfig{
		Timeout: common.SrcConsumeTimeout,
		KVMsgSerdes: commtypes.KVMsgSerdes{
			KeySerde: aicSerde,
			ValSerde: abSerde,
			MsgSerde: msgSerde,
		},
	}
	outConfig := &producer_consumer.StreamSinkConfig{
		KVMsgSerdes: commtypes.KVMsgSerdes{
			KeySerde: commtypes.Uint64Serde{},
			ValSerde: commtypes.Uint64Serde{},
			MsgSerde: msgSerde,
		},
		FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
	}
	warmup := time.Duration(sp.WarmupS) * time.Second
	src := producer_consumer.NewMeteredConsumer(
		producer_consumer.NewShardedSharedLogStreamConsumer(inputStream, inputConfig),
		warmup)
	sink := producer_consumer.NewMeteredProducer(
		producer_consumer.NewShardedSharedLogStreamProducer(outputStreams[0], outConfig),
		warmup)
	src.SetInitialSource(false)
	return []producer_consumer.MeteredConsumerIntr{src}, []producer_consumer.MeteredProducerIntr{sink}, nil
}

/*
type q4MaxBidProcessArgs struct {
	maxBid    *processor.MeteredProcessor
	changeKey *processor.MeteredProcessor
	groupBy   *processor.MeteredProcessor
	processor.BaseExecutionContext
}

func (h *q4MaxBid) procMsg(ctx context.Context, msg commtypes.Message, argsTmp interface{}) error {
	args := argsTmp.(*q4MaxBidProcessArgs)
	aggs, err := args.maxBid.ProcessAndReturn(ctx, msg)
	if err != nil {
		return err
	}
	remapped, err := args.changeKey.ProcessAndReturn(ctx, aggs[0])
	if err != nil {
		return err
	}
	_, err = args.groupBy.ProcessAndReturn(ctx, remapped[0])
	if err != nil {
		return err
	}
	return nil
}
*/

func (h *q4MaxBid) Q4MaxBid(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	srcs, sinks_arr, err := h.getSrcSink(ctx, sp)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	ectx := processor.NewExecutionContext(srcs, sinks_arr, h.funcName,
		sp.ScaleEpoch, sp.ParNum)
	maxBidStoreName := "q4MaxBidKVStore"
	warmup := time.Duration(sp.WarmupS) * time.Second
	mp, err := store_with_changelog.NewMaterializeParamBuilder().
		KVMsgSerdes(srcs[0].KVMsgSerdes()).
		StoreName(maxBidStoreName).
		ParNum(sp.ParNum).
		SerdeFormat(commtypes.SerdeFormat(sp.SerdeFormat)).
		StreamParam(commtypes.CreateStreamParam{
			Env:          h.env,
			NumPartition: sp.NumInPartition,
		}).Build(time.Duration(sp.FlushMs)*time.Millisecond, common.SrcConsumeTimeout)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	compare := func(a, b treemap.Key) int {
		ka := a.(*ntypes.AuctionIdCategory)
		kb := b.(*ntypes.AuctionIdCategory)
		return ntypes.CompareAuctionIdCategory(ka, kb)
	}
	kvstore := store_with_changelog.CreateInMemKVTableWithChangelog(mp, compare, warmup)
	ectx.Via(processor.NewMeteredProcessor(processor.NewStreamAggregateProcessor("maxBid", kvstore,
		processor.InitializerFunc(func() interface{} { return uint64(0) }),
		processor.AggregatorFunc(func(key, value, aggregate interface{}) interface{} {
			v := value.(*ntypes.AuctionBid)
			agg := aggregate.(uint64)
			if v.BidPrice > agg {
				return v.BidPrice
			} else {
				return agg
			}
		})), warmup)).
		Via(processor.NewMeteredProcessor(processor.NewStreamMapProcessor("changeKey",
			processor.MapperFunc(func(m commtypes.Message) (commtypes.Message, error) {
				return commtypes.Message{
					Key:   m.Key.(*ntypes.AuctionIdCategory).Category,
					Value: m.Value,
				}, nil
			})), warmup)).
		Via(processor.NewGroupByOutputProcessor(sinks_arr[0], &ectx))

	task := stream_task.NewStreamTaskBuilder().
		AppProcessFunc(func(ctx context.Context, task *stream_task.StreamTask, argsTmp interface{}) *common.FnOutput {
			args := argsTmp.(processor.ExecutionContext)
			return execution.CommonProcess(ctx, task, args, processor.ProcessMsg)
		}).Build()

	var kvc []*store_restore.KVStoreChangelog
	kvc = []*store_restore.KVStoreChangelog{
		store_restore.NewKVStoreChangelog(kvstore, mp.ChangelogManager(), sp.ParNum),
	}

	update_stats := func(ret *common.FnOutput) {
		ret.Latencies["eventTimeLatency"] = sinks_arr[0].GetEventTimeLatency()
	}
	transactionalID := fmt.Sprintf("%s-%s-%d-%s", h.funcName, sp.InputTopicNames[0],
		sp.ParNum, sp.OutputTopicNames[0])
	builder := stream_task.NewStreamTaskArgsBuilder(h.env, &ectx, transactionalID)
	streamTaskArgs := benchutil.UpdateStreamTaskArgs(sp, builder).
		KVStoreChangelogs(kvc).Build()
	return task.ExecuteApp(ctx, streamTaskArgs, update_stats)
}
