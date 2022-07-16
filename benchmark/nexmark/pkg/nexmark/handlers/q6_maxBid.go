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

type q6MaxBid struct {
	env      types.Environment
	funcName string
}

func NewQ6MaxBid(env types.Environment, funcName string) *q6MaxBid {
	return &q6MaxBid{
		env:      env,
		funcName: funcName,
	}
}

func (h *q6MaxBid) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &common.QueryInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output := h.Q6MaxBid(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	return utils.CompressData(encodedOutput), nil
}

func (h *q6MaxBid) getExecutionCtx(ctx context.Context, sp *common.QueryInput) (processor.BaseExecutionContext, error) {
	inputStream, outputStreams, err := benchutil.GetShardedInputOutputStreams(ctx, h.env, sp)
	if err != nil {
		return processor.EmptyBaseExecutionContext, err
	}
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	abSerde, err := ntypes.GetAuctionBidSerde(serdeFormat)
	if err != nil {
		return processor.EmptyBaseExecutionContext, err
	}
	asSerde, err := ntypes.GetAuctionIDSellerSerde(serdeFormat)
	if err != nil {
		return processor.EmptyBaseExecutionContext, err
	}
	inMsgSerde, err := commtypes.GetMsgSerde(serdeFormat, asSerde, abSerde)
	if err != nil {
		return processor.EmptyBaseExecutionContext, err
	}
	inputConfig := &producer_consumer.StreamConsumerConfig{
		Timeout:  common.SrcConsumeTimeout,
		MsgSerde: inMsgSerde,
	}
	ptSerde, err := ntypes.GetPriceTimeSerde(serdeFormat)
	if err != nil {
		return processor.EmptyBaseExecutionContext, err
	}
	changeSerde, err := commtypes.GetChangeSerde(serdeFormat, ptSerde)
	if err != nil {
		return processor.EmptyBaseExecutionContext, err
	}
	outMsgSerde, err := commtypes.GetMsgSerde(serdeFormat, commtypes.Uint64Serde{}, changeSerde)
	if err != nil {
		return processor.EmptyBaseExecutionContext, fmt.Errorf("get msg serde err: %v", err)
	}
	outConfig := &producer_consumer.StreamSinkConfig{
		MsgSerde:      outMsgSerde,
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
	ectx := processor.NewExecutionContext([]producer_consumer.MeteredConsumerIntr{src},
		[]producer_consumer.MeteredProducerIntr{sink}, h.funcName, sp.ScaleEpoch, sp.ParNum)
	return ectx, nil
}

func (h *q6MaxBid) Q6MaxBid(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	ectx, err := h.getExecutionCtx(ctx, sp)
	if err != nil {
		panic(err)
	}
	maxBidStoreName := "q6MaxBidKVStore"
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	ptSerde, err := ntypes.GetPriceTimeSerde(serdeFormat)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	msgSerde, err := processor.MsgSerdeWithValueTs(serdeFormat,
		ectx.Consumers()[0].MsgSerde().GetKeySerde(), ptSerde)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	mp, err := store_with_changelog.NewMaterializeParamBuilder().
		MessageSerde(msgSerde).
		StoreName(maxBidStoreName).
		ParNum(sp.ParNum).
		SerdeFormat(commtypes.SerdeFormat(sp.SerdeFormat)).
		ChangelogManagerParam(commtypes.CreateChangelogManagerParam{
			Env:           h.env,
			NumPartition:  sp.NumInPartition,
			TimeOut:       common.SrcConsumeTimeout,
			FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
		}).Build()
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	compare := func(a, b treemap.Key) int {
		ka := a.(ntypes.AuctionIdSeller)
		kb := b.(ntypes.AuctionIdSeller)
		return ntypes.CompareAuctionIDSeller(&ka, &kb)
	}
	kvstore, err := store_with_changelog.CreateInMemKVTableWithChangelog(
		mp, compare)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	ectx.Via(processor.NewMeteredProcessor(processor.NewStreamAggregateProcessor("maxBid", kvstore,
		processor.InitializerFunc(func() interface{} { return nil }),
		processor.AggregatorFunc(func(key, value, aggregate interface{}) interface{} {
			v := value.(ntypes.AuctionBid)
			if aggregate == nil {
				return ntypes.PriceTime{Price: v.BidPrice, DateTime: v.BidDateTime}
			}
			agg := aggregate.(ntypes.PriceTime)
			if v.BidPrice > agg.Price {
				return ntypes.PriceTime{Price: v.BidPrice, DateTime: v.BidDateTime}
			} else {
				return agg
			}
		})))).
		Via(processor.NewMeteredProcessor(processor.NewTableGroupByMapProcessor("changeKey",
			processor.MapperFunc(func(key, value interface{}) (interface{}, interface{}, error) {
				return key.(ntypes.AuctionIdSeller).Seller, value, nil
			})))).
		Via(processor.NewGroupByOutputProcessor(ectx.Producers()[0], &ectx))
	task := stream_task.NewStreamTaskBuilder().
		AppProcessFunc(func(ctx context.Context, task *stream_task.StreamTask, argsTmp interface{}) *common.FnOutput {
			args := argsTmp.(processor.ExecutionContext)
			return execution.CommonProcess(ctx, task, args, processor.ProcessMsg)
		}).Build()

	kvc := []*store_restore.KVStoreChangelog{
		store_restore.NewKVStoreChangelog(kvstore, kvstore.ChangelogManager()),
	}
	builder := stream_task.NewStreamTaskArgsBuilder(h.env, &ectx,
		fmt.Sprintf("%s-%s-%d-%s", h.funcName, sp.InputTopicNames[0],
			sp.ParNum, sp.OutputTopicNames[0]))
	streamTaskArgs := benchutil.UpdateStreamTaskArgs(sp, builder).
		KVStoreChangelogs(kvc).Build()
	return task.ExecuteApp(ctx, streamTaskArgs)
}