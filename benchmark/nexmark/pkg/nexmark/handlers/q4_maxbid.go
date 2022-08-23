package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/ntypes"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/producer_consumer"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/store_with_changelog"
	"sharedlog-stream/pkg/stream_task"
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
	return common.CompressData(encodedOutput), nil
}

func (h *q4MaxBid) getExecutionCtx(ctx context.Context, sp *common.QueryInput) (processor.BaseExecutionContext, error) {
	inputStream, outputStreams, err := benchutil.GetShardedInputOutputStreams(ctx, h.env, sp)
	if err != nil {
		return processor.BaseExecutionContext{}, err
	}
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	var abSerde commtypes.SerdeG[*ntypes.AuctionBid]
	var aicSerde commtypes.SerdeG[ntypes.AuctionIdCategory]
	if serdeFormat == commtypes.JSON {
		abSerde = ntypes.AuctionBidJSONSerdeG{}
		aicSerde = ntypes.AuctionIdCategoryJSONSerdeG{}
	} else if serdeFormat == commtypes.MSGP {
		abSerde = ntypes.AuctionBidMsgpSerdeG{}
		aicSerde = ntypes.AuctionIdCategoryMsgpSerdeG{}
	} else {
		return processor.BaseExecutionContext{}, fmt.Errorf("unrecognized format: %v", serdeFormat)
	}
	inMsgSerde, err := commtypes.GetMsgSerdeG(serdeFormat, aicSerde, abSerde)
	if err != nil {
		return processor.BaseExecutionContext{}, fmt.Errorf("get msg serde err: %v", err)
	}
	inputConfig := &producer_consumer.StreamConsumerConfigG[ntypes.AuctionIdCategory, *ntypes.AuctionBid]{
		Timeout:     common.SrcConsumeTimeout,
		MsgSerde:    inMsgSerde,
		SerdeFormat: serdeFormat,
	}
	changeSerde, err := commtypes.GetChangeSerdeG(serdeFormat, commtypes.Uint64Serde{})
	if err != nil {
		return processor.BaseExecutionContext{}, err
	}
	outMsgSerde, err := commtypes.GetMsgSerdeG[uint64](serdeFormat, commtypes.Uint64SerdeG{}, changeSerde)
	if err != nil {
		return processor.BaseExecutionContext{}, fmt.Errorf("get msg serde err: %v", err)
	}
	outConfig := &producer_consumer.StreamSinkConfig[uint64, commtypes.Change]{
		MsgSerde:      outMsgSerde,
		FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
	}
	warmup := time.Duration(sp.WarmupS) * time.Second
	consumer, err := producer_consumer.NewShardedSharedLogStreamConsumerG(inputStream, inputConfig, sp.NumSubstreamProducer[0], sp.ParNum)
	if err != nil {
		return processor.BaseExecutionContext{}, err
	}
	src := producer_consumer.NewMeteredConsumer(consumer, warmup)
	sink := producer_consumer.NewMeteredProducer(
		producer_consumer.NewShardedSharedLogStreamProducer(outputStreams[0], outConfig),
		warmup)
	src.SetInitialSource(false)
	ectx := processor.NewExecutionContext([]producer_consumer.MeteredConsumerIntr{src},
		[]producer_consumer.MeteredProducerIntr{sink}, h.funcName, sp.ScaleEpoch, sp.ParNum)
	return ectx, nil
}

func (h *q4MaxBid) Q4MaxBid(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	ectx, err := h.getExecutionCtx(ctx, sp)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	maxBidStoreName := "q4MaxBidKVStore"
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	aicSerde, err := ntypes.GetAuctionIdCategorySerdeG(serdeFormat)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	bpSerde, err := ntypes.GetBidPriceSerdeG(serdeFormat)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	msgSerde, err := processor.MsgSerdeWithValueTsG(serdeFormat,
		aicSerde, bpSerde)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	mp, err := store_with_changelog.NewMaterializeParamBuilder[ntypes.AuctionIdCategory, commtypes.ValueTimestampG[*ntypes.BidPrice]]().
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
	compare := func(a, b ntypes.AuctionIdCategory) bool {
		return ntypes.CompareAuctionIdCategory(&a, &b) < 0
	}
	kvstore, err := store_with_changelog.CreateInMemorySkipmapKVTableWithChangelogG(mp,
		store.LessFunc[ntypes.AuctionIdCategory](compare))
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	// store.NewCachingKeyValueStoreG[ntypes.AuctionIdCategory](ctx, mp.StoreName(), kvstoreWithChangelog,
	// 	ntypes.SizeOfAuctionIdCategory, commtypes.SizeOfValueTimestamp)
	ectx.Via(processor.NewMeteredProcessor(
		processor.NewStreamAggregateProcessorG[ntypes.AuctionIdCategory, *ntypes.AuctionBid, *ntypes.BidPrice]("maxBid", kvstore,
			processor.InitializerFuncG[*ntypes.BidPrice](func() *ntypes.BidPrice { return nil }),
			processor.AggregatorFuncG[ntypes.AuctionIdCategory, *ntypes.AuctionBid, *ntypes.BidPrice](
				func(key ntypes.AuctionIdCategory, value *ntypes.AuctionBid, aggregate *ntypes.BidPrice) *ntypes.BidPrice {
					if aggregate == nil {
						return &ntypes.BidPrice{Price: value.BidPrice}
					}
					if value.BidPrice > aggregate.Price {
						return &ntypes.BidPrice{Price: value.BidPrice}
					} else {
						return aggregate
					}
				})))).
		Via(processor.NewMeteredProcessor(processor.NewTableGroupByMapProcessor("changeKey",
			processor.MapperFunc(func(key, value interface{}) (interface{}, interface{}, error) {
				return key.(ntypes.AuctionIdCategory).Category, value.(*ntypes.BidPrice).Price, nil
			})))).
		Via(processor.NewGroupByOutputProcessor(ectx.Producers()[0], &ectx))

	task := stream_task.NewStreamTaskBuilder().Build()

	kvc := map[string]store.KeyValueStoreOpWithChangelog{kvstore.ChangelogTopicName(): kvstore}
	builder := stream_task.NewStreamTaskArgsBuilder(h.env, &ectx,
		fmt.Sprintf("%s-%s-%d-%s", h.funcName, sp.InputTopicNames[0],
			sp.ParNum, sp.OutputTopicNames[0]))
	streamTaskArgs := benchutil.UpdateStreamTaskArgs(sp, builder).
		KVStoreChangelogs(kvc).Build()
	return stream_task.ExecuteApp(ctx, task, streamTaskArgs)
}
