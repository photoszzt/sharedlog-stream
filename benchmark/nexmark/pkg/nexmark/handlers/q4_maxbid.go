package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/ntypes"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/optional"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/producer_consumer"
	"sharedlog-stream/pkg/snapshot_store"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/store_with_changelog"
	"sharedlog-stream/pkg/stream_task"
	"time"

	"cs.utexas.edu/zjia/faas/types"
)

const (
	q4SizePerStore = 10 * 1024 * 1024
)

type q4MaxBid struct {
	env         types.Environment
	funcName    string
	useCache    bool
	inMsgSerde  commtypes.MessageGSerdeG[ntypes.AuctionIdCategory, *ntypes.AuctionBid]
	outMsgSerde commtypes.MessageGSerdeG[uint64, commtypes.ChangeG[uint64]]
	msgSerde    commtypes.MessageGSerdeG[ntypes.AuctionIdCategory, commtypes.ValueTimestampG[uint64]]
}

func NewQ4MaxBid(env types.Environment, funcName string) *q4MaxBid {
	useCache := common.CheckCacheConfig()
	return &q4MaxBid{
		env:      env,
		funcName: funcName,
		useCache: useCache,
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

func getExecutionCtxSingleSrcSinkMiddle(ctx context.Context, env types.Environment, funcName string, sp *common.QueryInput) (processor.BaseExecutionContext, error) {
	inputStream, outputStreams, err := benchutil.GetShardedInputOutputStreams(ctx, env, sp)
	if err != nil {
		return processor.BaseExecutionContext{}, err
	}
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	inputConfig := &producer_consumer.StreamConsumerConfig{
		Timeout:     common.SrcConsumeTimeout,
		SerdeFormat: serdeFormat,
	}
	outConfig := &producer_consumer.StreamSinkConfig{
		FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
		Format:        serdeFormat,
	}
	warmup := time.Duration(sp.WarmupS) * time.Second
	consumer, err := producer_consumer.NewShardedSharedLogStreamConsumer(inputStream, inputConfig, sp.NumSubstreamProducer[0], sp.ParNum)
	if err != nil {
		return processor.BaseExecutionContext{}, err
	}
	src := producer_consumer.NewMeteredConsumer(consumer, warmup)
	sink, err := producer_consumer.NewMeteredProducer(
		producer_consumer.NewShardedSharedLogStreamProducer(outputStreams[0], outConfig),
		warmup)
	if err != nil {
		return processor.BaseExecutionContext{}, err
	}
	src.SetInitialSource(false)
	ectx := processor.NewExecutionContext([]*producer_consumer.MeteredConsumer{src},
		[]producer_consumer.MeteredProducerIntr{sink}, funcName, sp.ScaleEpoch, sp.ParNum)
	return ectx, nil
}

func (h *q4MaxBid) setupSerde(serdeFormat commtypes.SerdeFormat) *common.FnOutput {
	var abSerde commtypes.SerdeG[*ntypes.AuctionBid]
	var aicSerde commtypes.SerdeG[ntypes.AuctionIdCategory]
	var err error
	if serdeFormat == commtypes.JSON {
		abSerde = ntypes.AuctionBidJSONSerdeG{}
		aicSerde = ntypes.AuctionIdCategoryJSONSerdeG{}
	} else if serdeFormat == commtypes.MSGP {
		abSerde = ntypes.AuctionBidMsgpSerdeG{}
		aicSerde = ntypes.AuctionIdCategoryMsgpSerdeG{}
	} else {
		return common.GenErrFnOutput(fmt.Errorf("unrecognized format: %v", serdeFormat))
	}

	h.inMsgSerde, err = commtypes.GetMsgGSerdeG(serdeFormat, aicSerde, abSerde)
	if err != nil {
		return common.GenErrFnOutput(fmt.Errorf("get msg serde err: %v", err))
	}
	changeSerde, err := commtypes.GetChangeGSerdeG[uint64](serdeFormat, commtypes.Uint64SerdeG{})
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	h.outMsgSerde, err = commtypes.GetMsgGSerdeG[uint64](serdeFormat, commtypes.Uint64SerdeG{}, changeSerde)
	if err != nil {
		return common.GenErrFnOutput(fmt.Errorf("get msg serde err: %v", err))
	}
	h.msgSerde, err = processor.MsgSerdeWithValueTsG[ntypes.AuctionIdCategory, uint64](serdeFormat,
		aicSerde, commtypes.Uint64SerdeG{})
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	return nil
}

func (h *q4MaxBid) setupAggStore(ctx context.Context, sp *common.QueryInput) (
	store.CachedKeyValueStoreBackedByChangelogG[ntypes.AuctionIdCategory, commtypes.ValueTimestampG[uint64]],
	*common.FnOutput,
) {
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	maxBidStoreName := "q4MaxBidKVStore"
	mp, err := store_with_changelog.NewMaterializeParamBuilder[ntypes.AuctionIdCategory, commtypes.ValueTimestampG[uint64]]().
		MessageSerde(h.msgSerde).
		StoreName(maxBidStoreName).
		ParNum(sp.ParNum).
		SerdeFormat(serdeFormat).
		ChangelogManagerParam(commtypes.CreateChangelogManagerParam{
			Env:           h.env,
			NumPartition:  sp.NumChangelogPartition,
			TimeOut:       common.SrcConsumeTimeout,
			FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
		}).BufMaxSize(sp.BufMaxSize).Build()
	if err != nil {
		return nil, common.GenErrFnOutput(err)
	}
	compare := func(a, b ntypes.AuctionIdCategory) bool {
		return ntypes.CompareAuctionIdCategory(&a, &b) < 0
	}
	var aggStore store.CachedKeyValueStoreBackedByChangelogG[ntypes.AuctionIdCategory, commtypes.ValueTimestampG[uint64]]
	kvstore, err := store_with_changelog.CreateInMemorySkipmapKVTableWithChangelogG(mp,
		store.LessFunc[ntypes.AuctionIdCategory](compare))
	if err != nil {
		return nil, common.GenErrFnOutput(err)
	}
	if h.useCache {
		sizeOfVTs := commtypes.ValueTimestampGSize[uint64]{
			ValSizeFunc: commtypes.SizeOfUint64,
		}
		cacheStore := store.NewCachingKeyValueStoreG[ntypes.AuctionIdCategory, commtypes.ValueTimestampG[uint64]](
			ctx, kvstore, ntypes.SizeOfAuctionIdCategory, sizeOfVTs.SizeOfValueTimestamp, q4SizePerStore)
		aggStore = cacheStore
	} else {
		aggStore = kvstore
	}
	return aggStore, nil
}

func (h *q4MaxBid) Q4MaxBid(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	fn_out := h.setupSerde(serdeFormat)
	if fn_out != nil {
		return fn_out
	}
	ectx, err := getExecutionCtxSingleSrcSinkMiddle(ctx, h.env, h.funcName, sp)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	aggStore, fn_out := h.setupAggStore(ctx, sp)
	if fn_out != nil {
		return fn_out
	}
	aggProc := processor.NewMeteredProcessorG(
		processor.NewStreamAggregateProcessorG[ntypes.AuctionIdCategory, *ntypes.AuctionBid, uint64]("maxBid",
			aggStore, processor.InitializerFuncG[uint64](func() optional.Option[uint64] { return optional.None[uint64]() }),
			processor.AggregatorFuncG[ntypes.AuctionIdCategory, *ntypes.AuctionBid, uint64](
				func(key ntypes.AuctionIdCategory, value *ntypes.AuctionBid, aggregate optional.Option[uint64]) optional.Option[uint64] {
					if aggregate.IsNone() {
						return optional.Some(value.BidPrice)
					}
					p := aggregate.Unwrap()
					if value.BidPrice > p {
						return optional.Some(value.BidPrice)
					} else {
						return aggregate
					}
				}), h.useCache))
	groupByProc := processor.NewTableGroupByMapProcessorG[ntypes.AuctionIdCategory, uint64, uint64, uint64]("changeKey",
		processor.MapperFuncG[ntypes.AuctionIdCategory, uint64, uint64, uint64](
			func(key optional.Option[ntypes.AuctionIdCategory], value optional.Option[uint64]) (optional.Option[uint64], optional.Option[uint64], error) {
				k := key.Unwrap()
				v := value.Unwrap()
				return optional.Some(k.Category), optional.Some(v), nil
			}))
	sinkProc := processor.NewGroupByOutputProcessorG("subG3Proc", ectx.Producers()[0], &ectx, h.outMsgSerde)
	aggProc.NextProcessor(groupByProc)
	groupByProc.NextProcessor(sinkProc)
	task := stream_task.NewStreamTaskBuilder().
		AppProcessFunc(func(ctx context.Context, task *stream_task.StreamTask, args processor.ExecutionContext) (
			*common.FnOutput, optional.Option[commtypes.RawMsgAndSeq],
		) {
			return stream_task.CommonProcess(ctx, task, args.(*processor.BaseExecutionContext),
				func(ctx context.Context, msg commtypes.MessageG[ntypes.AuctionIdCategory, *ntypes.AuctionBid], argsTmp interface{}) error {
					return aggProc.Process(ctx, msg)
				}, h.inMsgSerde)
		}).
		Build()

	kvc := map[string]store.KeyValueStoreOpWithChangelog{aggStore.ChangelogTopicName(): aggStore}
	builder := stream_task.NewStreamTaskArgsBuilder(h.env, &ectx,
		fmt.Sprintf("%s-%s-%d-%s", h.funcName, sp.InputTopicNames[0],
			sp.ParNum, sp.OutputTopicNames[0]))
	streamTaskArgs := benchutil.UpdateStreamTaskArgs(sp, builder).
		KVStoreChangelogs(kvc).Build()
	return stream_task.ExecuteApp(ctx, task, streamTaskArgs, func(ctx context.Context,
		env types.Environment, serdeFormat commtypes.SerdeFormat,
		rs *snapshot_store.RedisSnapshotStore,
	) error {
		payloadSerde, err := commtypes.GetPayloadArrSerdeG(serdeFormat)
		if err != nil {
			return err
		}
		stream_task.SetKVStoreWithChangelogSnapshot[ntypes.AuctionIdCategory, commtypes.ValueTimestampG[uint64]](ctx, env, rs, aggStore, payloadSerde)
		return nil
	}, func() { sinkProc.OutputRemainingStats() })
}
