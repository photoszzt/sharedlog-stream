package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/ntypes"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/debug"
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
	q5SizePerStore = 5 * 1024 * 1024
)

type q5MaxBid struct {
	env         types.Environment
	funcName    string
	useCache    bool
	inMsgSerde  commtypes.MessageGSerdeG[ntypes.StartEndTime, ntypes.AuctionIdCount]
	outMsgSerde commtypes.MessageGSerdeG[ntypes.StartEndTime, ntypes.AuctionIdCntMax]
	msgSerde    commtypes.MessageGSerdeG[ntypes.StartEndTime, commtypes.ValueTimestampG[uint64]]
}

func NewQ5MaxBid(env types.Environment, funcName string) types.FuncHandler {
	useCache := common.CheckCacheConfig()
	return &q5MaxBid{
		env:      env,
		funcName: funcName,
		useCache: useCache,
	}
}

func (h *q5MaxBid) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &common.QueryInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output := h.processQ5MaxBid(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	return common.CompressData(encodedOutput), nil
}

func (h *q5MaxBid) getSrcSink(ctx context.Context, sp *common.QueryInput,
) ([]*producer_consumer.MeteredConsumer, []producer_consumer.MeteredProducerIntr, error) {
	input_stream, output_streams, err := benchutil.GetShardedInputOutputStreams(ctx, h.env, sp)
	if err != nil {
		return nil, nil, err
	}
	debug.Assert(len(output_streams) == 1, "expected only one output stream")

	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	warmup := time.Duration(sp.WarmupS) * time.Second
	inConfig := &producer_consumer.StreamConsumerConfig{
		Timeout:     time.Duration(20) * time.Second,
		SerdeFormat: serdeFormat,
	}
	outConfig := &producer_consumer.StreamSinkConfig{
		FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
		Format:        serdeFormat,
	}
	consumer, err := producer_consumer.NewShardedSharedLogStreamConsumer(input_stream,
		inConfig, sp.NumSubstreamProducer[0], sp.ParNum)
	if err != nil {
		return nil, nil, err
	}
	src := producer_consumer.NewMeteredConsumer(consumer, warmup)
	sink, err := producer_consumer.NewConcurrentMeteredSyncProducer(producer_consumer.NewShardedSharedLogStreamProducer(output_streams[0], outConfig),
		warmup)
	if err != nil {
		return nil, nil, err
	}
	src.SetInitialSource(false)
	sink.MarkFinalOutput()
	return []*producer_consumer.MeteredConsumer{src}, []producer_consumer.MeteredProducerIntr{sink}, nil
}

func (h *q5MaxBid) setupSerde(serdeFormat commtypes.SerdeFormat) *common.FnOutput {
	var seSerde commtypes.SerdeG[ntypes.StartEndTime]
	var aucIdCountSerde commtypes.SerdeG[ntypes.AuctionIdCount]
	var aucIdCountMaxSerde commtypes.SerdeG[ntypes.AuctionIdCntMax]
	var err error
	if serdeFormat == commtypes.JSON {
		seSerde = ntypes.StartEndTimeJSONSerdeG{}
		aucIdCountSerde = ntypes.AuctionIdCountJSONSerdeG{}
		aucIdCountMaxSerde = ntypes.AuctionIdCntMaxJSONSerdeG{}
	} else if serdeFormat == commtypes.MSGP {
		seSerde = ntypes.StartEndTimeMsgpSerdeG{}
		aucIdCountSerde = ntypes.AuctionIdCountMsgpSerdeG{}

		aucIdCountMaxSerde = ntypes.AuctionIdCntMaxMsgpSerdeG{}
	} else {
		return common.GenErrFnOutput(
			fmt.Errorf("serde format should be either json or msgp; but %v is given", serdeFormat))
	}
	h.inMsgSerde, err = commtypes.GetMsgGSerdeG(serdeFormat, seSerde, aucIdCountSerde)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	h.outMsgSerde, err = commtypes.GetMsgGSerdeG(serdeFormat, seSerde, aucIdCountMaxSerde)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	h.msgSerde, err = processor.MsgSerdeWithValueTsG[ntypes.StartEndTime, uint64](serdeFormat,
		seSerde, commtypes.Uint64SerdeG{})
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	return nil
}

func (h *q5MaxBid) getAggStore(ctx context.Context, sp *common.QueryInput) (store.CachedKeyValueStoreBackedByChangelogG[ntypes.StartEndTime, commtypes.ValueTimestampG[uint64]], *common.FnOutput) {
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	maxBidStoreName := "maxBidsKVStore"
	mp, err := store_with_changelog.NewMaterializeParamBuilder[ntypes.StartEndTime, commtypes.ValueTimestampG[uint64]]().
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
	compare := func(a, b ntypes.StartEndTime) bool {
		return ntypes.CompareStartEndTime(a, b) < 0
	}
	var aggStore store.CachedKeyValueStoreBackedByChangelogG[ntypes.StartEndTime, commtypes.ValueTimestampG[uint64]]
	kvstore, err := store_with_changelog.CreateInMemorySkipmapKVTableWithChangelogG(mp, compare)
	if err != nil {
		return nil, common.GenErrFnOutput(err)
	}
	if h.useCache {
		sizeOfVTs := commtypes.ValueTimestampGSize[uint64]{
			ValSizeFunc: commtypes.SizeOfUint64,
		}
		cacheStore := store.NewCachingKeyValueStoreG[ntypes.StartEndTime, commtypes.ValueTimestampG[uint64]](
			ctx, kvstore, ntypes.SizeOfStartEndTime, sizeOfVTs.SizeOfValueTimestamp, q5SizePerStore)
		aggStore = cacheStore
	} else {
		aggStore = kvstore
	}
	return aggStore, nil
}

func (h *q5MaxBid) processQ5MaxBid(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	fn_out := h.setupSerde(serdeFormat)
	if fn_out != nil {
		return fn_out
	}
	srcs, sinks_arr, err := h.getSrcSink(ctx, sp)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	ectx := processor.NewExecutionContext(srcs,
		sinks_arr, h.funcName, sp.ScaleEpoch, sp.ParNum)
	aggStore, fn_out := h.getAggStore(ctx, sp)
	if fn_out != nil {
		return fn_out
	}
	maxBid := processor.NewMeteredProcessorG(
		processor.NewStreamAggregateProcessorG[ntypes.StartEndTime, ntypes.AuctionIdCount, uint64]("maxBid",
			aggStore, processor.InitializerFuncG[uint64](func() optional.Option[uint64] {
				return optional.Some(uint64(0))
			}),
			processor.AggregatorFuncG[ntypes.StartEndTime, ntypes.AuctionIdCount, uint64](
				func(key ntypes.StartEndTime, v ntypes.AuctionIdCount, agg optional.Option[uint64]) optional.Option[uint64] {
					aggVal := agg.Unwrap()
					if v.Count > aggVal {
						return optional.Some(v.Count)
					}
					return agg
				}), h.useCache))
	stJoin := processor.NewMeteredProcessorG[ntypes.StartEndTime, ntypes.AuctionIdCount, ntypes.StartEndTime, ntypes.AuctionIdCntMax](
		processor.NewStreamTableJoinProcessorG[ntypes.StartEndTime, ntypes.AuctionIdCount, uint64, ntypes.AuctionIdCntMax](aggStore,
			processor.ValueJoinerWithKeyFuncG[ntypes.StartEndTime, ntypes.AuctionIdCount, uint64, ntypes.AuctionIdCntMax](
				func(readOnlyKey ntypes.StartEndTime, lv ntypes.AuctionIdCount, rv uint64) optional.Option[ntypes.AuctionIdCntMax] {
					return optional.Some(ntypes.AuctionIdCntMax{
						AucId:  lv.AucId,
						Count:  lv.Count,
						MaxCnt: rv,
					})
				})))
	chooseMaxCnt := processor.NewStreamFilterProcessorG[ntypes.StartEndTime, ntypes.AuctionIdCntMax]("chooseMaxCnt",
		processor.PredicateFuncG[ntypes.StartEndTime, ntypes.AuctionIdCntMax](func(key optional.Option[ntypes.StartEndTime], value optional.Option[ntypes.AuctionIdCntMax]) (bool, error) {
			v := value.Unwrap()
			return v.Count >= v.MaxCnt, nil
		}))
	outProc := processor.NewFixedSubstreamOutputProcessorG("subG3Proc", sinks_arr[0],
		ectx.SubstreamNum(), h.outMsgSerde)
	stJoin.NextProcessor(chooseMaxCnt)
	chooseMaxCnt.NextProcessor(outProc)
	task := stream_task.NewStreamTaskBuilder().
		AppProcessFunc(func(ctx context.Context, task *stream_task.StreamTask,
			argsTmp processor.ExecutionContext,
		) (*common.FnOutput, optional.Option[commtypes.RawMsgAndSeq]) {
			args := argsTmp.(*processor.BaseExecutionContext)
			return stream_task.CommonProcess(ctx, task, args,
				func(ctx context.Context, msg commtypes.MessageG[ntypes.StartEndTime, ntypes.AuctionIdCount], argsTmp interface{}) error {
					// fmt.Fprintf(os.Stderr, "got msg with key: %v, val: %v, ts: %v\n", msg.Msg.Key, msg.Msg.Value, msg.Msg.Timestamp)
					_, err := maxBid.ProcessAndReturn(ctx, msg)
					if err != nil {
						return fmt.Errorf("maxBid err: %v", err)
					}
					return stJoin.Process(ctx, msg)
				}, h.inMsgSerde)
		}).MarkFinalStage().Build()
	kvc := map[string]store.KeyValueStoreOpWithChangelog{aggStore.ChangelogTopicName(): aggStore}
	transactionalID := fmt.Sprintf("%s-%s-%d-%s", h.funcName,
		sp.InputTopicNames[0], sp.ParNum, sp.OutputTopicNames[0])
	streamTaskArgs, err := benchutil.UpdateStreamTaskArgs(sp,
		stream_task.NewStreamTaskArgsBuilder(h.env, &ectx, transactionalID)).
		KVStoreChangelogs(kvc).
		FixedOutParNum(sp.ParNum).
		Build()
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	return stream_task.ExecuteApp(ctx, task, streamTaskArgs, func(ctx context.Context, env types.Environment,
		serdeFormat commtypes.SerdeFormat, rs *snapshot_store.RedisSnapshotStore,
	) error {
		payloadSerde, err := commtypes.GetPayloadArrSerdeG(serdeFormat)
		if err != nil {
			return err
		}
		stream_task.SetKVStoreWithChangelogSnapshot[ntypes.StartEndTime, commtypes.ValueTimestampG[uint64]](ctx, env, rs, aggStore, payloadSerde)
		return nil
	}, func() { outProc.OutputRemainingStats() })
}
