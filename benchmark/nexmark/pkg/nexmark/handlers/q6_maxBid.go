package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/ntypes"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/optional"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/store_with_changelog"
	"sharedlog-stream/pkg/stream_task"
	"time"

	"cs.utexas.edu/zjia/faas/types"
)

type q6MaxBid struct {
	env      types.Environment
	funcName string
	useCache bool
}

func NewQ6MaxBid(env types.Environment, funcName string) *q6MaxBid {
	envConfig := checkEnvConfig()
	fmt.Fprintf(os.Stderr, "Q6MaxBid useCache: %v\n", envConfig.useCache)
	return &q6MaxBid{
		env:      env,
		funcName: funcName,
		useCache: envConfig.useCache,
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
	return common.CompressData(encodedOutput), nil
}

func (h *q6MaxBid) Q6MaxBid(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	abSerde, err := ntypes.GetAuctionBidSerdeG(serdeFormat)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	asSerde, err := ntypes.GetAuctionIDSellerSerdeG(serdeFormat)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	inMsgSerde, err := commtypes.GetMsgGSerdeG(serdeFormat, asSerde, abSerde)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	ptSerde, err := ntypes.GetPriceTimeSerdeG(serdeFormat)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	changeSerde, err := commtypes.GetChangeGSerdeG(serdeFormat, ptSerde)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	outMsgSerde, err := commtypes.GetMsgGSerdeG[uint64](serdeFormat, commtypes.Uint64SerdeG{}, changeSerde)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	ectx, err := getExecutionCtxSingleSrcSinkMiddle(ctx, h.env, h.funcName, sp)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	maxBidStoreName := "q6MaxBidKVStore"
	msgSerde, err := processor.MsgSerdeWithValueTsG(serdeFormat, asSerde, ptSerde)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	mp, err := store_with_changelog.NewMaterializeParamBuilder[ntypes.AuctionIdSeller, commtypes.ValueTimestampG[ntypes.PriceTime]]().
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
	compare := func(a, b ntypes.AuctionIdSeller) bool {
		return ntypes.CompareAuctionIDSeller(a, b) < 0
	}
	var aggStore store.KeyValueStoreBackedByChangelogG[ntypes.AuctionIdSeller, commtypes.ValueTimestampG[ntypes.PriceTime]]
	kvstore, err := store_with_changelog.CreateInMemorySkipmapKVTableWithChangelogG(
		mp, store.LessFunc[ntypes.AuctionIdSeller](compare))
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	if h.useCache {
		sizeOfVTs := commtypes.ValueTimestampGSize[ntypes.PriceTime]{
			ValSizeFunc: ntypes.SizeOfPriceTime,
		}
		cacheStore := store.NewCachingKeyValueStoreG[ntypes.AuctionIdSeller, commtypes.ValueTimestampG[ntypes.PriceTime]](
			ctx, kvstore, ntypes.SizeOfAuctionIdSeller, sizeOfVTs.SizeOfValueTimestamp, q4SizePerStore)
		aggStore = cacheStore
	} else {
		aggStore = kvstore
	}
	aggProc := processor.NewMeteredProcessorG(
		processor.NewStreamAggregateProcessorG[ntypes.AuctionIdSeller, *ntypes.AuctionBid, ntypes.PriceTime]("maxBid",
			aggStore,
			processor.InitializerFuncG[ntypes.PriceTime](func() optional.Option[ntypes.PriceTime] { return optional.None[ntypes.PriceTime]() }),
			processor.AggregatorFuncG[ntypes.AuctionIdSeller, *ntypes.AuctionBid, ntypes.PriceTime](
				func(key ntypes.AuctionIdSeller, v *ntypes.AuctionBid, agg optional.Option[ntypes.PriceTime]) optional.Option[ntypes.PriceTime] {
					if agg.IsNone() {
						return optional.Some(ntypes.PriceTime{Price: v.BidPrice, DateTime: v.BidDateTime})
					}
					aggVal := agg.Unwrap()
					if v.BidPrice > aggVal.Price {
						return optional.Some(ntypes.PriceTime{Price: v.BidPrice, DateTime: v.BidDateTime})
					} else {
						return agg
					}
				}), h.useCache))
	groupByProc := processor.NewTableGroupByMapProcessorG[ntypes.AuctionIdSeller, ntypes.PriceTime, uint64, ntypes.PriceTime]("changeKey",
		processor.MapperFuncG[ntypes.AuctionIdSeller, ntypes.PriceTime, uint64, ntypes.PriceTime](
			func(key optional.Option[ntypes.AuctionIdSeller], value optional.Option[ntypes.PriceTime]) (uint64, ntypes.PriceTime, error) {
				k := key.Unwrap()
				return k.Seller, value.Unwrap(), nil
			}))
	sinkProc := processor.NewGroupByOutputProcessorG(ectx.Producers()[0], &ectx, outMsgSerde)
	aggProc.NextProcessor(groupByProc)
	groupByProc.NextProcessor(sinkProc)
	task := stream_task.NewStreamTaskBuilder().
		AppProcessFunc(func(ctx context.Context, task *stream_task.StreamTask, args processor.ExecutionContext) (
			*common.FnOutput, optional.Option[commtypes.RawMsgAndSeq],
		) {
			return stream_task.CommonProcess(ctx, task, args.(*processor.BaseExecutionContext),
				func(ctx context.Context, msg commtypes.MessageG[ntypes.AuctionIdSeller, *ntypes.AuctionBid], argsTmp interface{}) error {
					return aggProc.Process(ctx, msg)
				}, inMsgSerde)
		}).
		Build()
	kvc := map[string]store.KeyValueStoreOpWithChangelog{kvstore.ChangelogTopicName(): aggStore}
	builder := stream_task.NewStreamTaskArgsBuilder(h.env, &ectx,
		fmt.Sprintf("%s-%s-%d-%s", h.funcName, sp.InputTopicNames[0],
			sp.ParNum, sp.OutputTopicNames[0]))
	streamTaskArgs := benchutil.UpdateStreamTaskArgs(sp, builder).
		KVStoreChangelogs(kvc).Build()
	return stream_task.ExecuteApp(ctx, task, streamTaskArgs)
}
