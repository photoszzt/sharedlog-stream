package handlers

import (
	"context"
	"encoding/json"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/ntypes"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/optional"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/stream_task"

	"cs.utexas.edu/zjia/faas/types"
)

type q6MaxBid struct {
	env         types.Environment
	funcName    string
	useCache    bool
	inMsgSerde  commtypes.MessageGSerdeG[ntypes.AuctionIdSeller, *ntypes.AuctionBid]
	outMsgSerde commtypes.MessageGSerdeG[uint64, commtypes.ChangeG[ntypes.PriceTime]]
	msgSerde    commtypes.MessageGSerdeG[ntypes.AuctionIdSeller, commtypes.ValueTimestampG[ntypes.PriceTime]]
}

func NewQ6MaxBid(env types.Environment, funcName string) *q6MaxBid {
	useCache := common.CheckCacheConfig()
	return &q6MaxBid{
		env:      env,
		funcName: funcName,
		useCache: useCache,
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

func (h *q6MaxBid) setupSerde(serdeFormat commtypes.SerdeFormat) *common.FnOutput {
	abSerde, err := ntypes.GetAuctionBidSerdeG(serdeFormat)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	asSerde, err := ntypes.GetAuctionIDSellerSerdeG(serdeFormat)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	h.inMsgSerde, err = commtypes.GetMsgGSerdeG(serdeFormat, asSerde, abSerde)
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
	h.outMsgSerde, err = commtypes.GetMsgGSerdeG[uint64](serdeFormat, commtypes.Uint64SerdeG{}, changeSerde)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	h.msgSerde, err = processor.MsgSerdeWithValueTsG(serdeFormat, asSerde, ptSerde)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	return nil
}

func (h *q6MaxBid) Q6MaxBid(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	fn_out := h.setupSerde(serdeFormat)
	if fn_out != nil {
		return fn_out
	}
	ectx, err := getExecutionCtxSingleSrcSinkMiddle(ctx, h.env, h.funcName, sp)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	useCache := benchutil.UseCache(h.useCache, sp.GuaranteeMth)
	aggStore, builder, snapfunc, err := getKVStoreAndStreamArgs(ctx, h.env, sp,
		&KVStoreStreamArgsParam[ntypes.AuctionIdSeller, ntypes.PriceTime]{
			StoreName: "q6MaxBidKVStore",
			FuncName:  h.funcName,
			MsgSerde:  h.msgSerde,
			Compare:   ntypes.AuctionIdSellerLess,
			SizeofK:   ntypes.SizeOfAuctionIdSeller,
			SizeofV:   ntypes.SizeOfPriceTime,
			UseCache:  useCache,
		}, &ectx)
	if err != nil {
		return common.GenErrFnOutput(err)
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
				}), useCache))
	groupByProc := processor.NewTableGroupByMapProcessorG[ntypes.AuctionIdSeller, ntypes.PriceTime, uint64, ntypes.PriceTime]("changeKey",
		processor.MapperFuncG[ntypes.AuctionIdSeller, ntypes.PriceTime, uint64, ntypes.PriceTime](
			func(key optional.Option[ntypes.AuctionIdSeller], value optional.Option[ntypes.PriceTime]) (optional.Option[uint64], optional.Option[ntypes.PriceTime], error) {
				k := key.Unwrap()
				return optional.Some(k.Seller), value, nil
			}))
	sinkProc := processor.NewGroupByOutputProcessorG("subG3Proc", ectx.Producers()[0], &ectx, h.outMsgSerde)
	aggProc.NextProcessor(groupByProc)
	groupByProc.NextProcessor(sinkProc)
	task := stream_task.NewStreamTaskBuilder().
		AppProcessFunc(func(ctx context.Context, task *stream_task.StreamTask, args processor.ExecutionContext) (
			*common.FnOutput, optional.Option[commtypes.RawMsgAndSeq],
		) {
			return stream_task.CommonProcess(ctx, task, args.(*processor.BaseExecutionContext),
				func(ctx context.Context, msg commtypes.MessageG[ntypes.AuctionIdSeller, *ntypes.AuctionBid], argsTmp interface{}) error {
					return aggProc.Process(ctx, msg)
				}, h.inMsgSerde)
		}).
		Build()
	streamTaskArgs, err := builder.Build()
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	return stream_task.ExecuteApp(ctx, task, streamTaskArgs, snapfunc, func() { sinkProc.OutputRemainingStats() })
}
