package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/ntypes"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/execution"
	"sharedlog-stream/pkg/optional"
	"sharedlog-stream/pkg/proc_interface"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/producer_consumer"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/store_with_changelog"
	"sharedlog-stream/pkg/stream_task"
	"time"

	"cs.utexas.edu/zjia/faas/types"
)

const (
	auctionDurationUpper = time.Duration(1800) * time.Second
)

type q4JoinStreamHandler struct {
	env      types.Environment
	funcName string
}

func NewQ4JoinStreamHandler(env types.Environment, funcName string) types.FuncHandler {
	return &q4JoinStreamHandler{
		env:      env,
		funcName: funcName,
	}
}

func (h *q4JoinStreamHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &common.QueryInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output := h.Q4JoinStream(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	// fmt.Printf("query 3 output: %v\n", encodedOutput)
	return common.CompressData(encodedOutput), nil
}

func (h *q4JoinStreamHandler) getSrcSink(ctx context.Context, sp *common.QueryInput,
) ([]*producer_consumer.MeteredConsumer, []producer_consumer.MeteredProducerIntr, error) {
	stream1, stream2, outputStream, err := getInOutStreams(h.env, sp)
	if err != nil {
		return nil, nil, err
	}
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	timeout := time.Duration(10) * time.Millisecond
	auctionsConfig := &producer_consumer.StreamConsumerConfig{
		Timeout:     timeout,
		SerdeFormat: serdeFormat,
	}
	personsConfig := &producer_consumer.StreamConsumerConfig{
		Timeout:     timeout,
		SerdeFormat: serdeFormat,
	}
	outConfig := &producer_consumer.StreamSinkConfig{
		FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
		Format:        serdeFormat,
	}
	warmup := time.Duration(sp.WarmupS) * time.Second
	consumer1, err := producer_consumer.NewShardedSharedLogStreamConsumer(stream1, auctionsConfig, sp.NumSubstreamProducer[0], sp.ParNum)
	if err != nil {
		return nil, nil, err
	}
	consumer2, err := producer_consumer.NewShardedSharedLogStreamConsumer(stream2, personsConfig, sp.NumSubstreamProducer[1], sp.ParNum)
	if err != nil {
		return nil, nil, err
	}
	src1 := producer_consumer.NewMeteredConsumer(consumer1, warmup)
	src2 := producer_consumer.NewMeteredConsumer(consumer2, warmup)
	sink, err := producer_consumer.NewConcurrentMeteredSyncProducer(
		producer_consumer.NewShardedSharedLogStreamProducer(outputStream, outConfig), warmup)
	if err != nil {
		return nil, nil, err
	}
	src1.SetInitialSource(false)
	src2.SetInitialSource(false)
	src1.SetName("aucsByIDSrc")
	src2.SetName("bidsByAucIDSrc")
	return []*producer_consumer.MeteredConsumer{src1, src2}, []producer_consumer.MeteredProducerIntr{sink}, nil
}

func (h *q4JoinStreamHandler) Q4JoinStream(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	eventSerde, err := ntypes.GetEventSerdeG(serdeFormat)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	msgSerde, err := commtypes.GetMsgGSerdeG[uint64](serdeFormat, commtypes.Uint64SerdeG{}, eventSerde)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	abSerde, err := ntypes.GetAuctionBidSerdeG(serdeFormat)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	aicSerde, err := ntypes.GetAuctionIdCategorySerdeG(serdeFormat)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	outMsgSerde, err := commtypes.GetMsgGSerdeG(serdeFormat, aicSerde, abSerde)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	srcs, sinks_arr, err := h.getSrcSink(ctx, sp)
	if err != nil {
		return &common.FnOutput{Success: false, Message: fmt.Sprintf("getSrcSink err: %v\n", err)}
	}
	jw, err := commtypes.NewJoinWindowsNoGrace(auctionDurationUpper)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	joiner := processor.ValueJoinerWithKeyTsFuncG[uint64, *ntypes.Event, *ntypes.Event, *ntypes.AuctionBid](
		func(_readOnlyKey uint64, value1 *ntypes.Event, value2 *ntypes.Event, _leftTs, otherTs int64) *ntypes.AuctionBid {
			auc := value1.NewAuction
			bid := value2.Bid
			return &ntypes.AuctionBid{
				BidDateTime: bid.DateTime,
				BidPrice:    bid.Price,
				AucDateTime: auc.DateTime,
				AucExpires:  auc.Expires,
				AucCategory: auc.Category,
			}
		})
	flushDur := time.Duration(sp.FlushMs) * time.Millisecond
	aucMp, err := store_with_changelog.NewMaterializeParamBuilder[uint64, *ntypes.Event]().
		MessageSerde(msgSerde).StoreName("auctionsByIDStore").
		ParNum(sp.ParNum).SerdeFormat(serdeFormat).
		ChangelogManagerParam(commtypes.CreateChangelogManagerParam{
			Env:           h.env,
			NumPartition:  sp.NumChangelogPartition,
			TimeOut:       common.SrcConsumeTimeout,
			FlushDuration: flushDur,
		}).Build()
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	bidMp, err := store_with_changelog.NewMaterializeParamBuilder[uint64, *ntypes.Event]().
		MessageSerde(msgSerde).StoreName("bidsByAuctionIDStore").
		ParNum(sp.ParNum).SerdeFormat(serdeFormat).
		ChangelogManagerParam(commtypes.CreateChangelogManagerParam{
			Env:           h.env,
			NumPartition:  sp.NumChangelogPartition,
			TimeOut:       common.SrcConsumeTimeout,
			FlushDuration: flushDur,
		}).Build()
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	aucJoinBidsFunc, bidsJoinAucFunc, wsc, setSnapFunc, err := execution.SetupSkipMapStreamStreamJoin(
		aucMp, bidMp, store.IntegerCompare[uint64], joiner, jw)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	getValidBid := processor.NewStreamFilterProcessorG[uint64, *ntypes.AuctionBid]("getValidBid",
		processor.PredicateFuncG[uint64, *ntypes.AuctionBid](
			func(key optional.Option[uint64], value optional.Option[*ntypes.AuctionBid]) (bool, error) {
				ab := value.Unwrap()
				return ab.BidDateTime >= ab.AucDateTime && ab.BidDateTime <= ab.AucExpires, nil
			}))

	filterAndGroupMsg := func(ctx context.Context, msgs []commtypes.MessageG[uint64, *ntypes.AuctionBid]) (
		[]commtypes.MessageG[ntypes.AuctionIdCategory, *ntypes.AuctionBid], error,
	) {
		var newMsgs []commtypes.MessageG[ntypes.AuctionIdCategory, *ntypes.AuctionBid]
		for _, msg := range msgs {
			validBid, err := getValidBid.ProcessAndReturn(ctx, msg)
			if err != nil {
				return nil, err
			}
			if validBid != nil && validBid[0].Value.IsSome() {
				k := validBid[0].Key.Unwrap()
				v := validBid[0].Value.Unwrap()
				aic := ntypes.AuctionIdCategory{
					AucId:    k,
					Category: v.AucCategory,
				}
				newMsg := commtypes.MessageG[ntypes.AuctionIdCategory, *ntypes.AuctionBid]{
					Key: optional.Some(aic), Value: validBid[0].Value,
					TimestampMs:   validBid[0].TimestampMs,
					StartProcTime: validBid[0].StartProcTime,
				}
				newMsgs = append(newMsgs, newMsg)
			}
		}
		return newMsgs, nil
	}

	aJoinB := execution.JoinWorkerFunc[uint64, *ntypes.Event, ntypes.AuctionIdCategory, *ntypes.AuctionBid](
		func(ctx context.Context, m commtypes.MessageG[uint64, *ntypes.Event]) (
			[]commtypes.MessageG[ntypes.AuctionIdCategory, *ntypes.AuctionBid], error,
		) {
			joined, err := aucJoinBidsFunc(ctx, m)
			if err != nil {
				return nil, err
			}
			msgs, err := filterAndGroupMsg(ctx, joined)
			// debug.Fprintf(os.Stderr, "after left filter and group\n")
			return msgs, err
		})

	bJoinA := execution.JoinWorkerFunc[uint64, *ntypes.Event, ntypes.AuctionIdCategory, *ntypes.AuctionBid](
		func(ctx context.Context, m commtypes.MessageG[uint64, *ntypes.Event]) (
			[]commtypes.MessageG[ntypes.AuctionIdCategory, *ntypes.AuctionBid], error,
		) {
			joined, err := bidsJoinAucFunc(ctx, m)
			if err != nil {
				return nil, err
			}
			msgs, err := filterAndGroupMsg(ctx, joined)
			// debug.Fprintf(os.Stderr, "after right filter and group\n")
			return msgs, err
		})
	msgSerdePair := execution.NewMsgSerdePair(msgSerde, outMsgSerde)
	task, procArgs := execution.PrepareTaskWithJoin(
		ctx, aJoinB, bJoinA, proc_interface.NewBaseSrcsSinks(srcs, sinks_arr),
		proc_interface.NewBaseProcArgs(h.funcName, sp.ScaleEpoch, sp.ParNum), false,
		msgSerdePair, msgSerdePair, "subG2")
	transactionalID := fmt.Sprintf("%s-%s-%d", h.funcName,
		sp.InputTopicNames[0], sp.ParNum)
	streamTaskArgs := benchutil.UpdateStreamTaskArgs(sp,
		stream_task.NewStreamTaskArgsBuilder(h.env, procArgs, transactionalID)).
		WindowStoreChangelogs(wsc).FixedOutParNum(sp.ParNum).Build()
	return stream_task.ExecuteApp(ctx, task, streamTaskArgs, setSnapFunc, func() { procArgs.OutputRemainingStats() })
}
