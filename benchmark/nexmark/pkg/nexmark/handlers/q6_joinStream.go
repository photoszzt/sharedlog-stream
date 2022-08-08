package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"
	nutils "sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/concurrent_skiplist"
	"sharedlog-stream/pkg/execution"
	"sharedlog-stream/pkg/proc_interface"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/producer_consumer"
	"sharedlog-stream/pkg/store_with_changelog"
	"sharedlog-stream/pkg/stream_task"
	"sharedlog-stream/pkg/utils"
	"time"

	"cs.utexas.edu/zjia/faas/types"
)

type q6JoinStreamHandler struct {
	env      types.Environment
	funcName string
}

func NewQ6JoinStreamHandler(env types.Environment, funcName string) types.FuncHandler {
	return &q6JoinStreamHandler{
		env:      env,
		funcName: funcName,
	}
}

func (h *q6JoinStreamHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &common.QueryInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output := h.Q6JoinStream(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	return nutils.CompressData(encodedOutput), nil
}

func (h *q6JoinStreamHandler) getSrcSink(ctx context.Context, sp *common.QueryInput,
) ([]producer_consumer.MeteredConsumerIntr, []producer_consumer.MeteredProducerIntr, error) {
	stream1, stream2, outputStream, err := getInOutStreams(h.env, sp)
	if err != nil {
		return nil, nil, err
	}
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	eventSerde, err := ntypes.GetEventSerdeG(serdeFormat)
	if err != nil {
		return nil, nil, fmt.Errorf("get event serde err: %v", err)
	}
	msgSerde, err := commtypes.GetMsgSerdeG[uint64](serdeFormat, commtypes.Uint64SerdeG{}, eventSerde)
	if err != nil {
		return nil, nil, fmt.Errorf("get msg serde err: %v", err)
	}
	timeout := time.Duration(10) * time.Millisecond
	src1Config := &producer_consumer.StreamConsumerConfigG[uint64, *ntypes.Event]{
		Timeout:     timeout,
		MsgSerde:    msgSerde,
		SerdeFormat: serdeFormat,
	}
	src2Config := &producer_consumer.StreamConsumerConfigG[uint64, *ntypes.Event]{
		Timeout:     timeout,
		MsgSerde:    msgSerde,
		SerdeFormat: serdeFormat,
	}
	abSerde, err := ntypes.GetAuctionBidSerdeG(serdeFormat)
	if err != nil {
		return nil, nil, err
	}
	asSerde, err := ntypes.GetAuctionIDSellerSerdeG(serdeFormat)
	if err != nil {
		return nil, nil, err
	}
	outMsgSerde, err := commtypes.GetMsgSerdeG(serdeFormat, asSerde, abSerde)
	if err != nil {
		return nil, nil, err
	}
	outConfig := &producer_consumer.StreamSinkConfig[ntypes.AuctionIdSeller, *ntypes.AuctionBid]{
		FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
		MsgSerde:      outMsgSerde,
	}
	warmup := time.Duration(sp.WarmupS) * time.Second
	consumer1, err := producer_consumer.NewShardedSharedLogStreamConsumerG(stream1,
		src1Config, sp.NumSubstreamProducer[0], sp.ParNum)
	if err != nil {
		return nil, nil, err
	}
	consumer2, err := producer_consumer.NewShardedSharedLogStreamConsumerG(stream2,
		src2Config, sp.NumSubstreamProducer[1], sp.ParNum)
	if err != nil {
		return nil, nil, err
	}
	src1 := producer_consumer.NewMeteredConsumer(consumer1, warmup)
	src2 := producer_consumer.NewMeteredConsumer(consumer2, warmup)
	sink := producer_consumer.NewConcurrentMeteredSyncProducer(
		producer_consumer.NewShardedSharedLogStreamProducer(outputStream, outConfig), warmup)
	src1.SetInitialSource(false)
	src2.SetInitialSource(false)
	src1.SetName("aucsByIDSrc")
	src2.SetName("bidsByAucIDSrc")
	return []producer_consumer.MeteredConsumerIntr{src1, src2}, []producer_consumer.MeteredProducerIntr{sink}, nil
}

func (h *q6JoinStreamHandler) Q6JoinStream(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	srcs, sinks_arr, err := h.getSrcSink(ctx, sp)
	if err != nil {
		return &common.FnOutput{Success: false, Message: fmt.Sprintf("getSrcSink err: %v\n", err)}
	}
	jw, err := processor.NewJoinWindowsNoGrace(auctionDurationUpper)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	compare := concurrent_skiplist.CompareFunc(q8CompareFunc)
	joiner := processor.ValueJoinerWithKeyTsFunc(func(readOnlyKey, value1, value2 interface{}, leftTs, otherTs int64) interface{} {
		leftE := value1.(*ntypes.Event)
		auc := leftE.NewAuction
		rightE := value2.(*ntypes.Event)
		bid := rightE.Bid
		return &ntypes.AuctionBid{
			BidDateTime: bid.DateTime,
			BidPrice:    bid.Price,
			AucDateTime: auc.DateTime,
			AucExpires:  auc.Expires,
			AucSeller:   auc.Seller,
			AucCategory: auc.Category,
		}
	})
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	eventSerde, err := ntypes.GetEventSerdeG(serdeFormat)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	msgSerde, err := commtypes.GetMsgSerdeG[uint64](serdeFormat, commtypes.Uint64SerdeG{}, eventSerde)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	flushDur := time.Duration(sp.FlushMs) * time.Millisecond
	aucMp, err := store_with_changelog.NewMaterializeParamBuilder[uint64, *ntypes.Event]().
		MessageSerde(msgSerde).StoreName("auctionsByIDStore").
		ParNum(sp.ParNum).SerdeFormat(serdeFormat).
		ChangelogManagerParam(commtypes.CreateChangelogManagerParam{
			Env:           h.env,
			NumPartition:  sp.NumInPartition,
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
			NumPartition:  sp.NumInPartition,
			TimeOut:       common.SrcConsumeTimeout,
			FlushDuration: flushDur,
		}).Build()
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	aucJoinBidsFunc, bidsJoinAucFunc, wsc, err := execution.SetupStreamStreamJoin(
		aucMp, bidMp, compare, joiner, jw)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	getValidBid := processor.NewMeteredProcessor(
		processor.NewStreamFilterProcessor("getValidBid",
			processor.PredicateFunc(
				func(key, value interface{}) (bool, error) {
					ab := value.(*ntypes.AuctionBid)
					return ab.BidDateTime >= ab.AucDateTime && ab.BidDateTime <= ab.AucExpires, nil
				})))
	filterAndGroupMsg := func(ctx context.Context, msgs []commtypes.Message) ([]commtypes.Message, error) {
		var newMsgs []commtypes.Message
		for _, msg := range msgs {
			validBid, err := getValidBid.ProcessAndReturn(ctx, msg)
			if err != nil {
				return nil, err
			}
			if validBid != nil && !utils.IsNil(validBid[0].Value) {
				aic := ntypes.AuctionIdSeller{
					AucId:  validBid[0].Key.(uint64),
					Seller: validBid[0].Value.(*ntypes.AuctionBid).AucSeller,
				}
				aucBid := validBid[0].Value.(*ntypes.AuctionBid)
				newMsg := commtypes.Message{Key: aic, Value: aucBid, Timestamp: validBid[0].Timestamp}
				newMsgs = append(newMsgs, newMsg)
			}
		}
		return newMsgs, nil
	}
	aJoinB := execution.JoinWorkerFunc(func(ctx context.Context, m commtypes.Message) ([]commtypes.Message, error) {
		joined, err := aucJoinBidsFunc(ctx, m)
		if err != nil {
			return nil, err
		}
		return filterAndGroupMsg(ctx, joined)
	})

	bJoinA := execution.JoinWorkerFunc(func(ctx context.Context, m commtypes.Message) ([]commtypes.Message, error) {
		joined, err := bidsJoinAucFunc(ctx, m)
		if err != nil {
			return nil, err
		}
		return filterAndGroupMsg(ctx, joined)
	})
	task, procArgs := execution.PrepareTaskWithJoin(
		ctx, aJoinB, bJoinA, proc_interface.NewBaseSrcsSinks(srcs, sinks_arr),
		proc_interface.NewBaseProcArgs(h.funcName, sp.ScaleEpoch, sp.ParNum), false)
	transactionalID := fmt.Sprintf("%s-%s-%d", h.funcName,
		sp.InputTopicNames[0], sp.ParNum)
	streamTaskArgs := benchutil.UpdateStreamTaskArgs(sp,
		stream_task.NewStreamTaskArgsBuilder(h.env, procArgs, transactionalID)).
		WindowStoreChangelogs(wsc).FixedOutParNum(sp.ParNum).Build()
	return stream_task.ExecuteApp(ctx, task, streamTaskArgs)
}
