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
	"sharedlog-stream/pkg/concurrent_skiplist"
	"sharedlog-stream/pkg/execution"
	"sharedlog-stream/pkg/proc_interface"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/producer_consumer"
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
	output := h.Q4JoinTable(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	// fmt.Printf("query 3 output: %v\n", encodedOutput)
	return utils.CompressData(encodedOutput), nil
}

func (h *q4JoinStreamHandler) process(ctx context.Context,
	t *stream_task.StreamTask,
	argsTmp interface{},
) *common.FnOutput {
	return execution.HandleJoinErrReturn(argsTmp)
}

func (h *q4JoinStreamHandler) getSrcSink(ctx context.Context, sp *common.QueryInput,
) ([]producer_consumer.MeteredConsumerIntr, []producer_consumer.MeteredProducerIntr, error) {
	stream1, stream2, outputStream, err := getInOutStreams(ctx, h.env, sp)
	if err != nil {
		return nil, nil, err
	}
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	msgSerde, err := commtypes.GetMsgSerde(serdeFormat)
	if err != nil {
		return nil, nil, fmt.Errorf("get msg serde err: %v", err)
	}

	eventSerde, err := ntypes.GetEventSerde(serdeFormat)
	if err != nil {
		return nil, nil, fmt.Errorf("get event serde err: %v", err)
	}
	kvmsgSerdes := commtypes.KVMsgSerdes{
		KeySerde: commtypes.Uint64Serde{},
		ValSerde: eventSerde,
		MsgSerde: msgSerde,
	}
	auctionsConfig := &producer_consumer.StreamConsumerConfig{
		Timeout:     common.SrcConsumeTimeout,
		KVMsgSerdes: kvmsgSerdes,
	}
	personsConfig := &producer_consumer.StreamConsumerConfig{
		Timeout:     common.SrcConsumeTimeout,
		KVMsgSerdes: kvmsgSerdes,
	}
	var abSerde commtypes.Serde
	var aicSerde commtypes.Serde
	if sp.SerdeFormat == uint8(commtypes.JSON) {
		abSerde = ntypes.AuctionBidJSONSerde{}
		aicSerde = ntypes.AuctionIdCategoryJSONSerde{}
	} else {
		abSerde = ntypes.AuctionBidMsgpSerde{}
		aicSerde = ntypes.AuctionIdCategoryMsgpSerde{}
	}
	outConfig := &producer_consumer.StreamSinkConfig{
		KVMsgSerdes: commtypes.KVMsgSerdes{
			KeySerde: aicSerde,
			ValSerde: abSerde,
			MsgSerde: msgSerde,
		},
		FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
	}
	warmup := time.Duration(sp.WarmupS) * time.Second
	src1 := producer_consumer.NewMeteredConsumer(producer_consumer.NewShardedSharedLogStreamConsumer(stream1, auctionsConfig),
		warmup)
	src2 := producer_consumer.NewMeteredConsumer(producer_consumer.NewShardedSharedLogStreamConsumer(stream2, personsConfig),
		warmup)
	sink := producer_consumer.NewConcurrentMeteredSyncProducer(producer_consumer.NewShardedSharedLogStreamProducer(outputStream, outConfig),
		time.Duration(sp.WarmupS)*time.Second)
	src1.SetInitialSource(false)
	src2.SetInitialSource(false)
	return []producer_consumer.MeteredConsumerIntr{src1, src2}, []producer_consumer.MeteredProducerIntr{sink}, nil
}

func (h *q4JoinStreamHandler) Q4JoinTable(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
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
			AucCategory: auc.Category,
		}
	})
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	flushDur := time.Duration(sp.FlushMs) * time.Millisecond
	aucMp, err := store_with_changelog.NewMaterializeParamBuilder().
		KVMsgSerdes(srcs[0].KVMsgSerdes()).StoreName("auctionsByIDStore").
		ParNum(sp.ParNum).SerdeFormat(serdeFormat).StreamParam(commtypes.CreateStreamParam{
		Env:          h.env,
		NumPartition: sp.NumInPartition,
	}).Build(flushDur, common.SrcConsumeTimeout)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	bidMp, err := store_with_changelog.NewMaterializeParamBuilder().
		KVMsgSerdes(srcs[1].KVMsgSerdes()).StoreName("bidsByAuctionIDStore").
		ParNum(sp.ParNum).SerdeFormat(serdeFormat).StreamParam(commtypes.CreateStreamParam{
		Env:          h.env,
		NumPartition: sp.NumInPartition,
	}).Build(flushDur, common.SrcConsumeTimeout)
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
				func(msg *commtypes.Message) (bool, error) {
					ab := msg.Value.(*ntypes.AuctionBid)
					return ab.BidDateTime >= ab.AucDateTime && ab.BidDateTime <= ab.AucExpires, nil
				})))

	filterAndGroupMsg := func(ctx context.Context, msgs []commtypes.Message) ([]commtypes.Message, error) {
		var newMsgs []commtypes.Message
		for _, msg := range msgs {
			validBid, err := getValidBid.ProcessAndReturn(ctx, msg)
			if err != nil {
				return nil, err
			}
			if validBid != nil {
				aic := ntypes.AuctionIdCategory{
					AucId:    validBid[0].Key.(uint64),
					Category: validBid[0].Value.(*ntypes.AuctionBid).AucCategory,
				}
				newMsg := commtypes.Message{Key: &aic, Value: validBid[0].Value, Timestamp: validBid[0].Timestamp}
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

	bJoinA := execution.JoinWorkerFunc(func(c context.Context, m commtypes.Message) ([]commtypes.Message, error) {
		joined, err := bidsJoinAucFunc(ctx, m)
		if err != nil {
			return nil, err
		}
		return filterAndGroupMsg(ctx, joined)
	})
	task, procArgs := execution.PrepareTaskWithJoin(
		ctx, aJoinB, bJoinA, proc_interface.NewBaseSrcsSinks(srcs, sinks_arr),
		proc_interface.NewBaseProcArgs(h.funcName, sp.ScaleEpoch, sp.ParNum),
	)
	transactionalID := fmt.Sprintf("%s-%s-%d", h.funcName,
		sp.InputTopicNames[0], sp.ParNum)
	streamTaskArgs := benchutil.UpdateStreamTaskArgs(sp,
		stream_task.NewStreamTaskArgsBuilder(h.env, procArgs, transactionalID)).
		WindowStoreChangelogs(wsc).FixedOutParNum(sp.ParNum).Build()
	return task.ExecuteApp(ctx, streamTaskArgs)
}
