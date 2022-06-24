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
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/execution"
	"sharedlog-stream/pkg/proc_interface"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/producer_consumer"
	"sharedlog-stream/pkg/store_with_changelog"
	"sharedlog-stream/pkg/stream_task"
	"time"

	"cs.utexas.edu/zjia/faas/types"
)

type q7JoinMaxBid struct {
	env      types.Environment
	funcName string
}

func NewQ7JoinMaxBid(env types.Environment, funcName string) types.FuncHandler {
	return &q7JoinMaxBid{
		env:      env,
		funcName: funcName,
	}
}

func (h *q7JoinMaxBid) Call(ctx context.Context, input []byte) ([]byte, error) {
	sp := &common.QueryInput{}
	err := json.Unmarshal(input, sp)
	if err != nil {
		return nil, err
	}
	output := h.q7JoinMaxBid(ctx, sp)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		return nil, err
	}
	return utils.CompressData(encodedOutput), nil
}

func (h *q7JoinMaxBid) getSrcSink(
	ctx context.Context,
	sp *common.QueryInput,
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
	seSerde, err := ntypes.GetStartEndTimeSerde(serdeFormat)
	if err != nil {
		return nil, nil, err
	}
	bmSerde, err := ntypes.GetBidAndMaxSerde(serdeFormat)
	if err != nil {
		return nil, nil, err
	}
	timeout := common.SrcConsumeTimeout
	warmup := time.Duration(sp.WarmupS) * time.Second
	src1 := producer_consumer.NewMeteredConsumer(producer_consumer.NewShardedSharedLogStreamConsumer(stream1, &producer_consumer.StreamConsumerConfig{
		KVMsgSerdes: commtypes.KVMsgSerdes{
			MsgSerde: msgSerde,
			KeySerde: commtypes.Uint64Serde{},
			ValSerde: eventSerde,
		},
		Timeout: timeout,
	}), warmup)
	src2 := producer_consumer.NewMeteredConsumer(producer_consumer.NewShardedSharedLogStreamConsumer(stream2, &producer_consumer.StreamConsumerConfig{
		KVMsgSerdes: commtypes.KVMsgSerdes{
			MsgSerde: msgSerde,
			KeySerde: commtypes.Uint64Serde{},
			ValSerde: seSerde,
		},
		Timeout: timeout,
	}), warmup)
	sink := producer_consumer.NewConcurrentMeteredSyncProducer(producer_consumer.NewShardedSharedLogStreamProducer(outputStream, &producer_consumer.StreamSinkConfig{
		KVMsgSerdes: commtypes.KVMsgSerdes{
			MsgSerde: msgSerde,
			KeySerde: commtypes.Uint64Serde{},
			ValSerde: bmSerde,
		},
	}), warmup)
	return []producer_consumer.MeteredConsumerIntr{src1, src2}, []producer_consumer.MeteredProducerIntr{sink}, nil
}

func (h *q7JoinMaxBid) q7JoinMaxBid(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	srcs, sinks_arr, err := h.getSrcSink(ctx, sp)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	debug.Assert(sp.ScaleEpoch != 0, "scale epoch should start from 1")
	jw, err := processor.NewJoinWindowsWithGrace(time.Duration(10)*time.Second, time.Duration(5)*time.Second)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	compare := concurrent_skiplist.CompareFunc(q8CompareFunc)
	joiner := processor.ValueJoinerWithKeyTsFunc(func(readOnlyKey, value1, value2 interface{},
		leftTs, otherTs int64) interface{} {
		// fmt.Fprintf(os.Stderr, "val1: %v, val2: %v\n", value1, value2)
		lv := value1.(*ntypes.Event)
		rv := value2.(*ntypes.StartEndTime)
		return &ntypes.BidAndMax{
			Price:    lv.Bid.Price,
			Auction:  lv.Bid.Auction,
			Bidder:   lv.Bid.Bidder,
			WStartMs: rv.StartTimeMs,
			WEndMs:   rv.EndTimeMs,
			BaseTs: ntypes.BaseTs{
				Timestamp: lv.Bid.DateTime,
			},
		}
	})
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	flushDur := time.Duration(sp.FlushMs) * time.Millisecond
	bMp, err := store_with_changelog.NewMaterializeParamBuilder().
		KVMsgSerdes(srcs[0].KVMsgSerdes()).
		StoreName("bidByPriceTab").
		ParNum(sp.ParNum).
		SerdeFormat(serdeFormat).StreamParam(commtypes.CreateStreamParam{
		Env:          h.env,
		NumPartition: sp.NumInPartition,
	}).BuildForKVStore(flushDur, common.SrcConsumeTimeout)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	maxBMp, err := store_with_changelog.NewMaterializeParamBuilder().
		KVMsgSerdes(srcs[1].KVMsgSerdes()).
		StoreName("maxBidByPriceTab").
		ParNum(sp.ParNum).
		SerdeFormat(serdeFormat).StreamParam(commtypes.CreateStreamParam{
		Env:          h.env,
		NumPartition: sp.NumInPartition,
	}).BuildForKVStore(flushDur, common.SrcConsumeTimeout)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	bJoinMaxBFunc, maxBJoinBFunc, wsc, err := execution.SetupStreamStreamJoin(bMp, maxBMp, compare, joiner, jw)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}

	filter := processor.NewMeteredProcessor(processor.NewStreamFilterProcessor(
		"filter", processor.PredicateFunc(func(m *commtypes.Message) (bool, error) {
			val := m.Value.(*ntypes.BidAndMax)
			return val.Timestamp >= val.WStartMs && val.Timestamp <= val.WEndMs, nil
		})))

	var bJoinM execution.JoinWorkerFunc = func(c context.Context, m commtypes.Message) ([]commtypes.Message, error) {
		joined, err := bJoinMaxBFunc(ctx, m)
		if err != nil {
			return nil, err
		}
		var outMsgs []commtypes.Message
		for _, jmsg := range joined {
			filtered, err := filter.ProcessAndReturn(ctx, jmsg)
			if err != nil {
				return nil, err
			}
			if filtered != nil {
				outMsgs = append(outMsgs, filtered...)
			}
		}
		return outMsgs, nil
	}
	var mJoinB execution.JoinWorkerFunc = func(c context.Context, m commtypes.Message) ([]commtypes.Message, error) {
		joined, err := maxBJoinBFunc(ctx, m)
		if err != nil {
			return nil, err
		}
		var outMsgs []commtypes.Message
		for _, jmsg := range joined {
			filtered, err := filter.ProcessAndReturn(ctx, jmsg)
			if err != nil {
				return nil, err
			}
			if filtered != nil {
				outMsgs = append(outMsgs, filtered...)
			}
		}
		return outMsgs, nil

	}
	task, procArgs := execution.PrepareTaskWithJoin(
		ctx, bJoinM, mJoinB, proc_interface.NewBaseSrcsSinks(srcs, sinks_arr),
		proc_interface.NewBaseProcArgs(h.funcName, sp.ScaleEpoch, sp.ParNum),
	)
	streamTaskArgs := benchutil.UpdateStreamTaskArgs(sp,
		stream_task.NewStreamTaskArgsBuilder(h.env, procArgs,
			fmt.Sprintf("%s-%d", h.funcName, sp.ParNum))).
		WindowStoreChangelogs(wsc).FixedOutParNum(sp.ParNum).Build()
	return task.ExecuteApp(ctx, streamTaskArgs)
}
