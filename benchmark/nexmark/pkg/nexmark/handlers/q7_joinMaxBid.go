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
	"sharedlog-stream/pkg/execution"
	"sharedlog-stream/pkg/proc_interface"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/producer_consumer"
	"sharedlog-stream/pkg/store"
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
	return common.CompressData(encodedOutput), nil
}

func (h *q7JoinMaxBid) getSrcSink(
	ctx context.Context,
	sp *common.QueryInput,
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
	seSerde, err := ntypes.GetStartEndTimeSerdeG(serdeFormat)
	if err != nil {
		return nil, nil, err
	}
	inMsgSerde1, err := commtypes.GetMsgSerdeG[uint64](serdeFormat, commtypes.Uint64SerdeG{}, eventSerde)
	if err != nil {
		return nil, nil, fmt.Errorf("get msg serde err: %v", err)
	}
	inMsgSerde2, err := commtypes.GetMsgSerdeG[uint64](serdeFormat, commtypes.Uint64SerdeG{}, seSerde)
	if err != nil {
		return nil, nil, fmt.Errorf("get msg serde err: %v", err)
	}
	bmSerde, err := ntypes.GetBidAndMaxSerdeG(serdeFormat)
	if err != nil {
		return nil, nil, err
	}
	outMsgSerde, err := commtypes.GetMsgSerdeG[uint64](serdeFormat, commtypes.Uint64SerdeG{}, bmSerde)
	if err != nil {
		return nil, nil, err
	}
	timeout := time.Duration(10) * time.Millisecond
	warmup := time.Duration(sp.WarmupS) * time.Second
	consumer1, err := producer_consumer.NewShardedSharedLogStreamConsumerG(stream1,
		&producer_consumer.StreamConsumerConfigG[uint64, *ntypes.Event]{
			MsgSerde:    inMsgSerde1,
			Timeout:     timeout,
			SerdeFormat: serdeFormat,
		}, sp.NumSubstreamProducer[0], sp.ParNum)
	if err != nil {
		return nil, nil, err
	}
	consumer2, err := producer_consumer.NewShardedSharedLogStreamConsumerG(stream2,
		&producer_consumer.StreamConsumerConfigG[uint64, ntypes.StartEndTime]{
			MsgSerde:    inMsgSerde2,
			Timeout:     timeout,
			SerdeFormat: serdeFormat,
		}, sp.NumSubstreamProducer[1], sp.ParNum)
	if err != nil {
		return nil, nil, err
	}
	src1 := producer_consumer.NewMeteredConsumer(consumer1, warmup)
	src1.SetName("bidByPriceSrc")
	src2 := producer_consumer.NewMeteredConsumer(consumer2, warmup)
	src2.SetName("maxBidsWithWinSrc")
	sink := producer_consumer.NewConcurrentMeteredSyncProducer(producer_consumer.NewShardedSharedLogStreamProducer(outputStream,
		&producer_consumer.StreamSinkConfig[uint64, ntypes.BidAndMax]{
			MsgSerde:      outMsgSerde,
			FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
		}), warmup)
	src1.SetInitialSource(false)
	src2.SetInitialSource(false)
	sink.MarkFinalOutput()
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
	joiner := processor.ValueJoinerWithKeyTsFunc(func(readOnlyKey, value1, value2 interface{},
		leftTs, otherTs int64) interface{} {
		// fmt.Fprintf(os.Stderr, "val1: %v, val2: %v\n", value1, value2)
		lv := value1.(*ntypes.Event)
		rv := value2.(ntypes.StartEndTime)
		return ntypes.BidAndMax{
			Price:    lv.Bid.Price,
			Auction:  lv.Bid.Auction,
			Bidder:   lv.Bid.Bidder,
			BidTs:    lv.Bid.DateTime,
			WStartMs: rv.StartTimeMs,
			WEndMs:   rv.EndTimeMs,
		}
	})
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	flushDur := time.Duration(sp.FlushMs) * time.Millisecond
	eventSerde, err := ntypes.GetEventSerdeG(serdeFormat)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	seSerde, err := ntypes.GetStartEndTimeSerdeG(serdeFormat)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	inMsgSerde1, err := commtypes.GetMsgSerdeG[uint64](serdeFormat, commtypes.Uint64SerdeG{}, eventSerde)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	inMsgSerde2, err := commtypes.GetMsgSerdeG[uint64](serdeFormat, commtypes.Uint64SerdeG{}, seSerde)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	bMp, err := store_with_changelog.NewMaterializeParamBuilder[uint64, *ntypes.Event]().
		MessageSerde(inMsgSerde1).
		StoreName("bidByPriceTab").
		ParNum(sp.ParNum).
		SerdeFormat(serdeFormat).
		ChangelogManagerParam(commtypes.CreateChangelogManagerParam{
			Env:           h.env,
			NumPartition:  sp.NumInPartition,
			FlushDuration: flushDur,
			TimeOut:       common.SrcConsumeTimeout,
		}).Build()
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	maxBMp, err := store_with_changelog.NewMaterializeParamBuilder[uint64, ntypes.StartEndTime]().
		MessageSerde(inMsgSerde2).
		StoreName("maxBidByPriceTab").
		ParNum(sp.ParNum).
		SerdeFormat(serdeFormat).
		ChangelogManagerParam(commtypes.CreateChangelogManagerParam{
			Env:           h.env,
			NumPartition:  sp.NumInPartition,
			FlushDuration: flushDur,
			TimeOut:       common.SrcConsumeTimeout,
		}).Build()
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	bJoinMaxBFunc, maxBJoinBFunc, wsc, err := execution.SetupStreamStreamJoin(bMp, maxBMp,
		store.Uint64IntrCompare, joiner, jw)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}

	filter := processor.NewMeteredProcessor(processor.NewStreamFilterProcessor(
		"filter", processor.PredicateFunc(func(key, value interface{}) (bool, error) {
			val := value.(ntypes.BidAndMax)
			return val.BidTs >= val.WStartMs && val.BidTs <= val.WEndMs, nil
		})))

	var bJoinM execution.JoinWorkerFunc = func(ctx context.Context, m commtypes.Message) ([]commtypes.Message, error) {
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
	var mJoinB execution.JoinWorkerFunc = func(ctx context.Context, m commtypes.Message) ([]commtypes.Message, error) {
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
		proc_interface.NewBaseProcArgs(h.funcName, sp.ScaleEpoch, sp.ParNum), true)
	streamTaskArgs := benchutil.UpdateStreamTaskArgs(sp,
		stream_task.NewStreamTaskArgsBuilder(h.env, procArgs,
			fmt.Sprintf("%s-%d", h.funcName, sp.ParNum))).
		WindowStoreChangelogs(wsc).FixedOutParNum(sp.ParNum).Build()
	return stream_task.ExecuteApp(ctx, task, streamTaskArgs)
}
