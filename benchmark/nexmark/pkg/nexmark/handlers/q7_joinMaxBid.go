package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/concurrent_skiplist"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/execution"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/source_sink"
	"sharedlog-stream/pkg/store_with_changelog"
	"sharedlog-stream/pkg/transaction"
	"sync"
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
	stream1 *sharedlog_stream.ShardedSharedLogStream,
	stream2 *sharedlog_stream.ShardedSharedLogStream,
	outputStream *sharedlog_stream.ShardedSharedLogStream,
) (*srcSinkSerde, error) {
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	msgSerde, err := commtypes.GetMsgSerde(serdeFormat)
	if err != nil {
		return nil, fmt.Errorf("get msg serde err: %v", err)
	}
	eventSerde, err := ntypes.GetEventSerde(serdeFormat)
	if err != nil {
		return nil, fmt.Errorf("get event serde err: %v", err)
	}
	seSerde, err := ntypes.GetStartEndTimeSerde(serdeFormat)
	if err != nil {
		return nil, err
	}
	bmSerde, err := ntypes.GetBidAndMaxSerde(serdeFormat)
	if err != nil {
		return nil, err
	}
	timeout := common.SrcConsumeTimeout
	warmup := time.Duration(sp.WarmupS) * time.Second
	src1 := source_sink.NewMeteredSource(source_sink.NewShardedSharedLogStreamSource(stream1, &source_sink.StreamSourceConfig{
		KVMsgSerdes: commtypes.KVMsgSerdes{
			MsgSerde: msgSerde,
			KeySerde: commtypes.Uint64Serde{},
			ValSerde: eventSerde,
		},
		Timeout: timeout,
	}), warmup)
	src2 := source_sink.NewMeteredSource(source_sink.NewShardedSharedLogStreamSource(stream2, &source_sink.StreamSourceConfig{
		KVMsgSerdes: commtypes.KVMsgSerdes{
			MsgSerde: msgSerde,
			KeySerde: commtypes.Uint64Serde{},
			ValSerde: seSerde,
		},
		Timeout: timeout,
	}), warmup)
	sink := source_sink.NewConcurrentMeteredSyncSink(source_sink.NewShardedSharedLogStreamSyncSink(outputStream, &source_sink.StreamSinkConfig{
		KVMsgSerdes: commtypes.KVMsgSerdes{
			MsgSerde: msgSerde,
			KeySerde: commtypes.Uint64Serde{},
			ValSerde: bmSerde,
		},
	}), warmup)
	return &srcSinkSerde{src1: src1, src2: src2, sink: sink}, nil
}

func (h *q7JoinMaxBid) q7JoinMaxBid(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	bidByPriceStream, maxBidByPrice, outputStream, err := getInOutStreams(ctx, h.env, sp)
	if err != nil {
		return &common.FnOutput{Success: false, Message: fmt.Sprintf("get input output err: %v", err)}
	}
	sss, err := h.getSrcSink(ctx, sp, bidByPriceStream, maxBidByPrice, outputStream)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	jw, err := processor.NewJoinWindowsWithGrace(time.Duration(10)*time.Second, time.Duration(5)*time.Second)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	compare := concurrent_skiplist.CompareFunc(q8CompareFunc)
	joiner := processor.ValueJoinerWithKeyTsFunc(func(readOnlyKey, value1, value2 interface{},
		leftTs, otherTs int64) interface{} {
		fmt.Fprintf(os.Stderr, "val1: %v, val2: %v\n", value1, value2)
		lv := value1.(*ntypes.Event)
		rv := value2.(*ntypes.StartEndTime)
		st := leftTs
		if st > otherTs {
			st = otherTs
		}
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
	warmup := time.Duration(sp.WarmupS) * time.Second
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	bMp, err := store_with_changelog.NewMaterializeParamBuilder().
		KVMsgSerdes(sss.src1.KVMsgSerdes()).
		StoreName("bidByPriceTab").
		ParNum(sp.ParNum).
		SerdeFormat(serdeFormat).StreamParam(commtypes.CreateStreamParam{
		Env:          h.env,
		NumPartition: sp.NumInPartition,
	}).Build()
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	maxBMp, err := store_with_changelog.NewMaterializeParamBuilder().
		KVMsgSerdes(sss.src2.KVMsgSerdes()).
		StoreName("maxBidByPriceTab").
		ParNum(sp.ParNum).
		SerdeFormat(serdeFormat).StreamParam(commtypes.CreateStreamParam{
		Env:          h.env,
		NumPartition: sp.NumInPartition,
	}).Build()
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	bJoinMaxBFunc, maxBJoinBFunc, procs, wsc, err := execution.SetupStreamStreamJoin(bMp, maxBMp, compare, joiner, jw, warmup)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}

	filter := processor.NewMeteredProcessor(processor.NewStreamFilterProcessor(processor.PredicateFunc(func(m *commtypes.Message) (bool, error) {
		val := m.Value.(*ntypes.BidAndMax)
		return val.Timestamp >= val.WStartMs && val.Timestamp <= val.WEndMs, nil
	})), warmup)

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
	debug.Assert(sp.ScaleEpoch != 0, "scale epoch should start from 1")

	joinProcBid := execution.NewJoinProcArgs(sss.src1, sss.sink, bJoinM,
		h.funcName, sp.ScaleEpoch, sp.ParNum)
	joinProcMaxBid := execution.NewJoinProcArgs(sss.src2, sss.sink, mJoinB,
		h.funcName, sp.ScaleEpoch, sp.ParNum)
	var wg sync.WaitGroup
	bidManager := execution.NewJoinProcManager()
	maxBidManager := execution.NewJoinProcManager()

	procArgs := execution.NewCommonJoinProcArgs(joinProcBid, joinProcMaxBid,
		bidManager.Out(), maxBidManager.Out(),
		h.funcName, sp.ScaleEpoch, sp.ParNum)
	bctx := context.WithValue(ctx, "id", "bid")
	mctx := context.WithValue(ctx, "id", "maxBid")

	task := transaction.NewStreamTaskBuilder().
		AppProcessFunc(func(ctx context.Context, task *transaction.StreamTask, args interface{}) *common.FnOutput {
			return execution.HandleJoinErrReturn(args)
		}).InitFunc(func(progArgs interface{}) {
		sss.src1.StartWarmup()
		sss.src2.StartWarmup()
		for _, proc := range procs {
			proc.StartWarmup()
		}

		bidManager.Run()
		maxBidManager.Run()
	}).PauseFunc(func() *common.FnOutput {
		bidManager.RequestToTerminate()
		maxBidManager.RequestToTerminate()
		wg.Wait()
		if ret := execution.HandleJoinErrReturn(procArgs); ret != nil {
			return ret
		}
		return nil
	}).ResumeFunc(func(task *transaction.StreamTask) {
		bidManager.LaunchJoinProcLoop(bctx, task, joinProcBid, &wg)
		maxBidManager.LaunchJoinProcLoop(mctx, task, joinProcMaxBid, &wg)

		bidManager.Run()
		maxBidManager.Run()
	}).Build()
	bidManager.LaunchJoinProcLoop(bctx, task, joinProcBid, &wg)
	maxBidManager.LaunchJoinProcLoop(mctx, task, joinProcMaxBid, &wg)

	srcs := []source_sink.Source{sss.src1, sss.src2}
	sinks_arr := []source_sink.Sink{sss.sink}
	update_stats := func(ret *common.FnOutput) {
		for proc_name, proc := range procs {
			ret.Latencies[proc_name] = proc.GetLatency()
		}
		ret.Latencies["eventTimeLatency"] = sss.sink.GetEventTimeLatency()
		ret.Counts["bidByPriceSrc"] = sss.src1.GetCount()
		ret.Counts["maxBidSrc"] = sss.src2.GetCount()
		ret.Counts["sink"] = sss.sink.GetCount()
	}
	if sp.EnableTransaction {
		transactionalID := fmt.Sprintf("%s-%d", h.funcName, sp.ParNum)
		streamTaskArgs := benchutil.UpdateStreamTaskArgsTransaction(sp,
			transaction.NewStreamTaskArgsTransactionBuilder().
				ProcArgs(procArgs).
				Env(h.env).
				Srcs(srcs).
				Sinks(sinks_arr).
				TransactionalID(transactionalID)).
			WindowStoreChangelogs(wsc).
			FixedOutParNum(sp.ParNum).
			Build()
		ret := transaction.SetupManagersAndProcessTransactional(ctx, h.env, streamTaskArgs, task)
		if ret != nil && ret.Success {
			update_stats(ret)
		}
		return ret
	} else {
		streamTaskArgs := transaction.NewStreamTaskArgs(h.env, procArgs, srcs, sinks_arr).
			WithWindowStoreChangelogs(wsc)
		benchutil.UpdateStreamTaskArgs(sp, streamTaskArgs)
		ret := task.Process(ctx, streamTaskArgs)
		if ret != nil && ret.Success {
			update_stats(ret)
		}
		return ret
	}
}
