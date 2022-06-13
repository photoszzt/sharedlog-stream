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
	"sharedlog-stream/pkg/execution"
	"sharedlog-stream/pkg/proc_interface"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/source_sink"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/store_restore"
	"sharedlog-stream/pkg/store_with_changelog"
	"sharedlog-stream/pkg/stream_task"
	"sharedlog-stream/pkg/treemap"
	"sync"
	"time"

	"cs.utexas.edu/zjia/faas/types"
)

type q4JoinTableHandler struct {
	env      types.Environment
	funcName string
}

func NewQ4JoinTableHandler(env types.Environment, funcName string) types.FuncHandler {
	return &q4JoinTableHandler{
		env:      env,
		funcName: funcName,
	}
}

func (h *q4JoinTableHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
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

func (h *q4JoinTableHandler) process(ctx context.Context,
	t *stream_task.StreamTask,
	argsTmp interface{},
) *common.FnOutput {
	return execution.HandleJoinErrReturn(argsTmp)
}

func (h *q4JoinTableHandler) getSrcSink(ctx context.Context, sp *common.QueryInput,
) (*source_sink.MeteredSource, /* src1 */
	*source_sink.MeteredSource, /* src2 */
	*source_sink.ConcurrentMeteredSyncSink,
	error,
) {
	stream1, stream2, outputStream, err := getInOutStreams(ctx, h.env, sp)
	if err != nil {
		return nil, nil, nil, err
	}
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	msgSerde, err := commtypes.GetMsgSerde(serdeFormat)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("get msg serde err: %v", err)
	}

	eventSerde, err := ntypes.GetEventSerde(serdeFormat)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("get event serde err: %v", err)
	}
	kvmsgSerdes := commtypes.KVMsgSerdes{
		KeySerde: commtypes.Uint64Serde{},
		ValSerde: eventSerde,
		MsgSerde: msgSerde,
	}
	auctionsConfig := &source_sink.StreamSourceConfig{
		Timeout:     common.SrcConsumeTimeout,
		KVMsgSerdes: kvmsgSerdes,
	}
	personsConfig := &source_sink.StreamSourceConfig{
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
	outConfig := &source_sink.StreamSinkConfig{
		KVMsgSerdes: commtypes.KVMsgSerdes{
			KeySerde: aicSerde,
			ValSerde: abSerde,
			MsgSerde: msgSerde,
		},
		FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
	}
	warmup := time.Duration(sp.WarmupS) * time.Second
	src1 := source_sink.NewMeteredSource(source_sink.NewShardedSharedLogStreamSource(stream1, auctionsConfig),
		warmup)
	src2 := source_sink.NewMeteredSource(source_sink.NewShardedSharedLogStreamSource(stream2, personsConfig),
		warmup)
	sink := source_sink.NewConcurrentMeteredSyncSink(source_sink.NewShardedSharedLogStreamSyncSink(outputStream, outConfig),
		time.Duration(sp.WarmupS)*time.Second)
	src1.SetInitialSource(false)
	src2.SetInitialSource(false)
	return src1, src2, sink, nil
}

func (h *q4JoinTableHandler) Q4JoinTable(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	auctionsSrc, bidsSrc, sink, err := h.getSrcSink(ctx, sp)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("getSrcSink err: %v\n", err),
		}
	}
	compare := func(a, b treemap.Key) int {
		valA := a.(uint64)
		valB := b.(uint64)
		if valA < valB {
			return -1
		} else if valA == valB {
			return 0
		} else {
			return 1
		}
	}
	toAuctionsTable, auctionsStore := processor.ToInMemKVTable("auctionsByIDStore", compare,
		time.Duration(sp.WarmupS)*time.Second)
	toBidsTable, bidsStore := processor.ToInMemKVTable("bidsByAuctionIDStore", compare,
		time.Duration(sp.WarmupS)*time.Second)
	joiner := processor.ValueJoinerWithKeyFunc(func(readOnlyKey interface{}, leftVal interface{}, rightVal interface{}) interface{} {
		leftE := leftVal.(*ntypes.Event)
		auc := leftE.NewAuction
		rightE := rightVal.(*ntypes.Event)
		bid := rightE.Bid
		return &ntypes.AuctionBid{
			BidDateTime: bid.DateTime,
			BidPrice:    bid.Price,
			AucDateTime: auc.DateTime,
			AucExpires:  auc.Expires,
			AucCategory: auc.Category,
		}
	})

	auctionsJoinBids := processor.NewMeteredProcessor(
		processor.NewTableTableJoinProcessor(bidsStore.Name(), bidsStore, joiner),
		time.Duration(sp.WarmupS)*time.Second)
	bidsJoinAuctions := processor.NewMeteredProcessor(
		processor.NewTableTableJoinProcessor(auctionsStore.Name(), auctionsStore, joiner),
		time.Duration(sp.WarmupS)*time.Second)
	getValidBid := processor.NewMeteredProcessor(
		processor.NewStreamFilterProcessor(
			processor.PredicateFunc(
				func(msg *commtypes.Message) (bool, error) {
					ab := msg.Value.(*ntypes.AuctionBid)
					return ab.BidDateTime >= ab.AucDateTime && ab.BidDateTime <= ab.AucExpires, nil
				})),
		time.Duration(sp.WarmupS)*time.Second,
	)

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
		_, err := toAuctionsTable.ProcessAndReturn(ctx, m)
		if err != nil {
			return nil, err
		}
		msgs, err := auctionsJoinBids.ProcessAndReturn(ctx, m)
		if err != nil {
			return nil, err
		}
		return filterAndGroupMsg(ctx, msgs)
	})

	bJoinA := execution.JoinWorkerFunc(func(c context.Context, m commtypes.Message) ([]commtypes.Message, error) {
		_, err := toBidsTable.ProcessAndReturn(ctx, m)
		if err != nil {
			return nil, err
		}
		msgs, err := bidsJoinAuctions.ProcessAndReturn(ctx, m)
		if err != nil {
			return nil, err
		}
		return filterAndGroupMsg(ctx, msgs)
	})
	srcs := []source_sink.Source{auctionsSrc, bidsSrc}
	sinks_arr := []source_sink.Sink{sink}
	joinProcAuction, joinProcBid := execution.CreateJoinProcArgsPair(
		aJoinB, bJoinA, srcs, sinks_arr,
		proc_interface.NewBaseProcArgs(h.funcName, sp.ScaleEpoch, sp.ParNum))

	var wg sync.WaitGroup
	aucManager := execution.NewJoinProcManager()
	bidManager := execution.NewJoinProcManager()

	procArgs := execution.NewCommonJoinProcArgs(
		joinProcAuction, joinProcBid,
		aucManager.Out(), bidManager.Out(),
		proc_interface.NewBaseSrcsSinks(srcs, sinks_arr))

	bctx := context.WithValue(ctx, "id", "bid")
	actx := context.WithValue(ctx, "id", "auction")

	task := stream_task.NewStreamTaskBuilder().
		AppProcessFunc(h.process).
		InitFunc(func(progArgs interface{}) {
			auctionsSrc.StartWarmup()
			bidsSrc.StartWarmup()
			sink.StartWarmup()
			toAuctionsTable.StartWarmup()
			toBidsTable.StartWarmup()

			aucManager.Run()
			bidManager.Run()
		}).
		PauseFunc(func() *common.FnOutput {
			aucManager.RequestToTerminate()
			bidManager.RequestToTerminate()
			wg.Wait()
			ret := execution.HandleJoinErrReturn(procArgs)
			if ret != nil {
				return ret
			}
			return nil
		}).
		ResumeFunc(func(task *stream_task.StreamTask) {
			aucManager.LaunchJoinProcLoop(actx, task, joinProcAuction, &wg)
			bidManager.LaunchJoinProcLoop(bctx, task, joinProcBid, &wg)

			aucManager.Run()
			bidManager.Run()
		}).Build()
	aucManager.LaunchJoinProcLoop(actx, task, joinProcAuction, &wg)
	bidManager.LaunchJoinProcLoop(bctx, task, joinProcBid, &wg)

	var kvchangelogs []*store_restore.KVStoreChangelog
	if sp.TableType == uint8(store.IN_MEM) {
		serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
		kvchangelogs = []*store_restore.KVStoreChangelog{
			store_restore.NewKVStoreChangelog(auctionsStore,
				store_with_changelog.NewChangelogManager(auctionsSrc.Stream().(*sharedlog_stream.ShardedSharedLogStream),
					serdeFormat),
				auctionsSrc.KVMsgSerdes(), sp.ParNum,
			),
			store_restore.NewKVStoreChangelog(bidsStore,
				store_with_changelog.NewChangelogManager(bidsSrc.Stream().(*sharedlog_stream.ShardedSharedLogStream),
					serdeFormat),
				bidsSrc.KVMsgSerdes(), sp.ParNum,
			),
		}
	} else if sp.TableType == uint8(store.MONGODB) {
		kvchangelogs = []*store_restore.KVStoreChangelog{
			store_restore.NewKVStoreChangelogForExternalStore(auctionsStore, auctionsSrc.Stream(), execution.JoinProcSerialWithoutSink,
				execution.NewJoinProcWithoutSinkArgs(auctionsSrc.InnerSource(), aJoinB, sp.ParNum),
				fmt.Sprintf("%s-%s-%d", h.funcName, auctionsStore.Name(), sp.ParNum), sp.ParNum),
			store_restore.NewKVStoreChangelogForExternalStore(bidsStore, bidsSrc.Stream(), execution.JoinProcSerialWithoutSink,
				execution.NewJoinProcWithoutSinkArgs(bidsSrc.InnerSource(), bJoinA, sp.ParNum),
				fmt.Sprintf("%s-%s-%d", h.funcName, bidsStore.Name(), sp.ParNum), sp.ParNum),
		}
	}
	update_stats := func(ret *common.FnOutput) {
		ret.Latencies["toAuctionsTable"] = toAuctionsTable.GetLatency()
		ret.Latencies["toBidsTable"] = toBidsTable.GetLatency()
		ret.Latencies["bidsJoinAuctions"] = bidsJoinAuctions.GetLatency()
		ret.Latencies["auctionsJoinBids"] = auctionsJoinBids.GetLatency()
		ret.Counts["auctionsSrc"] = auctionsSrc.GetCount()
		ret.Counts["bidsSrc"] = bidsSrc.GetCount()
		ret.Counts["sink"] = sink.GetCount()
	}
	transactionalID := fmt.Sprintf("%s-%s-%d", h.funcName,
		sp.InputTopicNames[0], sp.ParNum)
	streamTaskArgs := benchutil.UpdateStreamTaskArgs(sp,
		stream_task.NewStreamTaskArgsBuilder(h.env, procArgs, transactionalID)).
		KVStoreChangelogs(kvchangelogs).FixedOutParNum(sp.ParNum).Build()
	return task.ExecuteApp(ctx, streamTaskArgs, sp.EnableTransaction, update_stats)
}
