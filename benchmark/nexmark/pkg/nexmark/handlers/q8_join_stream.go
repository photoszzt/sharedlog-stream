package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"
	nutils "sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/concurrent_skiplist"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/execution"
	"sharedlog-stream/pkg/proc_interface"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/producer_consumer"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/store_restore"
	"sharedlog-stream/pkg/store_with_changelog"
	"sharedlog-stream/pkg/stream_task"
	"sharedlog-stream/pkg/utils"
	"sync"
	"time"

	"cs.utexas.edu/zjia/faas/types"
)

type q8JoinStreamHandler struct {
	env      types.Environment
	funcName string
}

func NewQ8JoinStreamHandler(env types.Environment, funcName string) types.FuncHandler {
	return &q8JoinStreamHandler{
		env:      env,
		funcName: funcName,
	}
}

func (h *q8JoinStreamHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &common.QueryInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output := h.Query8JoinStream(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	// fmt.Printf("query 3 output: %v\n", encodedOutput)
	return nutils.CompressData(encodedOutput), nil
}

func (h *q8JoinStreamHandler) process(
	ctx context.Context,
	t *stream_task.StreamTask,
	argsTmp interface{},
) *common.FnOutput {
	return execution.HandleJoinErrReturn(argsTmp)
}

func q8CompareFunc(lhs, rhs interface{}) int {
	l, ok := lhs.(uint64)
	if ok {
		r := rhs.(uint64)
		if l < r {
			return -1
		} else if l == r {
			return 0
		} else {
			return 1
		}
	} else {
		lv := lhs.(store.VersionedKey)
		rv := rhs.(store.VersionedKey)
		lvk := lv.Key.(uint64)
		rvk := rv.Key.(uint64)
		if lvk < rvk {
			return -1
		} else if lvk == rvk {
			if lv.Version < rv.Version {
				return -1
			} else if lv.Version == rv.Version {
				return 0
			} else {
				return 1
			}
		} else {
			return 1
		}
	}
}

func (h *q8JoinStreamHandler) getSrcSink(ctx context.Context, sp *common.QueryInput,
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
	timeout := common.SrcConsumeTimeout
	warmup := time.Duration(sp.WarmupS) * time.Second
	kvmsgSerdes := commtypes.KVMsgSerdes{
		KeySerde: commtypes.Uint64Serde{},
		ValSerde: eventSerde,
		MsgSerde: msgSerde,
	}
	ptSerde, err := ntypes.GetPersonTimeSerde(serdeFormat)
	if err != nil {
		return nil, nil, err
	}

	src1 := producer_consumer.NewMeteredConsumer(producer_consumer.NewShardedSharedLogStreamConsumer(stream1,
		&producer_consumer.StreamConsumerConfig{
			Timeout:     timeout,
			KVMsgSerdes: kvmsgSerdes,
		}), warmup)
	src2 := producer_consumer.NewMeteredConsumer(producer_consumer.NewShardedSharedLogStreamConsumer(stream2,
		&producer_consumer.StreamConsumerConfig{
			Timeout:     timeout,
			KVMsgSerdes: kvmsgSerdes,
		}), warmup)
	sink := producer_consumer.NewConcurrentMeteredSyncProducer(producer_consumer.NewShardedSharedLogStreamProducer(outputStream,
		&producer_consumer.StreamSinkConfig{
			KVMsgSerdes: commtypes.KVMsgSerdes{
				KeySerde: commtypes.Uint64Serde{},
				ValSerde: ptSerde,
				MsgSerde: msgSerde,
			},
			FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
		}), warmup)
	src1.SetInitialSource(false)
	src2.SetInitialSource(false)
	sink.MarkFinalOutput()
	return []producer_consumer.MeteredConsumerIntr{src1, src2}, []producer_consumer.MeteredProducerIntr{sink}, nil
}

func getTabAndToTab(env types.Environment,
	sp *common.QueryInput,
	tabName string,
	compare concurrent_skiplist.CompareFunc,
	kvmegSerdes commtypes.KVMsgSerdes,
	joinWindows *processor.JoinWindows,
) (*processor.MeteredProcessor, store.WindowStore, *store_with_changelog.MaterializeParam, error) {
	format := commtypes.SerdeFormat(sp.SerdeFormat)
	mp, err := store_with_changelog.NewMaterializeParamBuilder().
		KVMsgSerdes(kvmegSerdes).
		StoreName(tabName).
		ParNum(sp.ParNum).
		SerdeFormat(format).
		StreamParam(commtypes.CreateStreamParam{
			Env:          env,
			NumPartition: sp.NumInPartition,
		}).Build(time.Duration(sp.FlushMs)*time.Millisecond, common.SrcConsumeTimeout)
	if err != nil {
		return nil, nil, nil, err
	}
	toTab, winTab, err := store_with_changelog.ToInMemWindowTableWithChangelog(
		mp, joinWindows, compare, time.Duration(sp.WarmupS)*time.Second)
	if err != nil {
		return nil, nil, nil, err
	}
	return toTab, winTab, mp, nil
}

func (h *q8JoinStreamHandler) Query8JoinStream(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	srcs, sinks_arr, err := h.getSrcSink(ctx, sp)
	if err != nil {
		return &common.FnOutput{Success: false, Message: fmt.Sprintf("getSrcSink err: %v\n", err)}
	}
	joinWindows, err := processor.NewJoinWindowsWithGrace(time.Duration(10)*time.Second, time.Duration(5)*time.Second)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	var toAuctionsWindowTab *processor.MeteredProcessor
	var auctionsWinStore store.WindowStore
	var toPersonsWinTab *processor.MeteredProcessor
	var personsWinTab store.WindowStore
	aucTabName := "auctionsBySellerIDWinTab"
	perTabName := "personsByIDWinTab"
	var aucMp *store_with_changelog.MaterializeParam = nil
	var perMp *store_with_changelog.MaterializeParam = nil
	src1KVMsgSerdes := srcs[0].KVMsgSerdes()
	src2KVMsgSerdes := srcs[1].KVMsgSerdes()
	warmup := time.Duration(sp.WarmupS) * time.Second
	if sp.TableType == uint8(store.IN_MEM) {
		compare := concurrent_skiplist.CompareFunc(q8CompareFunc)
		toAuctionsWindowTab, auctionsWinStore, aucMp, err = getTabAndToTab(h.env, sp, aucTabName, compare, src1KVMsgSerdes, joinWindows)
		if err != nil {
			return &common.FnOutput{Success: false, Message: err.Error()}
		}
		toPersonsWinTab, personsWinTab, perMp, err = getTabAndToTab(h.env, sp, perTabName, compare, src2KVMsgSerdes, joinWindows)
		if err != nil {
			return &common.FnOutput{Success: false, Message: err.Error()}
		}
	} else if sp.TableType == uint8(store.MONGODB) {
		client, err := store.InitMongoDBClient(ctx, sp.MongoAddr)
		if err != nil {
			return &common.FnOutput{Success: false, Message: err.Error()}
		}

		toAuctionsWindowTab, auctionsWinStore, err = processor.ToMongoDBWindowTable(ctx,
			aucTabName, client, joinWindows, src1KVMsgSerdes.KeySerde, src1KVMsgSerdes.ValSerde,
			warmup)
		if err != nil {
			return &common.FnOutput{Success: false, Message: fmt.Sprintf("to table err: %v", err)}
		}

		toPersonsWinTab, personsWinTab, err = processor.ToMongoDBWindowTable(ctx,
			perTabName, client, joinWindows, src2KVMsgSerdes.KeySerde, src2KVMsgSerdes.ValSerde,
			warmup)
		if err != nil {
			return &common.FnOutput{Success: false, Message: fmt.Sprintf("to table err: %v", err)}
		}
	} else {
		panic("unrecognized table type")
	}
	windowSizeMs := int64(10 * 1000)
	joiner := processor.ValueJoinerWithKeyTsFunc(func(readOnlyKey interface{},
		leftValue interface{}, rightValue interface{}, leftTs int64, rightTs int64) interface{} {
		// fmt.Fprint(os.Stderr, "get into joiner\n")
		rv := rightValue.(*ntypes.Event)
		ts := rv.NewPerson.DateTime
		windowStart := (utils.MaxInt64(0, ts-windowSizeMs+windowSizeMs) / windowSizeMs) * windowSizeMs
		return &ntypes.PersonTime{
			ID:        rv.NewPerson.ID,
			Name:      rv.NewPerson.Name,
			StartTime: windowStart,
		}
	})

	auctionsJoinsPersons, personsJoinsAuctions := execution.ConfigureStreamStreamJoinProcessor(
		auctionsWinStore,
		personsWinTab,
		joiner,
		joinWindows,
		warmup)

	aJoinP := func(ctx context.Context, m commtypes.Message) ([]commtypes.Message, error) {
		_, err := toAuctionsWindowTab.ProcessAndReturn(ctx, m)
		if err != nil {
			return nil, err
		}
		return auctionsJoinsPersons.ProcessAndReturn(ctx, m)
	}

	pJoinA := func(ctx context.Context, m commtypes.Message) ([]commtypes.Message, error) {
		_, err := toPersonsWinTab.ProcessAndReturn(ctx, m)
		if err != nil {
			return nil, err
		}
		return personsJoinsAuctions.ProcessAndReturn(ctx, m)
	}
	debug.Assert(sp.ScaleEpoch != 0, "scale epoch should start from 1")

	joinProcAuction, joinProcPerson := execution.CreateJoinProcArgsPair(
		aJoinP, pJoinA, srcs, sinks_arr,
		proc_interface.NewBaseProcArgs(h.funcName, sp.ScaleEpoch, sp.ParNum))
	var wg sync.WaitGroup
	aucManager := execution.NewJoinProcManager()
	perManager := execution.NewJoinProcManager()

	procArgs := execution.NewCommonJoinProcArgs(
		joinProcAuction,
		joinProcPerson,
		aucManager.Out(),
		perManager.Out(),
		proc_interface.NewBaseSrcsSinks(srcs, sinks_arr))
	pctx := context.WithValue(ctx, benchutil.CTXID("id"), "person")
	actx := context.WithValue(ctx, benchutil.CTXID("id"), "auction")

	task := stream_task.NewStreamTaskBuilder().
		AppProcessFunc(h.process).
		InitFunc(func(progArgs interface{}) {
			aucManager.Run()
			perManager.Run()
		}).
		PauseFunc(func() *common.FnOutput {
			// debug.Fprintf(os.Stderr, "in flush func\n")
			aucManager.RequestToTerminate()
			perManager.RequestToTerminate()
			debug.Fprintf(os.Stderr, "waiting join proc to exit\n")
			wg.Wait()
			if ret := execution.HandleJoinErrReturn(procArgs); ret != nil {
				return ret
			}
			// debug.Fprintf(os.Stderr, "join procs exited\n")
			return nil
		}).
		ResumeFunc(func(task *stream_task.StreamTask) {
			// debug.Fprintf(os.Stderr, "resume join porc\n")
			aucManager.LaunchJoinProcLoop(actx, task, joinProcAuction, &wg)
			perManager.LaunchJoinProcLoop(pctx, task, joinProcPerson, &wg)

			aucManager.Run()
			perManager.Run()
			// debug.Fprintf(os.Stderr, "done resume join proc\n")
		}).Build()

	aucManager.LaunchJoinProcLoop(actx, task, joinProcAuction, &wg)
	perManager.LaunchJoinProcLoop(pctx, task, joinProcPerson, &wg)

	var wsc []*store_restore.WindowStoreChangelog
	if sp.TableType == uint8(store.IN_MEM) {
		wsc = []*store_restore.WindowStoreChangelog{
			store_restore.NewWindowStoreChangelog(
				auctionsWinStore, aucMp.ChangelogManager(), sp.ParNum),
			store_restore.NewWindowStoreChangelog(
				personsWinTab, perMp.ChangelogManager(), sp.ParNum),
		}
	} else if sp.TableType == uint8(store.MONGODB) {
		wsc = []*store_restore.WindowStoreChangelog{
			store_restore.NewWindowStoreChangelogForExternalStore(
				auctionsWinStore, srcs[0].Stream(), execution.JoinProcSerialWithoutSink,
				execution.NewJoinProcWithoutSinkArgs(srcs[0].InnerSource(), aJoinP, sp.ParNum),
				fmt.Sprintf("%s-%s-%d", h.funcName, auctionsWinStore.Name(), sp.ParNum), sp.ParNum),
			store_restore.NewWindowStoreChangelogForExternalStore(
				personsWinTab, srcs[1].Stream(), execution.JoinProcSerialWithoutSink,
				execution.NewJoinProcWithoutSinkArgs(srcs[1].InnerSource(), pJoinA, sp.ParNum),
				fmt.Sprintf("%s-%s-%d", h.funcName, personsWinTab.Name(), sp.ParNum), sp.ParNum),
		}
	} else {
		panic("unrecognized table type")
	}

	update_stats := func(ret *common.FnOutput) {
		ret.Latencies["eventTimeLatency"] = sinks_arr[0].GetEventTimeLatency()
	}
	transactionalID := fmt.Sprintf("%s-%d", h.funcName, sp.ParNum)
	streamTaskArgs := benchutil.UpdateStreamTaskArgs(sp,
		stream_task.NewStreamTaskArgsBuilder(h.env, procArgs, transactionalID)).
		WindowStoreChangelogs(wsc).
		FixedOutParNum(sp.ParNum).
		Build()
	return task.ExecuteApp(ctx, streamTaskArgs, update_stats)
}
