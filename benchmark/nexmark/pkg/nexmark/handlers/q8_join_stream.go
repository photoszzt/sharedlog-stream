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
	"sharedlog-stream/pkg/concurrent_skiplist"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/hash"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/source_sink"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"
	"sharedlog-stream/pkg/stream/processor/store_with_changelog"
	"sharedlog-stream/pkg/transaction"
	"sharedlog-stream/pkg/transaction/tran_interface"
	"sync"
	"time"

	"cs.utexas.edu/zjia/faas/types"
)

type q8JoinStreamHandler struct {
	env types.Environment

	cHashMu sync.RWMutex
	cHash   *hash.ConsistentHash

	funcName string
}

func NewQ8JoinStreamHandler(env types.Environment, funcName string) types.FuncHandler {
	return &q8JoinStreamHandler{
		env:      env,
		cHash:    hash.NewConsistentHash(),
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
	return utils.CompressData(encodedOutput), nil
}

type q8JoinStreamProcessArgs struct {
	personsOutChan   <-chan *common.FnOutput
	auctionsOutChan  <-chan *common.FnOutput
	recordFinishFunc transaction.RecordPrevInstanceFinishFunc
	funcName         string
	curEpoch         uint64
	parNum           uint8
}

func (a *q8JoinStreamProcessArgs) ParNum() uint8    { return a.parNum }
func (a *q8JoinStreamProcessArgs) CurEpoch() uint64 { return a.curEpoch }
func (a *q8JoinStreamProcessArgs) FuncName() string { return a.funcName }
func (a *q8JoinStreamProcessArgs) RecordFinishFunc() func(ctx context.Context, funcName string, instanceId uint8) error {
	return a.recordFinishFunc
}

func (h *q8JoinStreamHandler) process(
	ctx context.Context,
	t *transaction.StreamTask,
	argsTmp interface{},
) *common.FnOutput {
	return handleQ8ErrReturn(argsTmp)
}

func handleQ8ErrReturn(argsTmp interface{}) *common.FnOutput {
	args := argsTmp.(*q8JoinStreamProcessArgs)
	var aOut *common.FnOutput
	var pOut *common.FnOutput
	select {
	case personOutput := <-args.personsOutChan:
		pOut = personOutput
		debug.Fprintf(os.Stderr, "Got persons out: %v\n", pOut)
	case auctionOutput := <-args.auctionsOutChan:
		aOut = auctionOutput
		debug.Fprintf(os.Stderr, "Got auctions out: %v\n", aOut)
	default:
	}
	if pOut != nil && !pOut.Success {
		return pOut
	}
	if aOut != nil && !aOut.Success {
		return aOut
	}
	return nil
}

/*
func (h *q8JoinStreamHandler) processSerial(
	ctx context.Context,
	t *transaction.StreamTask,
	argsTmp interface{},
) (map[string]uint64, *common.FnOutput) {
	args := argsTmp.(*q8JoinStreamProcessArgs)

	joinProgArgsAuction := &joinProcArgs{
		src:           args.auctionSrc,
		sink:          args.sink,
		parNum:        args.parNum,
		runner:        args.aJoinP,
		offMu:         &h.offMu,
		currentOffset: t.CurrentOffset,
		trackParFunc:  args.trackParFunc,
		cHashMu:       &h.cHashMu,
		cHash:         h.cHash,
	}
	aOut := joinProcSerial(ctx, joinProgArgsAuction)
	joinProcArgsPerson := &joinProcArgs{
		src:           args.personSrc,
		sink:          args.sink,
		parNum:        args.parNum,
		runner:        args.pJoinA,
		offMu:         &h.offMu,
		currentOffset: t.CurrentOffset,
		trackParFunc:  args.trackParFunc,
		cHashMu:       &h.cHashMu,
		cHash:         h.cHash,
	}
	pOut := joinProcSerial(ctx, joinProcArgsPerson)
	if pOut != nil {
		if aOut != nil {
			succ := pOut.Success && aOut.Success
			return t.CurrentOffset, &common.FnOutput{Success: succ, Message: pOut.Message + "," + aOut.Message}
		}
		return t.CurrentOffset, pOut
	} else if aOut != nil {
		return t.CurrentOffset, aOut
	}
	return t.CurrentOffset, nil
}
*/

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
	stream1 *sharedlog_stream.ShardedSharedLogStream,
	stream2 *sharedlog_stream.ShardedSharedLogStream,
	outputStream *sharedlog_stream.ShardedSharedLogStream,
) (*srcSinkSerde, error) {
	msgSerde, err := commtypes.GetMsgSerde(sp.SerdeFormat)
	if err != nil {
		return nil, fmt.Errorf("get msg serde err: %v", err)
	}

	eventSerde, err := getEventSerde(sp.SerdeFormat)
	if err != nil {
		return nil, fmt.Errorf("get event serde err: %v", err)
	}
	timeout := common.SrcConsumeTimeout
	kvmsgSerdes := commtypes.KVMsgSerdes{
		KeySerde: commtypes.Uint64Serde{},
		ValSerde: eventSerde,
		MsgSerde: msgSerde,
	}
	auctionsConfig := &source_sink.StreamSourceConfig{
		Timeout:     timeout,
		KVMsgSerdes: kvmsgSerdes,
	}
	personsConfig := &source_sink.StreamSourceConfig{
		Timeout:     timeout,
		KVMsgSerdes: kvmsgSerdes,
	}
	var ptSerde commtypes.Serde
	if sp.SerdeFormat == uint8(commtypes.JSON) {
		ptSerde = &ntypes.PersonTimeJSONSerde{}
	} else {
		ptSerde = &ntypes.PersonTimeMsgpSerde{}
	}
	outConfig := &source_sink.StreamSinkConfig{
		KVMsgSerdes: commtypes.KVMsgSerdes{
			KeySerde: commtypes.Uint64Serde{},
			ValSerde: ptSerde,
			MsgSerde: msgSerde,
		},
		FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
	}

	src1 := source_sink.NewMeteredSource(source_sink.NewShardedSharedLogStreamSource(stream1, auctionsConfig),
		time.Duration(sp.WarmupS)*time.Second)
	src2 := source_sink.NewMeteredSource(source_sink.NewShardedSharedLogStreamSource(stream2, personsConfig),
		time.Duration(sp.WarmupS)*time.Second)
	sink := source_sink.NewConcurrentMeteredSyncSink(source_sink.NewShardedSharedLogStreamSyncSink(outputStream, outConfig),
		time.Duration(sp.WarmupS)*time.Second)
	src1.SetInitialSource(false)
	src2.SetInitialSource(false)
	sink.MarkFinalOutput()
	return &srcSinkSerde{src1: src1, src2: src2, sink: sink,
		srcKVMsgSerdes: kvmsgSerdes}, nil
}

func getTabAndToTab(env types.Environment,
	sp *common.QueryInput,
	tabName string,
	compare concurrent_skiplist.CompareFunc,
	kvmegSerdes commtypes.KVMsgSerdes,
	joinWindows *processor.JoinWindows,
) (*processor.MeteredProcessor, store.WindowStore, *store_with_changelog.MaterializeParam, error) {
	format := commtypes.SerdeFormat(sp.SerdeFormat)
	mp, err := store_with_changelog.NewMaterializeParamForWindowStore(env,
		kvmegSerdes, tabName, commtypes.CreateStreamParam{
			Format:       format,
			NumPartition: sp.NumInPartition,
		}, sp.ParNum, compare)
	if err != nil {
		return nil, nil, nil, err
	}
	toTab, winTab, err := store_with_changelog.ToInMemWindowTableWithChangelog(
		tabName, mp, joinWindows, time.Duration(sp.WarmupS)*time.Second)
	if err != nil {
		return nil, nil, nil, err
	}
	return toTab, winTab, mp, nil
}

func (h *q8JoinStreamHandler) Query8JoinStream(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	auctionsStream, personsStream, outputStream, err := getInOutStreams(ctx, h.env, sp)
	if err != nil {
		return &common.FnOutput{Success: false, Message: fmt.Sprintf("get input output err: %v", err)}
	}
	sss, err := h.getSrcSink(ctx, sp, auctionsStream,
		personsStream, outputStream)
	if err != nil {
		return &common.FnOutput{Success: false, Message: fmt.Sprintf("getSrcSink err: %v\n", err)}
	}
	auctionsSrc, personsSrc, sink := sss.src1, sss.src2, sss.sink
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
	if sp.TableType == uint8(store.IN_MEM) {
		compare := concurrent_skiplist.CompareFunc(q8CompareFunc)
		toAuctionsWindowTab, auctionsWinStore, aucMp, err = getTabAndToTab(h.env, sp, aucTabName, compare, sss.srcKVMsgSerdes, joinWindows)
		if err != nil {
			return &common.FnOutput{Success: false, Message: err.Error()}
		}
		toPersonsWinTab, personsWinTab, perMp, err = getTabAndToTab(h.env, sp, perTabName, compare, sss.srcKVMsgSerdes, joinWindows)
		if err != nil {
			return &common.FnOutput{Success: false, Message: err.Error()}
		}
	} else if sp.TableType == uint8(store.MONGODB) {
		client, err := store.InitMongoDBClient(ctx, sp.MongoAddr)
		if err != nil {
			return &common.FnOutput{Success: false, Message: err.Error()}
		}
		toAuctionsWindowTab, auctionsWinStore, err = processor.ToMongoDBWindowTable(ctx,
			aucTabName, client, joinWindows, sss.srcKVMsgSerdes.KeySerde, sss.srcKVMsgSerdes.ValSerde,
			time.Duration(sp.WarmupS)*time.Second)
		if err != nil {
			return &common.FnOutput{Success: false, Message: fmt.Sprintf("to table err: %v", err)}
		}
		toPersonsWinTab, personsWinTab, err = processor.ToMongoDBWindowTable(ctx,
			perTabName, client, joinWindows, sss.srcKVMsgSerdes.KeySerde, sss.srcKVMsgSerdes.ValSerde,
			time.Duration(sp.WarmupS)*time.Second)
		if err != nil {
			return &common.FnOutput{Success: false, Message: fmt.Sprintf("to table err: %v", err)}
		}
	} else {
		panic("unrecognized table type")
	}

	joiner := processor.ValueJoinerWithKeyTsFunc(func(readOnlyKey interface{},
		leftValue interface{}, rightValue interface{}, leftTs int64, rightTs int64) interface{} {
		// fmt.Fprint(os.Stderr, "get into joiner\n")
		lv := leftValue.(*ntypes.Event)
		rv := rightValue.(*ntypes.Event)
		st := leftTs
		if st > rightTs {
			st = rightTs
		}
		if lv.Etype == ntypes.PERSON {
			return &ntypes.PersonTime{
				ID:        lv.NewPerson.ID,
				Name:      lv.NewPerson.Name,
				StartTime: st,
			}
		} else {
			return &ntypes.PersonTime{
				ID:        rv.NewPerson.ID,
				Name:      rv.NewPerson.Name,
				StartTime: st,
			}
		}
	})

	sharedTimeTracker := processor.NewTimeTracker()
	personsJoinsAuctions := processor.NewMeteredProcessor(
		processor.NewStreamStreamJoinProcessor(auctionsWinStore, joinWindows,
			joiner, false, true, sharedTimeTracker), time.Duration(sp.WarmupS)*time.Second)

	auctionsJoinsPersons := processor.NewMeteredProcessor(
		processor.NewStreamStreamJoinProcessor(personsWinTab, joinWindows,
			processor.ReverseValueJoinerWithKeyTs(joiner), false, false, sharedTimeTracker),
		time.Duration(sp.WarmupS)*time.Second,
	)

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

	transaction.SetupConsistentHash(&h.cHashMu, h.cHash, sp.NumOutPartitions[0])
	debug.Assert(sp.ScaleEpoch != 0, "scale epoch should start from 1")

	currentOffset := make(map[string]uint64)
	joinProcPerson := &joinProcArgs{
		src:          sss.src2,
		sink:         sss.sink,
		parNum:       sp.ParNum,
		runner:       pJoinA,
		trackParFunc: tran_interface.DefaultTrackSubstreamFunc,
		cHashMu:      &h.cHashMu,
		cHash:        h.cHash,
	}
	joinProcAuction := &joinProcArgs{
		src:          sss.src1,
		sink:         sss.sink,
		parNum:       sp.ParNum,
		runner:       aJoinP,
		trackParFunc: tran_interface.DefaultTrackSubstreamFunc,
		cHashMu:      &h.cHashMu,
		cHash:        h.cHash,
	}
	var wg sync.WaitGroup
	aucManager := NewJoinProcManager()
	perManager := NewJoinProcManager()

	procArgs := &q8JoinStreamProcessArgs{
		personsOutChan:   perManager.Out(),
		auctionsOutChan:  aucManager.Out(),
		parNum:           sp.ParNum,
		recordFinishFunc: transaction.DefaultRecordPrevInstanceFinishFunc,
		curEpoch:         sp.ScaleEpoch,
		funcName:         h.funcName,
	}
	pctx := context.WithValue(ctx, "id", "person")
	actx := context.WithValue(ctx, "id", "auction")

	task := transaction.StreamTask{
		ProcessFunc:   h.process,
		CurrentOffset: currentOffset,
		PauseFunc: func() *common.FnOutput {
			// debug.Fprintf(os.Stderr, "in flush func\n")
			aucManager.RequestToTerminate()
			perManager.RequestToTerminate()
			debug.Fprintf(os.Stderr, "waiting join proc to exit\n")
			wg.Wait()
			if ret := handleQ8ErrReturn(procArgs); ret != nil {
				return ret
			}
			// debug.Fprintf(os.Stderr, "join procs exited\n")
			return nil
		},
		ResumeFunc: func(task *transaction.StreamTask) {
			// debug.Fprintf(os.Stderr, "resume join porc\n")
			aucManager.LaunchJoinProcLoop(actx, task, joinProcAuction, &wg)
			perManager.LaunchJoinProcLoop(pctx, task, joinProcPerson, &wg)

			aucManager.Run()
			perManager.Run()
			// debug.Fprintf(os.Stderr, "done resume join proc\n")
		},
		InitFunc: func(progArgs interface{}) {
			sss.src1.StartWarmup()
			sss.src2.StartWarmup()
			sss.sink.StartWarmup()
			toAuctionsWindowTab.StartWarmup()
			toPersonsWinTab.StartWarmup()
			auctionsJoinsPersons.StartWarmup()
			personsJoinsAuctions.StartWarmup()

			aucManager.Run()
			perManager.Run()
		},
		CommitEveryForAtLeastOnce: common.CommitDuration,
	}

	aucManager.LaunchJoinProcLoop(actx, &task, joinProcAuction, &wg)
	perManager.LaunchJoinProcLoop(pctx, &task, joinProcPerson, &wg)

	srcs := []source_sink.Source{auctionsSrc, personsSrc}
	sinks_arr := []source_sink.Sink{sink}
	var wsc []*transaction.WindowStoreChangelog
	if sp.TableType == uint8(store.IN_MEM) {
		wsc = []*transaction.WindowStoreChangelog{
			transaction.NewWindowStoreChangelog(
				auctionsWinStore,
				aucMp.ChangelogManager,
				nil,
				sss.srcKVMsgSerdes,
				sp.ParNum,
			),
			transaction.NewWindowStoreChangelog(
				personsWinTab,
				perMp.ChangelogManager,
				nil,
				sss.srcKVMsgSerdes,
				sp.ParNum,
			),
		}
	} else if sp.TableType == uint8(store.MONGODB) {
		wsc = []*transaction.WindowStoreChangelog{
			transaction.NewWindowStoreChangelogForExternalStore(
				auctionsWinStore, auctionsStream, joinProcSerialWithoutSink,
				&joinProcWithoutSinkArgs{
					src:    sss.src1.InnerSource(),
					parNum: sp.ParNum,
					runner: aJoinP,
				}, fmt.Sprintf("%s-%s-%d", h.funcName, auctionsWinStore.Name(), sp.ParNum), sp.ParNum),
			transaction.NewWindowStoreChangelogForExternalStore(
				personsWinTab, personsStream, joinProcSerialWithoutSink,
				&joinProcWithoutSinkArgs{
					src:    sss.src2.InnerSource(),
					parNum: sp.ParNum,
					runner: pJoinA,
				}, fmt.Sprintf("%s-%s-%d", h.funcName, personsWinTab.Name(), sp.ParNum), sp.ParNum),
		}
	} else {
		panic("unrecognized table type")
	}
	if sp.EnableTransaction {
		streamTaskArgs := transaction.StreamTaskArgsTransaction{
			ProcArgs:              procArgs,
			Env:                   h.env,
			Srcs:                  srcs,
			Sinks:                 sinks_arr,
			TransactionalId:       fmt.Sprintf("%s-%d", h.funcName, sp.ParNum),
			WindowStoreChangelogs: wsc,
			KVChangelogs:          nil,
			FixedOutParNum:        0,
		}
		benchutil.UpdateStreamTaskArgsTransaction(sp, &streamTaskArgs)
		ret := transaction.SetupManagersAndProcessTransactional(ctx, h.env, &streamTaskArgs,
			func(procArgs interface{}, trackParFunc tran_interface.TrackKeySubStreamFunc, recordFinishFunc transaction.RecordPrevInstanceFinishFunc) {
				joinProcAuction.trackParFunc = trackParFunc
				joinProcPerson.trackParFunc = trackParFunc
				procArgs.(*q8JoinStreamProcessArgs).recordFinishFunc = recordFinishFunc
			}, &task)
		if ret != nil && ret.Success {
			ret.Latencies["auctionsSrc"] = auctionsSrc.GetLatency()
			ret.Latencies["personsSrc"] = personsSrc.GetLatency()
			ret.Latencies["toAuctionsWindowTab"] = toAuctionsWindowTab.GetLatency()
			ret.Latencies["toPersonsWinTab"] = toPersonsWinTab.GetLatency()
			ret.Latencies["personsJoinsAuctions"] = personsJoinsAuctions.GetLatency()
			ret.Latencies["auctionsJoinsPersons"] = auctionsJoinsPersons.GetLatency()
			ret.Latencies["sink"] = sink.GetLatency()
			ret.Latencies["eventTimeLatency"] = sink.GetEventTimeLatency()
			ret.Consumed["auctionsSrc"] = auctionsSrc.GetCount()
			ret.Consumed["personsSrc"] = personsSrc.GetCount()
		}
		return ret
	}
	streamTaskArgs := transaction.NewStreamTaskArgs(h.env, procArgs, srcs, sinks_arr).
		WithWindowStoreChangelogs(wsc)
	benchutil.UpdateStreamTaskArgs(sp, streamTaskArgs)
	ret := task.Process(ctx, streamTaskArgs)
	if ret != nil && ret.Success {
		ret.Latencies["auctionsSrc"] = auctionsSrc.GetLatency()
		ret.Latencies["personsSrc"] = personsSrc.GetLatency()
		ret.Latencies["sink"] = sink.GetLatency()
		ret.Latencies["toAuctionsWindowTab"] = toAuctionsWindowTab.GetLatency()
		ret.Latencies["toPersonsWinTab"] = toPersonsWinTab.GetLatency()
		ret.Latencies["personsJoinsAuctions"] = personsJoinsAuctions.GetLatency()
		ret.Latencies["auctionsJoinsPersons"] = auctionsJoinsPersons.GetLatency()
		ret.Latencies["eventTimeLatency"] = sink.GetEventTimeLatency()
		ret.Consumed["auctionsSrc"] = auctionsSrc.GetCount()
		ret.Consumed["personsSrc"] = personsSrc.GetCount()
	}
	return ret
}
