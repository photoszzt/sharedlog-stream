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
	"sharedlog-stream/pkg/treemap"
	"sync"
	"time"

	"cs.utexas.edu/zjia/faas/types"
)

type q3JoinTableHandler struct {
	env     types.Environment
	cHashMu sync.RWMutex
	cHash   *hash.ConsistentHash

	funcName string
}

func NewQ3JoinTableHandler(env types.Environment, funcName string) types.FuncHandler {
	return &q3JoinTableHandler{
		env:      env,
		cHash:    hash.NewConsistentHash(),
		funcName: funcName,
	}
}

func (h *q3JoinTableHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &common.QueryInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output := h.Query3JoinTable(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	// fmt.Printf("query 3 output: %v\n", encodedOutput)
	return utils.CompressData(encodedOutput), nil
}

func (h *q3JoinTableHandler) process(ctx context.Context,
	t *transaction.StreamTask,
	argsTmp interface{},
) *common.FnOutput {
	return handleQ3ErrReturn(argsTmp)
}

func handleQ3ErrReturn(argsTmp interface{}) *common.FnOutput {
	args := argsTmp.(*q3JoinTableProcessArgs)
	var aOut *common.FnOutput
	var pOut *common.FnOutput
	select {
	case personOutput := <-args.personsOutChan:
		pOut = personOutput
		debug.Fprintf(os.Stderr, "Got per out: %v, per out channel len: %d\n", pOut, len(args.personsOutChan))
	case auctionOutput := <-args.auctionsOutChan:
		aOut = auctionOutput
		debug.Fprintf(os.Stderr, "Got auc out: %v, auc out channel len: %d\n", aOut, len(args.auctionsOutChan))
	default:
	}
	if aOut != nil || pOut != nil {
		debug.Fprintf(os.Stderr, "aOut: %v\n", aOut)
		debug.Fprintf(os.Stderr, "pOut: %v\n", pOut)
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
func (h *q3JoinTableHandler) processOut(aOut *common.FnOutput, pOut *common.FnOutput, args *q3JoinTableProcessArgs) *common.FnOutput {
	debug.Fprintf(os.Stderr, "aOut: %v\n", aOut)
	debug.Fprintf(os.Stderr, "pOut: %v\n", pOut)
	if pOut != nil && !pOut.Success {
		return pOut
	}
	if aOut != nil && !aOut.Success {
		return aOut
	}
	if args.personDone && args.auctionDone {
		return &common.FnOutput{Success: true, Message: errors.ErrStreamSourceTimeout.Error()}
	}
	return nil
}
*/

func getInOutStreams(
	ctx context.Context,
	env types.Environment,
	input *common.QueryInput,
) (*sharedlog_stream.ShardedSharedLogStream, /* auction */
	*sharedlog_stream.ShardedSharedLogStream, /* person */
	*sharedlog_stream.ShardedSharedLogStream, /* output */
	error,
) {
	inputStream1, err := sharedlog_stream.NewShardedSharedLogStream(env, input.InputTopicNames[0], input.NumInPartition,
		commtypes.SerdeFormat(input.SerdeFormat))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("NewSharedlogStream for input stream failed: %v", err)
	}
	inputStream2, err := sharedlog_stream.NewShardedSharedLogStream(env, input.InputTopicNames[1], input.NumInPartition,
		commtypes.SerdeFormat(input.SerdeFormat))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("NewSharedlogStream for input stream failed: %v", err)
	}
	outputStream, err := sharedlog_stream.NewShardedSharedLogStream(env, input.OutputTopicNames[0], input.NumOutPartitions[0],
		commtypes.SerdeFormat(input.SerdeFormat))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("NewSharedlogStream for output stream failed: %v", err)
	}
	return inputStream1, inputStream2, outputStream, nil
}

type srcSinkSerde struct {
	src1           *source_sink.MeteredSource
	src2           *source_sink.MeteredSource
	sink           *source_sink.ConcurrentMeteredSyncSink
	srcKVMsgSerdes commtypes.KVMsgSerdes
}

func (h *q3JoinTableHandler) getSrcSink(ctx context.Context, sp *common.QueryInput,
	stream1 *sharedlog_stream.ShardedSharedLogStream,
	stream2 *sharedlog_stream.ShardedSharedLogStream,
	outputStream *sharedlog_stream.ShardedSharedLogStream,
) (*srcSinkSerde,
	error,
) {
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
	var ncsiSerde commtypes.Serde
	if sp.SerdeFormat == uint8(commtypes.JSON) {
		ncsiSerde = ntypes.NameCityStateIdJSONSerde{}
	} else {
		ncsiSerde = ntypes.NameCityStateIdMsgpSerde{}
	}
	outConfig := &source_sink.StreamSinkConfig{
		KVMsgSerdes: commtypes.KVMsgSerdes{
			KeySerde: commtypes.Uint64Serde{},
			ValSerde: ncsiSerde,
			MsgSerde: msgSerde,
		},
		FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
	}

	src1 := source_sink.NewMeteredSource(source_sink.NewShardedSharedLogStreamSource(stream1, auctionsConfig),
		time.Duration(sp.WarmupS)*time.Second)
	src2 := source_sink.NewMeteredSource(source_sink.NewShardedSharedLogStreamSource(stream2, personsConfig),
		time.Duration(sp.WarmupS)*time.Second)
	src1.SetInitialSource(false)
	src2.SetInitialSource(false)
	sink := source_sink.NewConcurrentMeteredSyncSink(source_sink.NewShardedSharedLogStreamSyncSink(outputStream, outConfig),
		time.Duration(sp.WarmupS)*time.Second)
	sink.MarkFinalOutput()
	sss := &srcSinkSerde{
		src1:           src1,
		src2:           src2,
		sink:           sink,
		srcKVMsgSerdes: kvmsgSerdes,
	}
	return sss, nil
}

type kvtables struct {
	toTab1 *processor.MeteredProcessor
	tab1   store.KeyValueStore
	toTab2 *processor.MeteredProcessor
	tab2   store.KeyValueStore
}

func (h *q3JoinTableHandler) setupTables(ctx context.Context,
	tabType store.TABLE_TYPE,
	sss *srcSinkSerde,
	serdeFormat commtypes.SerdeFormat,
	mongoAddr string,
	warmup time.Duration,
) (
	*kvtables,
	error,
) {
	if tabType == store.IN_MEM {
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

		toAuctionsTable, auctionsStore, err := processor.ToInMemKVTable(
			"auctionsBySellerIDStore", compare, warmup)
		if err != nil {
			return nil, fmt.Errorf("toAucTab err: %v", err)
		}

		toPersonsTable, personsStore, err := processor.ToInMemKVTable(
			"personsByIDStore", compare, warmup)
		if err != nil {
			return nil, fmt.Errorf("toPersonsTab err: %v", err)
		}
		return &kvtables{toAuctionsTable, auctionsStore, toPersonsTable, personsStore}, nil
	} else if tabType == store.MONGODB {
		var vtSerde0 commtypes.Serde
		var vtSerde1 commtypes.Serde
		if serdeFormat == commtypes.JSON {
			vtSerde0 = commtypes.ValueTimestampJSONSerde{
				ValJSONSerde: sss.srcKVMsgSerdes.ValSerde,
			}
			vtSerde1 = commtypes.ValueTimestampJSONSerde{
				ValJSONSerde: sss.srcKVMsgSerdes.ValSerde,
			}
		} else if serdeFormat == commtypes.MSGP {
			vtSerde0 = commtypes.ValueTimestampMsgpSerde{
				ValMsgpSerde: sss.srcKVMsgSerdes.ValSerde,
			}
			vtSerde1 = commtypes.ValueTimestampMsgpSerde{
				ValMsgpSerde: sss.srcKVMsgSerdes.ValSerde,
			}
		} else {
			panic("unrecognized serde format")
		}
		client, err := store.InitMongoDBClient(ctx, mongoAddr)
		if err != nil {
			return nil, err
		}
		toAuctionsTable, auctionsStore, err := processor.ToMongoDBKVTable(ctx, "auctionsBySellerIDStore",
			client, sss.srcKVMsgSerdes.KeySerde, vtSerde0, warmup)
		if err != nil {
			return nil, err
		}
		toPersonsTable, personsStore, err := processor.ToMongoDBKVTable(ctx, "personsByIDStore",
			client, sss.srcKVMsgSerdes.KeySerde, vtSerde1, warmup)
		if err != nil {
			return nil, err
		}
		return &kvtables{toAuctionsTable, auctionsStore, toPersonsTable, personsStore}, nil
	} else {
		return nil, fmt.Errorf("unrecognized table type")
	}
}

type q3JoinTableProcessArgs struct {
	personsOutChan   <-chan *common.FnOutput
	auctionsOutChan  <-chan *common.FnOutput
	recordFinishFunc transaction.RecordPrevInstanceFinishFunc
	funcName         string
	curEpoch         uint64
	parNum           uint8
}

func (a *q3JoinTableProcessArgs) ParNum() uint8    { return a.parNum }
func (a *q3JoinTableProcessArgs) CurEpoch() uint64 { return a.curEpoch }
func (a *q3JoinTableProcessArgs) FuncName() string { return a.funcName }
func (a *q3JoinTableProcessArgs) RecordFinishFunc() func(ctx context.Context, funcName string, instanceId uint8) error {
	return a.recordFinishFunc
}

func (h *q3JoinTableHandler) Query3JoinTable(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	auctionsStream, personsStream, outputStream, err := getInOutStreams(ctx, h.env, sp)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("get input output err: %v", err),
		}
	}
	sss, err := h.getSrcSink(ctx, sp, auctionsStream,
		personsStream, outputStream)
	if err != nil {
		return &common.FnOutput{Success: false, Message: fmt.Sprintf("getSrcSink err: %v\n", err)}
	}

	kvtabs, err := h.setupTables(ctx, store.TABLE_TYPE(sp.TableType), sss, commtypes.SerdeFormat(sp.SerdeFormat), sp.MongoAddr, time.Duration(sp.WarmupS)*time.Second)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	joiner := processor.ValueJoinerWithKeyFunc(
		func(readOnlyKey interface{}, leftVal interface{}, rightVal interface{}) interface{} {
			var event *ntypes.Event
			vt, ok := rightVal.(commtypes.ValueTimestamp)
			if ok {
				event = vt.Value.(*ntypes.Event)
			} else {
				event = rightVal.(*ntypes.Event)
			}
			return &ntypes.NameCityStateId{
				Name:  event.NewPerson.Name,
				City:  event.NewPerson.City,
				State: event.NewPerson.State,
				ID:    event.NewPerson.ID,
			}
		})

	auctionJoinsPersons := processor.NewMeteredProcessor(
		processor.NewTableTableJoinProcessor(kvtabs.tab2.Name(), kvtabs.tab2, joiner),
		time.Duration(sp.WarmupS)*time.Second)

	personJoinsAuctions := processor.NewMeteredProcessor(
		processor.NewTableTableJoinProcessor(kvtabs.tab1.Name(), kvtabs.tab1,
			processor.ReverseValueJoinerWithKey(joiner)),
		time.Duration(sp.WarmupS)*time.Second)

	aJoinP := JoinWorkerFunc(func(c context.Context, m commtypes.Message) ([]commtypes.Message, error) {
		// msg is auction
		_, err := kvtabs.toTab1.ProcessAndReturn(ctx, m)
		if err != nil {
			return nil, fmt.Errorf("ToTabA err: %v", err)
		}
		msgs, err := auctionJoinsPersons.ProcessAndReturn(ctx, m)
		if err != nil {
			err = fmt.Errorf("aJoinP err: %v", err)
		}
		return msgs, err
	})
	pJoinA := JoinWorkerFunc(func(ctx context.Context, m commtypes.Message) ([]commtypes.Message, error) {
		// msg is person
		_, err := kvtabs.toTab2.ProcessAndReturn(ctx, m)
		if err != nil {
			return nil, fmt.Errorf("ToTabP err: %v", err)
		}
		msgs, err := personJoinsAuctions.ProcessAndReturn(ctx, m)
		if err != nil {
			err = fmt.Errorf("pJoinA err: %v", err)
		}
		return msgs, err
	})

	debug.Assert(len(sp.NumOutPartitions) == 1 && len(sp.OutputTopicNames) == 1,
		"expected only one output stream")
	transaction.SetupConsistentHash(&h.cHashMu, h.cHash, sp.NumOutPartitions[0])

	debug.Assert(sp.ScaleEpoch != 0, "scale epoch should start from 1")

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
	procArgs := &q3JoinTableProcessArgs{
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
		CurrentOffset: make(map[string]uint64),
		PauseFunc: func() *common.FnOutput {
			aucManager.RequestToTerminate()
			perManager.RequestToTerminate()
			debug.Fprintf(os.Stderr, "q3 waiting for join proc to exit\n")
			wg.Wait()
			debug.Fprintf(os.Stderr, "down pause\n")
			// check the goroutine's return in case any of them returns output
			ret := handleQ3ErrReturn(procArgs)
			if ret != nil {
				return ret
			}
			return nil
		},
		ResumeFunc: func(task *transaction.StreamTask) {
			debug.Fprintf(os.Stderr, "resume begin\n")
			aucManager.LaunchJoinProcLoop(actx, task, joinProcAuction, &wg)
			perManager.LaunchJoinProcLoop(pctx, task, joinProcPerson, &wg)

			aucManager.Run()
			perManager.Run()
			debug.Fprintf(os.Stderr, "resume done\n")
		},
		InitFunc: func(procArgsTmp interface{}) {
			sss.src1.StartWarmup()
			sss.src2.StartWarmup()
			sss.sink.StartWarmup()
			kvtabs.toTab1.StartWarmup()
			kvtabs.toTab2.StartWarmup()
			auctionJoinsPersons.StartWarmup()
			personJoinsAuctions.StartWarmup()

			aucManager.Run()
			perManager.Run()
		},
		CommitEveryForAtLeastOnce: common.CommitDuration,
	}

	aucManager.LaunchJoinProcLoop(actx, &task, joinProcAuction, &wg)
	perManager.LaunchJoinProcLoop(pctx, &task, joinProcPerson, &wg)

	srcs := []source_sink.Source{sss.src1, sss.src2}
	sinks_arr := []source_sink.Sink{sss.sink}
	var kvchangelogs []*transaction.KVStoreChangelog
	if sp.TableType == uint8(store.IN_MEM) {
		serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
		kvchangelogs = []*transaction.KVStoreChangelog{
			transaction.NewKVStoreChangelog(kvtabs.tab1,
				store_with_changelog.NewChangelogManager(auctionsStream, serdeFormat),
				sss.srcKVMsgSerdes, sp.ParNum),
			transaction.NewKVStoreChangelog(kvtabs.tab2,
				store_with_changelog.NewChangelogManager(personsStream, serdeFormat),
				sss.srcKVMsgSerdes, sp.ParNum),
		}
	} else if sp.TableType == uint8(store.MONGODB) {
		kvchangelogs = []*transaction.KVStoreChangelog{
			transaction.NewKVStoreChangelogForExternalStore(kvtabs.tab1, auctionsStream, joinProcSerialWithoutSink,
				&joinProcWithoutSinkArgs{
					src:    sss.src1.InnerSource(),
					parNum: sp.ParNum,
					runner: aJoinP,
				}, fmt.Sprintf("%s-%s-%d", h.funcName, kvtabs.tab1.Name(), sp.ParNum), sp.ParNum),
			transaction.NewKVStoreChangelogForExternalStore(kvtabs.tab2, personsStream, joinProcSerialWithoutSink,
				&joinProcWithoutSinkArgs{
					src:    sss.src2.InnerSource(),
					parNum: sp.ParNum,
					runner: pJoinA,
				}, fmt.Sprintf("%s-%s-%d", h.funcName, kvtabs.tab2.Name(), sp.ParNum), sp.ParNum),
		}
	}
	if sp.EnableTransaction {
		streamTaskArgs := transaction.StreamTaskArgsTransaction{
			ProcArgs:              procArgs,
			Env:                   h.env,
			Srcs:                  srcs,
			Sinks:                 sinks_arr,
			TransactionalId:       fmt.Sprintf("%s-%d", h.funcName, sp.ParNum),
			KVChangelogs:          kvchangelogs,
			WindowStoreChangelogs: nil,
			FixedOutParNum:        0,
		}
		benchutil.UpdateStreamTaskArgsTransaction(sp, &streamTaskArgs)
		ret := transaction.SetupManagersAndProcessTransactional(ctx, h.env, &streamTaskArgs,
			func(procArgs interface{}, trackParFunc tran_interface.TrackKeySubStreamFunc, recordFinishFunc transaction.RecordPrevInstanceFinishFunc) {
				joinProcPerson.trackParFunc = trackParFunc
				joinProcAuction.trackParFunc = trackParFunc
				procArgs.(*q3JoinTableProcessArgs).recordFinishFunc = recordFinishFunc
			}, &task)
		if ret != nil && ret.Success {
			ret.Latencies["auctionsSrc"] = sss.src1.GetLatency()
			ret.Latencies["personsSrc"] = sss.src2.GetLatency()
			ret.Latencies["toAuctionsTable"] = kvtabs.toTab1.GetLatency()
			ret.Latencies["toPersonsTable"] = kvtabs.toTab2.GetLatency()
			ret.Latencies["personJoinsAuctions"] = personJoinsAuctions.GetLatency()
			ret.Latencies["auctionJoinsPersons"] = auctionJoinsPersons.GetLatency()
			ret.Latencies["sink"] = sss.sink.GetLatency()
			ret.Latencies["eventTimeLatency"] = sss.sink.GetEventTimeLatency()
			ret.Consumed["auctionsSrc"] = sss.src1.GetCount()
			ret.Consumed["personsSrc"] = sss.src2.GetCount()
		}
		return ret
	}
	streamTaskArgs := transaction.NewStreamTaskArgs(h.env, procArgs, srcs, sinks_arr)
	streamTaskArgs.WithKVChangelogs(kvchangelogs)
	benchutil.UpdateStreamTaskArgs(sp, streamTaskArgs)
	ret := task.Process(ctx, streamTaskArgs)
	if ret != nil && ret.Success {
		ret.Latencies["auctionsSrc"] = sss.src1.GetLatency()
		ret.Latencies["personsSrc"] = sss.src2.GetLatency()
		ret.Latencies["toAuctionsTable"] = kvtabs.toTab1.GetLatency()
		ret.Latencies["toPersonsTable"] = kvtabs.toTab2.GetLatency()
		ret.Latencies["personJoinsAuctions"] = personJoinsAuctions.GetLatency()
		ret.Latencies["auctionJoinsPersons"] = auctionJoinsPersons.GetLatency()
		ret.Latencies["sink"] = sss.sink.GetLatency()
		ret.Latencies["eventTimeLatency"] = sss.sink.GetEventTimeLatency()
		ret.Consumed["auctionsSrc"] = sss.src1.GetCount()
		ret.Consumed["personsSrc"] = sss.src2.GetCount()
	}
	return ret
}
