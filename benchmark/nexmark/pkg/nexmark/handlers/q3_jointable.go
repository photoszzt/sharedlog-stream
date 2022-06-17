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
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/execution"
	"sharedlog-stream/pkg/proc_interface"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/producer_consumer"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/store_restore"
	"sharedlog-stream/pkg/store_with_changelog"
	"sharedlog-stream/pkg/stream_task"
	"sharedlog-stream/pkg/treemap"
	"sync"
	"time"

	"cs.utexas.edu/zjia/faas/types"
)

type q3JoinTableHandler struct {
	env      types.Environment
	funcName string
}

func NewQ3JoinTableHandler(env types.Environment, funcName string) types.FuncHandler {
	return &q3JoinTableHandler{
		env:      env,
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
	t *stream_task.StreamTask,
	argsTmp interface{},
) *common.FnOutput {
	return execution.HandleJoinErrReturn(argsTmp)
}

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

func (h *q3JoinTableHandler) getSrcSink(ctx context.Context, sp *common.QueryInput,
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
	ncsiSerde, err := ntypes.GetNameCityStateIdSerde(serdeFormat)
	if err != nil {
		return nil, nil, err
	}

	src1 := producer_consumer.NewMeteredConsumer(producer_consumer.NewShardedSharedLogStreamConsumer(stream1, &producer_consumer.StreamConsumerConfig{
		Timeout:     timeout,
		KVMsgSerdes: kvmsgSerdes,
	}), warmup)
	src2 := producer_consumer.NewMeteredConsumer(producer_consumer.NewShardedSharedLogStreamConsumer(stream2, &producer_consumer.StreamConsumerConfig{
		Timeout:     timeout,
		KVMsgSerdes: kvmsgSerdes,
	}), warmup)
	src1.SetInitialSource(false)
	src2.SetInitialSource(false)
	sink := producer_consumer.NewConcurrentMeteredSyncProducer(producer_consumer.NewShardedSharedLogStreamProducer(outputStream, &producer_consumer.StreamSinkConfig{
		KVMsgSerdes: commtypes.KVMsgSerdes{
			KeySerde: commtypes.Uint64Serde{},
			ValSerde: ncsiSerde,
			MsgSerde: msgSerde,
		},
		FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
	}), warmup)
	sink.MarkFinalOutput()
	return []producer_consumer.MeteredConsumerIntr{src1, src2}, []producer_consumer.MeteredProducerIntr{sink}, nil
}

type kvtables struct {
	toTab1 *processor.MeteredProcessor
	tab1   store.KeyValueStore
	toTab2 *processor.MeteredProcessor
	tab2   store.KeyValueStore
}

func (h *q3JoinTableHandler) setupTables(ctx context.Context,
	tabType store.TABLE_TYPE,
	srcs []producer_consumer.MeteredConsumerIntr,
	serdeFormat commtypes.SerdeFormat,
	mongoAddr string,
	warmup time.Duration,
) (
	*kvtables,
	error,
) {
	aucTabName := "auctionsBySellerIDStore"
	perTabName := "personsByIDStore"
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
		toAuctionsTable, auctionsStore := processor.ToInMemKVTable(
			aucTabName, compare, warmup)
		toPersonsTable, personsStore := processor.ToInMemKVTable(
			perTabName, compare, warmup)
		return &kvtables{toAuctionsTable, auctionsStore, toPersonsTable, personsStore}, nil
	} else if tabType == store.MONGODB {
		src1ValSerde := srcs[0].KVMsgSerdes().ValSerde
		src2ValSerde := srcs[1].KVMsgSerdes().ValSerde
		vtSerde0, err := commtypes.GetValueTsSerde(serdeFormat,
			src1ValSerde, src1ValSerde)
		if err != nil {
			return nil, err
		}
		vtSerde1, err := commtypes.GetValueTsSerde(serdeFormat,
			src2ValSerde, src2ValSerde)
		if err != nil {
			return nil, err
		}
		client, err := store.InitMongoDBClient(ctx, mongoAddr)
		if err != nil {
			return nil, err
		}
		toAuctionsTable, auctionsStore, err := processor.ToMongoDBKVTable(
			ctx, aucTabName,
			client, srcs[0].KVMsgSerdes().KeySerde, vtSerde0, warmup)
		if err != nil {
			return nil, err
		}
		toPersonsTable, personsStore, err := processor.ToMongoDBKVTable(ctx, perTabName,
			client, srcs[1].KVMsgSerdes().KeySerde, vtSerde1, warmup)
		if err != nil {
			return nil, err
		}
		return &kvtables{toAuctionsTable, auctionsStore, toPersonsTable, personsStore}, nil
	} else {
		return nil, fmt.Errorf("unrecognized table type")
	}
}

func (h *q3JoinTableHandler) Query3JoinTable(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	srcs, sinks_arr, err := h.getSrcSink(ctx, sp)
	if err != nil {
		return &common.FnOutput{Success: false, Message: fmt.Sprintf("getSrcSink err: %v\n", err)}
	}
	srcs[0].SetName("auctionsSrc")
	srcs[1].SetName("personsSrc")
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)

	kvtabs, err := h.setupTables(ctx, store.TABLE_TYPE(sp.TableType), srcs, serdeFormat, sp.MongoAddr, time.Duration(sp.WarmupS)*time.Second)
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

	aJoinP := execution.JoinWorkerFunc(func(c context.Context, m commtypes.Message) ([]commtypes.Message, error) {
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
	pJoinA := execution.JoinWorkerFunc(func(ctx context.Context, m commtypes.Message) ([]commtypes.Message, error) {
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

	debug.Assert(sp.ScaleEpoch != 0, "scale epoch should start from 1")

	joinProcAuction, joinProcPerson := execution.CreateJoinProcArgsPair(
		aJoinP, pJoinA, srcs, sinks_arr,
		proc_interface.NewBaseProcArgs(h.funcName, sp.ScaleEpoch, sp.ParNum))
	var wg sync.WaitGroup
	aucManager := execution.NewJoinProcManager()
	perManager := execution.NewJoinProcManager()

	procArgs := execution.NewCommonJoinProcArgs(
		joinProcAuction, joinProcPerson,
		aucManager.Out(), perManager.Out(),
		proc_interface.NewBaseSrcsSinks(srcs, sinks_arr))

	pctx := context.WithValue(ctx, "id", "person")
	actx := context.WithValue(ctx, "id", "auction")

	task := stream_task.NewStreamTaskBuilder().
		AppProcessFunc(h.process).
		InitFunc(func(procArgsTmp interface{}) {
			kvtabs.toTab1.StartWarmup()
			kvtabs.toTab2.StartWarmup()
			auctionJoinsPersons.StartWarmup()
			personJoinsAuctions.StartWarmup()

			aucManager.Run()
			perManager.Run()
		}).
		PauseFunc(func() *common.FnOutput {
			aucManager.RequestToTerminate()
			perManager.RequestToTerminate()
			debug.Fprintf(os.Stderr, "q3 waiting for join proc to exit\n")
			wg.Wait()
			debug.Fprintf(os.Stderr, "down pause\n")
			// check the goroutine's return in case any of them returns output
			ret := execution.HandleJoinErrReturn(procArgs)
			if ret != nil {
				return ret
			}
			return nil
		}).
		ResumeFunc(func(task *stream_task.StreamTask) {
			debug.Fprintf(os.Stderr, "resume begin\n")
			aucManager.LaunchJoinProcLoop(actx, task, joinProcAuction, &wg)
			perManager.LaunchJoinProcLoop(pctx, task, joinProcPerson, &wg)

			aucManager.Run()
			perManager.Run()
			debug.Fprintf(os.Stderr, "resume done\n")
		}).Build()

	aucManager.LaunchJoinProcLoop(actx, task, joinProcAuction, &wg)
	perManager.LaunchJoinProcLoop(pctx, task, joinProcPerson, &wg)

	var kvchangelogs []*store_restore.KVStoreChangelog
	if sp.TableType == uint8(store.IN_MEM) {
		kvchangelogs = []*store_restore.KVStoreChangelog{
			store_restore.NewKVStoreChangelog(kvtabs.tab1,
				store_with_changelog.NewChangelogManagerForSrc(
					srcs[0].Stream().(*sharedlog_stream.ShardedSharedLogStream),
					srcs[0].KVMsgSerdes(), common.SrcConsumeTimeout), sp.ParNum),
			store_restore.NewKVStoreChangelog(kvtabs.tab2,
				store_with_changelog.NewChangelogManagerForSrc(
					srcs[1].Stream().(*sharedlog_stream.ShardedSharedLogStream),
					srcs[1].KVMsgSerdes(), common.SrcConsumeTimeout), sp.ParNum),
		}
	} else if sp.TableType == uint8(store.MONGODB) {
		kvchangelogs = []*store_restore.KVStoreChangelog{
			store_restore.NewKVStoreChangelogForExternalStore(kvtabs.tab1, srcs[0].Stream(), execution.JoinProcSerialWithoutSink,
				execution.NewJoinProcWithoutSinkArgs(srcs[1].InnerSource(), aJoinP, sp.ParNum),
				fmt.Sprintf("%s-%s-%d", h.funcName, kvtabs.tab1.Name(), sp.ParNum), sp.ParNum),
			store_restore.NewKVStoreChangelogForExternalStore(kvtabs.tab2, srcs[1].Stream(), execution.JoinProcSerialWithoutSink,
				execution.NewJoinProcWithoutSinkArgs(srcs[1].InnerSource(), pJoinA, sp.ParNum),
				fmt.Sprintf("%s-%s-%d", h.funcName, kvtabs.tab2.Name(), sp.ParNum), sp.ParNum),
		}
	}
	update_stats := func(ret *common.FnOutput) {
		ret.Latencies["toAuctionsTable"] = kvtabs.toTab1.GetLatency()
		ret.Latencies["toPersonsTable"] = kvtabs.toTab2.GetLatency()
		ret.Latencies["personJoinsAuctions"] = personJoinsAuctions.GetLatency()
		ret.Latencies["auctionJoinsPersons"] = auctionJoinsPersons.GetLatency()
		ret.Latencies["eventTimeLatency"] = sinks_arr[0].GetEventTimeLatency()
	}
	transactionalID := fmt.Sprintf("%s-%d", h.funcName, sp.ParNum)
	streamTaskArgs := benchutil.UpdateStreamTaskArgs(sp,
		stream_task.NewStreamTaskArgsBuilder(h.env, procArgs, transactionalID)).
		KVStoreChangelogs(kvchangelogs).
		FixedOutParNum(sp.ParNum).
		Build()
	return task.ExecuteApp(ctx, streamTaskArgs, update_stats)
}
