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
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/execution"
	"sharedlog-stream/pkg/proc_interface"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/producer_consumer"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/store_restore"
	"sharedlog-stream/pkg/store_with_changelog"
	"sharedlog-stream/pkg/stream_task"
	"sharedlog-stream/pkg/treemap"
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
	eventSerde, err := ntypes.GetEventSerde(serdeFormat)
	if err != nil {
		return nil, nil, fmt.Errorf("get event serde err: %v", err)
	}
	inMsgSerde, err := commtypes.GetMsgSerde(serdeFormat, commtypes.Uint64Serde{}, eventSerde)
	if err != nil {
		return nil, nil, fmt.Errorf("get msg serde err: %v", err)
	}

	timeout := common.SrcConsumeTimeout
	warmup := time.Duration(sp.WarmupS) * time.Second
	ncsiSerde, err := ntypes.GetNameCityStateIdSerde(serdeFormat)
	if err != nil {
		return nil, nil, err
	}
	outMsgSerde, err := commtypes.GetMsgSerde(serdeFormat, commtypes.Uint64Serde{}, ncsiSerde)
	if err != nil {
		return nil, nil, err
	}
	src1 := producer_consumer.NewMeteredConsumer(producer_consumer.NewShardedSharedLogStreamConsumer(stream1, &producer_consumer.StreamConsumerConfig{
		Timeout:  timeout,
		MsgSerde: inMsgSerde,
	}), warmup)
	src2 := producer_consumer.NewMeteredConsumer(producer_consumer.NewShardedSharedLogStreamConsumer(stream2, &producer_consumer.StreamConsumerConfig{
		Timeout:  timeout,
		MsgSerde: inMsgSerde,
	}), warmup)
	src1.SetInitialSource(false)
	src2.SetInitialSource(false)
	sink := producer_consumer.NewConcurrentMeteredSyncProducer(producer_consumer.NewShardedSharedLogStreamProducer(outputStream, &producer_consumer.StreamSinkConfig{
		MsgSerde:      outMsgSerde,
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
	srcs []producer_consumer.MeteredConsumerIntr,
	serdeFormat commtypes.SerdeFormat,
) (*kvtables, error) {
	aucTabName := "auctionsBySellerIDStore"
	perTabName := "personsByIDStore"
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
		aucTabName, compare)
	toPersonsTable, personsStore := processor.ToInMemKVTable(
		perTabName, compare)
	return &kvtables{toAuctionsTable, auctionsStore, toPersonsTable, personsStore}, nil
}

func (h *q3JoinTableHandler) Query3JoinTable(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	srcs, sinks_arr, err := h.getSrcSink(ctx, sp)
	if err != nil {
		return &common.FnOutput{Success: false, Message: fmt.Sprintf("getSrcSink err: %v\n", err)}
	}
	srcs[0].SetName("auctionsSrc")
	srcs[1].SetName("personsSrc")
	debug.Assert(len(sp.NumOutPartitions) == 1 && len(sp.OutputTopicNames) == 1,
		"expected only one output stream")
	debug.Assert(sp.ScaleEpoch != 0, "scale epoch should start from 1")
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)

	kvtabs, err := h.setupTables(ctx, srcs, serdeFormat)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	joiner := processor.ValueJoinerWithKeyFunc(
		func(readOnlyKey interface{}, leftVal interface{}, rightVal interface{}) interface{} {
			event := rightVal.(*ntypes.Event)
			ncsi := &ntypes.NameCityStateId{
				Name:  event.NewPerson.Name,
				City:  event.NewPerson.City,
				State: event.NewPerson.State,
				ID:    event.NewPerson.ID,
			}
			// debug.Fprintf(os.Stderr, "join outputs: %v\n", ncsi)
			return ncsi
		})

	auctionJoinsPersons := processor.NewMeteredProcessor(
		processor.NewTableTableJoinProcessor("auctionJoinsPersons", kvtabs.tab2, joiner))

	personJoinsAuctions := processor.NewMeteredProcessor(
		processor.NewTableTableJoinProcessor("personJoinsAuctions", kvtabs.tab1,
			processor.ReverseValueJoinerWithKey(joiner)))
	toStream := processor.NewTableToStreamProcessor()

	aJoinP := execution.JoinWorkerFunc(func(c context.Context, m commtypes.Message) ([]commtypes.Message, error) {
		// msg is auction
		ret, err := kvtabs.toTab1.ProcessAndReturn(ctx, m)
		if err != nil {
			return nil, fmt.Errorf("ToTabA err: %v", err)
		}
		if ret != nil {
			joinMsgs, err := auctionJoinsPersons.ProcessAndReturn(ctx, ret[0])
			if err != nil {
				err = fmt.Errorf("aJoinP err: %v", err)
			}
			if joinMsgs != nil {
				return toStream.ProcessAndReturn(ctx, joinMsgs[0])
			}
		}
		return nil, nil
	})
	pJoinA := execution.JoinWorkerFunc(func(ctx context.Context, m commtypes.Message) ([]commtypes.Message, error) {
		// msg is person
		ret, err := kvtabs.toTab2.ProcessAndReturn(ctx, m)
		if err != nil {
			return nil, fmt.Errorf("ToTabP err: %v", err)
		}
		if ret != nil {
			joinMsgs, err := personJoinsAuctions.ProcessAndReturn(ctx, ret[0])
			if err != nil {
				err = fmt.Errorf("pJoinA err: %v", err)
			}
			if joinMsgs != nil {
				return toStream.ProcessAndReturn(ctx, joinMsgs[0])
			}
		}
		return nil, nil
	})
	task, procArgs := execution.PrepareTaskWithJoin(ctx, aJoinP, pJoinA,
		proc_interface.NewBaseSrcsSinks(srcs, sinks_arr),
		proc_interface.NewBaseProcArgs(h.funcName, sp.ScaleEpoch, sp.ParNum),
	)
	kvchangelogs := []*store_restore.KVStoreChangelog{
		store_restore.NewKVStoreChangelog(kvtabs.tab1,
			store_with_changelog.NewChangelogManagerForSrc(
				srcs[0].Stream().(*sharedlog_stream.ShardedSharedLogStream),
				srcs[0].MsgSerde(), common.SrcConsumeTimeout)),
		store_restore.NewKVStoreChangelog(kvtabs.tab2,
			store_with_changelog.NewChangelogManagerForSrc(
				srcs[1].Stream().(*sharedlog_stream.ShardedSharedLogStream),
				srcs[1].MsgSerde(), common.SrcConsumeTimeout)),
	}
	transactionalID := fmt.Sprintf("%s-%d", h.funcName, sp.ParNum)
	streamTaskArgs := benchutil.UpdateStreamTaskArgs(sp,
		stream_task.NewStreamTaskArgsBuilder(h.env, procArgs, transactionalID)).
		KVStoreChangelogs(kvchangelogs).FixedOutParNum(sp.ParNum).Build()
	return task.ExecuteApp(ctx, streamTaskArgs)
}
