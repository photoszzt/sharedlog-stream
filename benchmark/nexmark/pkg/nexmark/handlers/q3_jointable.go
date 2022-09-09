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
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/store_with_changelog"
	"sharedlog-stream/pkg/stream_task"
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
	return common.CompressData(encodedOutput), nil
}

func getInOutStreams(
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
) ([]*producer_consumer.MeteredConsumer, []producer_consumer.MeteredProducerIntr, error) {
	stream1, stream2, outputStream, err := getInOutStreams(h.env, sp)
	if err != nil {
		return nil, nil, err
	}
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)

	timeout := time.Duration(4) * time.Millisecond
	warmup := time.Duration(sp.WarmupS) * time.Second
	inConfig := &producer_consumer.StreamConsumerConfig{
		Timeout:     timeout,
		SerdeFormat: serdeFormat,
	}
	consumer1, err := producer_consumer.NewShardedSharedLogStreamConsumer(stream1, inConfig,
		sp.NumSubstreamProducer[0], sp.ParNum)
	if err != nil {
		return nil, nil, err
	}
	consumer2, err := producer_consumer.NewShardedSharedLogStreamConsumer(stream2, inConfig,
		sp.NumSubstreamProducer[1], sp.ParNum)
	if err != nil {
		return nil, nil, err
	}
	src1 := producer_consumer.NewMeteredConsumer(consumer1, warmup)
	src2 := producer_consumer.NewMeteredConsumer(consumer2, warmup)
	src1.SetInitialSource(false)
	src2.SetInitialSource(false)
	sink, err := producer_consumer.NewConcurrentMeteredSyncProducer(producer_consumer.NewShardedSharedLogStreamProducer(outputStream,
		&producer_consumer.StreamSinkConfig{
			FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
			Format:        serdeFormat,
		}), warmup)
	if err != nil {
		return nil, nil, err
	}
	sink.MarkFinalOutput()
	return []*producer_consumer.MeteredConsumer{src1, src2}, []producer_consumer.MeteredProducerIntr{sink}, nil
}

func (h *q3JoinTableHandler) Query3JoinTable(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	eventSerde, err := ntypes.GetEventSerdeG(serdeFormat)
	if err != nil {
		return &common.FnOutput{Success: false, Message: fmt.Sprintf("get event serde err: %v", err)}
	}
	inMsgSerde, err := commtypes.GetMsgGSerdeG[uint64](serdeFormat, commtypes.Uint64SerdeG{}, eventSerde)
	if err != nil {
		return &common.FnOutput{Success: false, Message: fmt.Sprintf("get msg serde err: %v", err)}
	}
	ncsiSerde, err := ntypes.GetNameCityStateIdSerdeG(serdeFormat)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	outMsgSerde, err := commtypes.GetMsgGSerdeG[uint64](serdeFormat, commtypes.Uint64SerdeG{}, ncsiSerde)
	if err != nil {
		return common.GenErrFnOutput(err)
	}

	srcs, sinks_arr, err := h.getSrcSink(ctx, sp)
	if err != nil {
		return &common.FnOutput{Success: false, Message: fmt.Sprintf("getSrcSink err: %v\n", err)}
	}
	srcs[0].SetName("auctionsSrc")
	srcs[1].SetName("personsSrc")
	debug.Assert(len(sp.NumOutPartitions) == 1 && len(sp.OutputTopicNames) == 1,
		"expected only one output stream")
	debug.Assert(sp.ScaleEpoch != 0, "scale epoch should start from 1")
	flushDur := time.Duration(sp.FlushMs) * time.Millisecond

	storeMsgSerde, err := processor.MsgSerdeWithValueTsG[uint64](serdeFormat, commtypes.Uint64SerdeG{}, eventSerde)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	joiner := processor.ValueJoinerWithKeyFuncG[uint64, *ntypes.Event, *ntypes.Event, ntypes.NameCityStateId](
		func(_ uint64, _ *ntypes.Event, rightVal *ntypes.Event) ntypes.NameCityStateId {
			ncsi := ntypes.NameCityStateId{
				Name:  rightVal.NewPerson.Name,
				City:  rightVal.NewPerson.City,
				State: rightVal.NewPerson.State,
				ID:    rightVal.NewPerson.ID,
			}
			// debug.Fprintf(os.Stderr, "join outputs: %v\n", ncsi)
			return ncsi
		})
	mpAuc, err := store_with_changelog.NewMaterializeParamBuilder[uint64, commtypes.ValueTimestampG[*ntypes.Event]]().
		MessageSerde(storeMsgSerde).
		StoreName("auctionsBySellerIDStore").
		ParNum(sp.ParNum).
		SerdeFormat(serdeFormat).
		ChangelogManagerParam(commtypes.CreateChangelogManagerParam{
			Env:           h.env,
			NumPartition:  sp.NumInPartition,
			FlushDuration: flushDur,
			TimeOut:       common.SrcConsumeTimeout,
		}).Build()
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	mpPer, err := store_with_changelog.NewMaterializeParamBuilder[uint64, commtypes.ValueTimestampG[*ntypes.Event]]().
		MessageSerde(storeMsgSerde).
		StoreName("personsByIDStore").
		ParNum(sp.ParNum).
		SerdeFormat(serdeFormat).
		ChangelogManagerParam(commtypes.CreateChangelogManagerParam{
			Env:           h.env,
			NumPartition:  sp.NumInPartition,
			FlushDuration: flushDur,
			TimeOut:       common.SrcConsumeTimeout,
		}).Build()
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	aucJoinsPerFunc, perJoinsAucFunc, kvc, setupSnapFunc, err := execution.SetupTableTableJoinWithSkipmap(
		mpAuc, mpPer, store.Uint64LessFunc, joiner)
	if err != nil {
		return common.GenErrFnOutput(err)
	}

	toStream := processor.NewTableToStreamProcessorG[uint64, ntypes.NameCityStateId]()

	aJoinP := execution.JoinWorkerFunc[uint64, *ntypes.Event, uint64, ntypes.NameCityStateId](
		func(ctx context.Context, m commtypes.MessageG[uint64, *ntypes.Event]) (
			[]commtypes.MessageG[uint64, ntypes.NameCityStateId], error,
		) {
			// msg is auction
			ret, err := aucJoinsPerFunc(ctx, m)
			if err != nil {
				return nil, err
			}
			if ret != nil {
				return toStream.ProcessAndReturn(ctx, ret[0])
			}
			return nil, nil
		})
	pJoinA := execution.JoinWorkerFunc[uint64, *ntypes.Event, uint64, ntypes.NameCityStateId](
		func(ctx context.Context, m commtypes.MessageG[uint64, *ntypes.Event]) (
			[]commtypes.MessageG[uint64, ntypes.NameCityStateId], error,
		) {
			// msg is person
			ret, err := perJoinsAucFunc(ctx, m)
			if err != nil {
				return nil, fmt.Errorf("ToTabP err: %v", err)
			}
			if ret != nil {
				return toStream.ProcessAndReturn(ctx, ret[0])
			}
			return nil, nil
		})
	msgSerdePair := execution.NewMsgSerdePair(inMsgSerde, outMsgSerde)
	task, procArgs := execution.PrepareTaskWithJoin(ctx, aJoinP, pJoinA,
		proc_interface.NewBaseSrcsSinks(srcs, sinks_arr),
		proc_interface.NewBaseProcArgs(h.funcName, sp.ScaleEpoch, sp.ParNum),
		true, msgSerdePair, msgSerdePair)
	transactionalID := fmt.Sprintf("%s-%d", h.funcName, sp.ParNum)
	streamTaskArgs := benchutil.UpdateStreamTaskArgs(sp,
		stream_task.NewStreamTaskArgsBuilder(h.env, procArgs, transactionalID)).
		KVStoreChangelogs(kvc).FixedOutParNum(sp.ParNum).Build()
	return stream_task.ExecuteApp(ctx, task, streamTaskArgs, setupSnapFunc)
}
