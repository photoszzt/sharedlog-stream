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
	"sharedlog-stream/pkg/utils"
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
	return common.CompressData(encodedOutput), nil
}

func (h *q8JoinStreamHandler) getSrcSink(ctx context.Context, sp *common.QueryInput,
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
	msgSerde, err := commtypes.GetMsgSerdeG[uint64](serdeFormat, commtypes.Uint64SerdeG{}, eventSerde)
	if err != nil {
		return nil, nil, fmt.Errorf("get msg serde err: %v", err)
	}
	ptSerde, err := ntypes.GetPersonTimeSerdeG(serdeFormat)
	if err != nil {
		return nil, nil, err
	}
	outMsgSerde, err := commtypes.GetMsgSerdeG[uint64](serdeFormat, commtypes.Uint64SerdeG{}, ptSerde)
	if err != nil {
		return nil, nil, err
	}

	timeout := time.Duration(4) * time.Millisecond
	warmup := time.Duration(sp.WarmupS) * time.Second
	consumer1, err := producer_consumer.NewShardedSharedLogStreamConsumerG(stream1,
		&producer_consumer.StreamConsumerConfigG[uint64, *ntypes.Event]{
			Timeout:     timeout,
			MsgSerde:    msgSerde,
			SerdeFormat: serdeFormat,
		}, sp.NumSubstreamProducer[0], sp.ParNum)
	if err != nil {
		return nil, nil, err
	}
	consumer2, err := producer_consumer.NewShardedSharedLogStreamConsumerG(stream2,
		&producer_consumer.StreamConsumerConfigG[uint64, *ntypes.Event]{
			Timeout:     timeout,
			MsgSerde:    msgSerde,
			SerdeFormat: serdeFormat,
		}, sp.NumSubstreamProducer[1], sp.ParNum)
	if err != nil {
		return nil, nil, err
	}
	src1 := producer_consumer.NewMeteredConsumer(consumer1, warmup)
	src2 := producer_consumer.NewMeteredConsumer(consumer2, warmup)
	sink := producer_consumer.NewConcurrentMeteredSyncProducer(producer_consumer.NewShardedSharedLogStreamProducer(outputStream,
		&producer_consumer.StreamSinkConfig[uint64, ntypes.PersonTime]{
			MsgSerde:      outMsgSerde,
			FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
		}), warmup)
	src1.SetInitialSource(false)
	src2.SetInitialSource(false)
	src1.SetName("auctionsBySellerIDSrc")
	src2.SetName("personsByIDSrc")
	sink.MarkFinalOutput()
	return []producer_consumer.MeteredConsumerIntr{src1, src2}, []producer_consumer.MeteredProducerIntr{sink}, nil
}

func (h *q8JoinStreamHandler) Query8JoinStream(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	debug.Assert(sp.ScaleEpoch != 0, "scale epoch should start from 1")
	srcs, sinks_arr, err := h.getSrcSink(ctx, sp)
	if err != nil {
		return &common.FnOutput{Success: false, Message: fmt.Sprintf("getSrcSink err: %v\n", err)}
	}
	joinWindows, err := processor.NewJoinWindowsWithGrace(time.Duration(10)*time.Second, time.Duration(5)*time.Second)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	windowSizeMs := int64(10 * 1000)
	joiner := processor.ValueJoinerWithKeyTsFuncG[uint64, *ntypes.Event, *ntypes.Event, ntypes.PersonTime](
		func(readOnlyKey uint64, leftValue *ntypes.Event, rightValue *ntypes.Event,
			leftTs int64, rightTs int64,
		) ntypes.PersonTime {
			// fmt.Fprint(os.Stderr, "get into joiner\n")
			ts := rightValue.NewPerson.DateTime
			windowStart := (utils.MaxInt64(0, ts-windowSizeMs+windowSizeMs) / windowSizeMs) * windowSizeMs
			return ntypes.PersonTime{
				ID:        rightValue.NewPerson.ID,
				Name:      rightValue.NewPerson.Name,
				StartTime: windowStart,
			}
		})
	format := commtypes.SerdeFormat(sp.SerdeFormat)
	flushDur := time.Duration(sp.FlushMs) * time.Millisecond
	eventSerde, err := ntypes.GetEventSerdeG(format)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	msgSerde, err := commtypes.GetMsgSerdeG[uint64](format, commtypes.Uint64SerdeG{}, eventSerde)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	aucMp, err := store_with_changelog.NewMaterializeParamBuilder[uint64, *ntypes.Event]().
		MessageSerde(msgSerde).
		StoreName("auctionsBySellerIDWinTab").
		ParNum(sp.ParNum).
		SerdeFormat(format).
		ChangelogManagerParam(commtypes.CreateChangelogManagerParam{
			Env:           h.env,
			NumPartition:  sp.NumInPartition,
			FlushDuration: flushDur,
			TimeOut:       common.SrcConsumeTimeout,
		}).Build()
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	perMp, err := store_with_changelog.NewMaterializeParamBuilder[uint64, *ntypes.Event]().
		MessageSerde(msgSerde).
		StoreName("personsByIDWinTab").
		ParNum(sp.ParNum).
		SerdeFormat(format).
		ChangelogManagerParam(commtypes.CreateChangelogManagerParam{
			Env:           h.env,
			NumPartition:  sp.NumInPartition,
			FlushDuration: flushDur,
			TimeOut:       common.SrcConsumeTimeout,
		}).Build()
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	aucJoinsPerFunc, perJoinsAucFunc, wsc, err := execution.SetupSkipMapStreamStreamJoin(
		aucMp, perMp, store.IntegerCompare[uint64], joiner, joinWindows)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	task, procArgs := execution.PrepareTaskWithJoin(ctx,
		execution.JoinWorkerFunc(aucJoinsPerFunc),
		execution.JoinWorkerFunc(perJoinsAucFunc),
		proc_interface.NewBaseSrcsSinks(srcs, sinks_arr),
		proc_interface.NewBaseProcArgs(h.funcName, sp.ScaleEpoch, sp.ParNum), true)
	streamTaskArgs := benchutil.UpdateStreamTaskArgs(sp,
		stream_task.NewStreamTaskArgsBuilder(h.env, procArgs, fmt.Sprintf("%s-%d", h.funcName, sp.ParNum))).
		WindowStoreChangelogs(wsc).FixedOutParNum(sp.ParNum).Build()
	return stream_task.ExecuteApp(ctx, task, streamTaskArgs)
}
