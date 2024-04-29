package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	"sharedlog-stream/benchmark/tests/pkg/tests/test_types"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/exactly_once_intr"
	"sharedlog-stream/pkg/execution"
	"sharedlog-stream/pkg/optional"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/producer_consumer"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/store_with_changelog"
	"sharedlog-stream/pkg/stream_task"
	"time"

	"cs.utexas.edu/zjia/faas/types"
)

type joinHandler struct {
	env types.Environment
}

func NewJoinHandler(env types.Environment) types.FuncHandler {
	return &joinHandler{
		env: env,
	}
}

func (h *joinHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &test_types.TestInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output := h.tests(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	return common.CompressData(encodedOutput), nil
}

func (h *joinHandler) tests(ctx context.Context, sp *test_types.TestInput) *common.FnOutput {
	switch sp.TestName {
	case "streamStreamJoinMem":
		h.testStreamStreamJoinMem(ctx)
	default:
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("test not recognized: %s\n", sp.TestName),
		}
	}
	return &common.FnOutput{
		Success: true,
		Message: fmt.Sprintf("%s done", sp.TestName),
	}
}

func (h *joinHandler) getSrcSink(ctx context.Context, sp *common.TestParam,
) ([]*producer_consumer.MeteredConsumer, []producer_consumer.MeteredProducerIntr, error) {
	srcStreams, sinkStreams, err := benchutil.GetShardedInputOutputStreamsTest(ctx, h.env, sp)
	if err != nil {
		panic(err)
	}
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	timeout := time.Duration(10) * time.Millisecond
	srcConfig := &producer_consumer.StreamConsumerConfig{
		Timeout:     timeout,
		SerdeFormat: serdeFormat,
	}
	outConfig := &producer_consumer.StreamSinkConfig{
		FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
		Format:        serdeFormat,
	}
	warmup := time.Duration(0) * time.Second
	consumer1, err := producer_consumer.NewShardedSharedLogStreamConsumer(srcStreams[0],
		srcConfig, 1, 0)
	if err != nil {
		return nil, nil, err
	}
	consumer2, err := producer_consumer.NewShardedSharedLogStreamConsumer(srcStreams[1],
		srcConfig, 1, 0)
	if err != nil {
		return nil, nil, err
	}
	src1 := producer_consumer.NewMeteredConsumer(consumer1, warmup)
	src2 := producer_consumer.NewMeteredConsumer(consumer2, warmup)
	sink, err := producer_consumer.NewConcurrentMeteredSyncProducer(
		producer_consumer.NewShardedSharedLogStreamProducer(sinkStreams[0], outConfig), warmup)
	if err != nil {
		return nil, nil, err
	}
	src1.SetInitialSource(false)
	src2.SetInitialSource(false)
	src1.SetName("src1")
	src2.SetName("src2")
	return []*producer_consumer.MeteredConsumer{src1, src2}, []producer_consumer.MeteredProducerIntr{sink}, nil
}

func getMaterializedParam[K, V any](storeName string,
	kvMsgSerde commtypes.MessageGSerdeG[K, V],
	env types.Environment,
	sp *common.TestParam,
) (*store_with_changelog.MaterializeParam[K, V], error) {
	return store_with_changelog.NewMaterializeParamBuilder[K, V]().
		MessageSerde(kvMsgSerde).
		StoreName(storeName).
		ParNum(0).
		SerdeFormat(commtypes.SerdeFormat(sp.SerdeFormat)).
		ChangelogManagerParam(commtypes.CreateChangelogManagerParam{
			Env:           env,
			NumPartition:  1,
			TimeOut:       common.SrcConsumeTimeout,
			FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
		}).BufMaxSize(sp.BufMaxSize).Build()
}

func (h *joinHandler) testStreamStreamJoinMem(ctx context.Context) {
	default_buf_size := uint32(128 * 1024)
	sp := &common.TestParam{
		InStreamParam: []common.TestStreamParam{
			{TopicName: "src1", NumPartition: 1},
			{TopicName: "src2", NumPartition: 1},
		},
		OutStreamParam: []common.TestStreamParam{
			{TopicName: "sink", NumPartition: 1},
		},
		AppId:         "testStreamStreamJoinMem",
		CommitEveryMs: 100,
		FlushMs:       100,
		BufMaxSize:    default_buf_size,
		SerdeFormat:   uint8(commtypes.JSON),
	}
	srcs, sinks, err := h.getSrcSink(ctx, sp)
	msgSerde, err := processor.MsgSerdeWithValueTsG(commtypes.JSON, commtypes.IntSerdeG{}, commtypes.StringSerdeG{})
	if err != nil {
		panic(err)
	}

	joinWindows, err := commtypes.NewJoinWindowsWithGrace(time.Duration(50)*time.Millisecond,
		time.Duration(50)*time.Millisecond)
	if err != nil {
		panic(err)
	}
	joiner := processor.ValueJoinerWithKeyTsFuncG[int, string, string, string](
		func(readOnlyKey int,
			leftValue string, rightValue string, leftTs int64, rightTs int64,
		) optional.Option[string] {
			debug.Fprintf(os.Stderr, "left val: %v, ts: %d, right val: %v, ts: %d\n", leftValue, leftTs, rightValue, rightTs)
			return optional.Some(fmt.Sprintf("%s+%s", leftValue, rightValue))
		})
	oneMp, err := getMaterializedParam[int, string]("oneStore", msgSerde, h.env, sp)
	if err != nil {
		panic(err)
	}
	twoMp, err := getMaterializedParam[int, string]("twoStore", msgSerde, h.env, sp)
	if err != nil {
		panic(err)
	}
	oneJoinTwoProc, twoJoinOneProc, wsos, setupSnapCallbackFunc, err := execution.SetupSkipMapStreamStreamJoin(
		oneMp, twoMp, store.IntegerCompare[int], joiner, joinWindows,
		exactly_once_intr.TWO_PHASE_COMMIT,
	)
	oneJoinTwo := func(ctx context.Context, m commtypes.MessageG[int, string], sink *producer_consumer.ShardedSharedLogStreamProducer,
		trackParFunc exactly_once_intr.TrackProdSubStreamFunc,
	) error {
		joinedMsgs, err := oneJoinTwoProc(ctx, m)
		if err != nil {
			return err
		}
		return pushMsgsToSink(ctx, sink, joinedMsgs, trackParFunc)
	}
	twoJoinOne := func(ctx context.Context, m commtypes.MessageG[int, string], sink *producer_consumer.ShardedSharedLogStreamProducer,
		trackParFunc exactly_once_intr.TrackProdSubStreamFunc,
	) error {
		joinedMsgs, err := twoJoinOneProc(ctx, m)
		if err != nil {
			return err
		}
		return pushMsgsToSink(ctx, sink, joinedMsgs, trackParFunc)
	}

	payloadArrSerde := sharedlog_stream.DEFAULT_PAYLOAD_ARR_SERDEG
	streamTaskArgs, err := benchutil.UpdateStreamTaskArgs(&common.QueryInput{},
		stream_task.NewStreamTaskArgsBuilder(h.env, nil, "joinTestMem")).Build()
	if err != nil {
		panic(err)
	}
	tm, err := stream_task.SetupManagersFor2pcTest(ctx, streamTaskArgs)
	if err != nil {
		panic(err)
	}
	expected_keys := []int{0, 1, 2, 3}

	srcStream1 := srcs[0].Stream().(*sharedlog_stream.ShardedSharedLogStream)
	srcStream2 := srcs[1].Stream().(*sharedlog_stream.ShardedSharedLogStream)
	for i := 0; i < 2; i++ {
		err := pushMsgToStream(ctx, expected_keys[i],
			commtypes.ValueTimestampG[string]{Value: fmt.Sprintf("A%d", expected_keys[i]), Timestamp: 0},
			msgSerde, srcs[0].Stream().(*sharedlog_stream.ShardedSharedLogStream), tm.GetProducerId())
		if err != nil {
			panic(err)
		}
	}

	if _, _, err = tm.CommitTransaction(ctx); err != nil {
		panic(err)
	}

	got, err := readMsgs[int, string](ctx, msgSerde, payloadArrSerde, commtypes.JSON, srcStream1)
	if err != nil {
		panic(err)
	}
	expected := []commtypes.MessageG[int, string]{
		{Key: optional.Some(0), Value: optional.Some("A0"), TimestampMs: 0},
		{Key: optional.Some(1), Value: optional.Some("A1"), TimestampMs: 0},
	}
	if !reflect.DeepEqual(expected, got) {
		panic(fmt.Sprintf("should equal. expected: %v, got: %v", expected, got))
	}
	srcStream1.SetCursor(0, 0)

	tm.AddTopicSubstream(srcStream2.TopicName(), 0)
	for i := 0; i < 2; i++ {
		err := pushMsgToStream(ctx, expected_keys[i],
			commtypes.ValueTimestampG[string]{Value: fmt.Sprintf("a%d", expected_keys[i]), Timestamp: 0},
			msgSerde, srcStream2, tm.GetProducerId())
		if err != nil {
			panic(err)
		}
	}
	if _, _, err = tm.CommitTransaction(ctx); err != nil {
		panic(err)
	}

	got, err = readMsgs[int, string](ctx, msgSerde, payloadArrSerde, commtypes.JSON, srcStream2)
	if err != nil {
		panic(err)
	}
	expected = []commtypes.MessageG[int, commtypes.ValueTimestampG[string]]{
		{Key: optional.Some(0), Value: optional.Some(commtypes.ValueTimestampG[string]{Value: "a0", Timestamp: 0}), TimestampMs: 0},
		{Key: optional.Some(1), Value: optional.Some(commtypes.ValueTimestampG[string]{Value: "a1", Timestamp: 0}), TimestampMs: 0},
	}
	if !reflect.DeepEqual(expected, got) {
		panic(fmt.Sprintf("should equal. expected: %v, got: %v", expected, got))
	}
	srcStream2.SetCursor(0, 0)

	tm.RecordTopicStreams(srcs[0].TopicName(), srcStream1)
	tm.RecordTopicStreams(srcs[1].TopicName(), srcStream2)
	tm.RecordTopicStreams(sinks[0].TopicName(), sinks[0].Stream().(*sharedlog_stream.ShardedSharedLogStream))
	tm.AddTopicSubstream(sinks[0].TopicName(), 0)
	tm.AddTopicSubstream(srcStream1.TopicName(), 0)
	tm.AddTopicSubstream(srcStream2.TopicName(), 0)
	joinProc(ctx, src1, sink, msgSerde, trackParFunc, oneJoinTwo)
	joinProc(ctx, src2, sink, msgSerde, trackParFunc, twoJoinOne)
	if err = tm.CommitTransaction(ctx); err != nil {
		panic(err)
	}
	gotOut, err := readMsgs[int, string](ctx, outMsgSerde, payloadArrSerde, commtypes.JSON, sinkStream)
	if err != nil {
		panic(err)
	}
	expected_join := []commtypes.MessageG[int, string]{
		{Key: optional.Some(0), Value: optional.Some("A0+a0"), TimestampMs: 0},
		{Key: optional.Some(1), Value: optional.Some("A1+a1"), TimestampMs: 0},
	}
	if !reflect.DeepEqual(expected_join, got) {
		panic(fmt.Sprintf("should equal. expected: %v, got: %v", expected_join, got))
	}
}

func joinProc(ctx context.Context,
	src *producer_consumer.ShardedSharedLogStreamConsumer,
	sink *producer_consumer.ShardedSharedLogStreamProducer,
	inMsgSerde commtypes.MessageGSerdeG[int, string],
	trackParFunc func(
		topicName string,
		substreamId uint8,
	),
	runner func(ctx context.Context, m commtypes.MessageG[int, string], sink *producer_consumer.ShardedSharedLogStreamProducer,
		trackParFunc exactly_once_intr.TrackProdSubStreamFunc,
	) error,
) {
	for {
		rawMsgSeq, err := src.Consume(ctx, 0)
		if err != nil {
			if common_errors.IsStreamEmptyError(err) || common_errors.IsStreamTimeoutError(err) || err == common_errors.ErrStreamSourceTimeout {
				return
			}
			panic(err)
		}
		msgs, err := commtypes.DecodeRawMsgSeqG[int, string](rawMsgSeq, inMsgSerde)
		if err != nil {
			panic(err)
		}
		debug.Fprintf(os.Stderr, "joinProc: %s got %v\n", src.TopicName(), rawMsgSeq)
		if msgs.MsgArr != nil {
			for _, subMsg := range msgs.MsgArr {
				err = runner(ctx, subMsg, sink, trackParFunc)
				if err != nil {
					panic(err)
				}
			}
		} else {
			err = runner(ctx, msgs.Msg, sink, trackParFunc)
			if err != nil {
				panic(err)
			}
		}
	}
}

func readMsgs[K, V any](ctx context.Context,
	msgSerde commtypes.MessageGSerdeG[K, V],
	payloadArrSerde commtypes.SerdeG[commtypes.PayloadArr],
	serdeFormat commtypes.SerdeFormat,
	log *sharedlog_stream.ShardedSharedLogStream,
) ([]commtypes.MessageG[K, V], error) {
	ret := make([]commtypes.MessageG[K, V], 0)
	tac, err := producer_consumer.NewShardedSharedLogStreamConsumer(log, serdeFormat)
	if err != nil {
		return nil, err
	}
	for {
		msg, err := tac.Consume(ctx, 0)
		if common_errors.IsStreamEmptyError(err) {
			return ret, nil
		} else if err != nil {
			return ret, err
		}

		msgAndSeq, err := commtypes.DecodeRawMsgG(msg, msgSerde, payloadArrSerde)
		if err != nil {
			return nil, fmt.Errorf("DecodeRawMsg err: %v", err)
		}
		debug.Fprintf(os.Stderr, "readMsgs: %s got %v\n", log.TopicName(), msg.LogSeqNum)
		if msgAndSeq.MsgArr != nil {
			for _, msg := range msgAndSeq.MsgArr {
				debug.Fprintf(os.Stderr, "readMsgs: got msg key: %v, val: %v\n", msg.Key, msg.Value)
			}
			ret = append(ret, msgAndSeq.MsgArr...)
		} else {
			debug.Fprintf(os.Stderr, "readMsgs: got msg key: %v, val: %v\n",
				msgAndSeq.Msg.Key, msgAndSeq.Msg.Value)
			ret = append(ret, msgAndSeq.Msg)
		}
	}
}

func pushMsgToStream(ctx context.Context, key int, val commtypes.ValueTimestampG[string], msgSerde commtypes.MessageGSerdeG[int, commtypes.ValueTimestampG[string]],
	log *sharedlog_stream.ShardedSharedLogStream, producerId commtypes.ProducerId,
) error {
	msg := commtypes.MessageG[int, commtypes.ValueTimestampG[string]]{Key: optional.Some(key), Value: optional.Some(val)}
	encoded, err := msgSerde.Encode(msg)
	if err != nil {
		return err
	}
	if encoded != nil {
		debug.Fprintf(os.Stderr, "%s encoded: \n", log.TopicName())
		debug.Fprintf(os.Stderr, "%s\n", string(encoded))
		debug.PrintByteSlice(encoded)
		_, err = log.Push(ctx, encoded, 0, sharedlog_stream.SingleDataRecordMeta, producerId)
		return err
	}
	return nil
}

func pushMsgsToSink[K, V any](ctx context.Context, sink producer_consumer.Producer,
	msgs []commtypes.MessageG[K, V], trackParFunc exactly_once_intr.TrackProdSubStreamFunc,
) error {
	for _, msg := range msgs {
		kBytes, err := sink.KeyEncoder().Encode(msg.Key)
		if err != nil {
			return err
		}
		err = sink.Produce(ctx, msg, 0, false)
		if err != nil {
			return err
		}
	}
	return nil
}
