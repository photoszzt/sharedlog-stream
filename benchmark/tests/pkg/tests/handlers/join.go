package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/benchmark/tests/pkg/tests/test_types"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/concurrent_skiplist"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/exactly_once_intr"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/producer_consumer"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/store"
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
	return utils.CompressData(encodedOutput), nil
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

func (h *joinHandler) getSrcSink(
	ctx context.Context,
	flush time.Duration,
	inMsgSerde commtypes.MessageSerde,
	outMsgSerde commtypes.MessageSerde,
	stream1 *sharedlog_stream.ShardedSharedLogStream,
	stream2 *sharedlog_stream.ShardedSharedLogStream,
	outputStream *sharedlog_stream.ShardedSharedLogStream,
) (*producer_consumer.ShardedSharedLogStreamConsumer, /* src1 */
	*producer_consumer.ShardedSharedLogStreamConsumer, /* src2 */
	*producer_consumer.ShardedSharedLogStreamProducer,
	error,
) {
	src1Config := &producer_consumer.StreamConsumerConfig{
		Timeout:  common.SrcConsumeTimeout,
		MsgSerde: inMsgSerde,
	}
	src2Config := &producer_consumer.StreamConsumerConfig{
		Timeout:  common.SrcConsumeTimeout,
		MsgSerde: inMsgSerde,
	}
	outConfig := &producer_consumer.StreamSinkConfig{
		MsgSerde:      outMsgSerde,
		FlushDuration: flush,
	}
	src1 := producer_consumer.NewShardedSharedLogStreamConsumer(stream1, src1Config)
	src2 := producer_consumer.NewShardedSharedLogStreamConsumer(stream2, src2Config)
	sink := producer_consumer.NewShardedSharedLogStreamProducer(outputStream, outConfig)
	return src1, src2, sink, nil
}

/*
func (h *joinHandler) testStreamStreamJoinMongoDB(ctx context.Context) {
	debug.Fprint(os.Stderr, "############## stream stream join mongodb\n")
	srcStream1, err := sharedlog_stream.NewShardedSharedLogStream(h.env, "src1Mongo", 1, commtypes.JSON)
	if err != nil {
		panic(err)
	}
	srcStream2, err := sharedlog_stream.NewShardedSharedLogStream(h.env, "src2Mongo", 1, commtypes.JSON)
	if err != nil {
		panic(err)
	}
	sinkStream, err := sharedlog_stream.NewShardedSharedLogStream(h.env, "sinkMongo", 1, commtypes.JSON)
	if err != nil {
		panic(err)
	}
	kSerde := commtypes.IntSerde{}
	vSerde := strTsJSONSerde{}
	inMsgSerde, err := commtypes.GetMsgSerde(commtypes.JSON, kSerde, vSerde)
	if err != nil {
		panic(err)
	}
	outMsgSerde, err := commtypes.GetMsgSerde(commtypes.JSON, kSerde, commtypes.StringSerde{})
	if err != nil {
		panic(err)
	}
	src1, src2, sink, err := h.getSrcSink(ctx, common.FlushDuration, inMsgSerde,
		outMsgSerde, srcStream1, srcStream2, sinkStream)
	if err != nil {
		panic(err)
	}
	joinWindows, err := processor.NewJoinWindowsWithGrace(time.Duration(50)*time.Millisecond,
		time.Duration(50)*time.Millisecond)
	if err != nil {
		panic(err)
	}
	mongoAddr := "mongodb://localhost:27017"

	payloadArrSerde := sharedlog_stream.DEFAULT_PAYLOAD_ARR_SERDE
	client, err := store.InitMongoDBClient(ctx, mongoAddr)
	if err != nil {
		panic(err)
	}
	toWinTab1, winTab1, err := processor.ToMongoDBWindowTable(ctx, "tab1Mongo", client, joinWindows, kSerde, vSerde)
	if err != nil {
		panic(err)
	}
	toWinTab2, winTab2, err := processor.ToMongoDBWindowTable(ctx, "tab2Mongo", client, joinWindows, kSerde, vSerde)
	if err != nil {
		panic(err)
	}
	joiner := processor.ValueJoinerWithKeyTsFunc(
		func(readOnlyKey interface{},
			leftValue interface{}, rightValue interface{}, leftTs int64, rightTs int64,
		) interface{} {
			debug.Fprintf(os.Stderr, "left val: %v, ts: %d, right val: %v, ts: %d\n", leftValue, leftTs, rightValue, rightTs)
			return fmt.Sprintf("%s+%s", leftValue.(strTs).Val, rightValue.(strTs).Val)
		})
	sharedTimeTracker := processor.NewTimeTracker()
	oneJoinTwoProc := processor.NewStreamStreamJoinProcessor("oneJoinTwo", winTab2, joinWindows, joiner, false, true, sharedTimeTracker)
	twoJoinOneProc := processor.NewStreamStreamJoinProcessor("twoJoinOne", winTab1, joinWindows, processor.ReverseValueJoinerWithKeyTs(joiner), false, false, sharedTimeTracker)
	oneJoinTwo := func(ctx context.Context, m commtypes.Message, sink *producer_consumer.ShardedSharedLogStreamProducer,
		trackParFunc exactly_once_intr.TrackProdSubStreamFunc,
	) error {
		_, err := toWinTab1.ProcessAndReturn(ctx, m)
		if err != nil {
			return err
		}
		joinedMsgs, err := oneJoinTwoProc.ProcessAndReturn(ctx, m)
		if err != nil {
			return err
		}
		return pushMsgsToSink(ctx, sink, joinedMsgs, trackParFunc)
	}
	twoJoinOne := func(ctx context.Context, m commtypes.Message, sink *producer_consumer.ShardedSharedLogStreamProducer,
		trackParFunc exactly_once_intr.TrackProdSubStreamFunc,
	) error {
		_, err := toWinTab2.ProcessAndReturn(ctx, m)
		if err != nil {
			return err
		}
		joinedMsgs, err := twoJoinOneProc.ProcessAndReturn(ctx, m)
		if err != nil {
			return err
		}
		return pushMsgsToSink(ctx, sink, joinedMsgs, trackParFunc)
	}
	builder := stream_task.NewStreamTaskArgsBuilder(h.env, nil, "joinTestMongo")
	streamTaskArgs := benchutil.UpdateStreamTaskArgs(&common.QueryInput{}, builder).Build()

	tm, err := stream_task.SetupTransactionManager(ctx, streamTaskArgs)
	if err != nil {
		panic(err)
	}
	debug.Fprintf(os.Stderr, "task id: %x, task epoch: %d\n", tm.GetCurrentTaskId(), tm.GetCurrentEpoch())
	trackParFunc := func(ctx context.Context,
		key interface{},
		keySerde commtypes.Serde,
		topicName string,
		substreamId uint8,
	) error {
		err := tm.AddTopicSubstream(ctx, topicName, substreamId)
		if err != nil {
			return err
		}
		return err
	}
	expected_keys := []int{0, 1, 2, 3}

	tm.RecordTopicStreams(src1.TopicName(), srcStream1)

	if err = tm.BeginTransaction(ctx); err != nil {
		panic(err)
	}
	if err = tm.AddTopicSubstream(ctx, srcStream1.TopicName(), 0); err != nil {
		panic(err)
	}
	for i := 0; i < 2; i++ {
		err := pushMsgToStream(ctx, expected_keys[i],
			&strTs{Val: fmt.Sprintf("A%d", expected_keys[i]), Ts: 0},
			inMsgSerde, srcStream1, tm.GetProducerId())
		if err != nil {
			panic(err)
		}
	}

	if err = tm.CommitTransaction(ctx, nil, nil); err != nil {
		panic(err)
	}


	tm.RecordTopicStreams(src2.TopicName(), srcStream2)
	if err = tm.BeginTransaction(ctx); err != nil {
		panic(err)
	}
	if err = tm.AddTopicSubstream(ctx, srcStream2.TopicName(), 0); err != nil {
		panic(err)
	}
	for i := 0; i < 2; i++ {
		err := pushMsgToStream(ctx, expected_keys[i],
			&strTs{Val: fmt.Sprintf("a%d", expected_keys[i]), Ts: 0},
			inMsgSerde, srcStream2, tm.GetProducerId())
		if err != nil {
			panic(err)
		}
	}
	if err = tm.CommitTransaction(ctx, nil, nil); err != nil {
		panic(err)
	}

	tm.RecordTopicStreams(src1.TopicName(), srcStream1)
	tm.RecordTopicStreams(src2.TopicName(), srcStream2)
	tm.RecordTopicStreams(sinkStream.TopicName(), sinkStream)
	if err = tm.BeginTransaction(ctx); err != nil {
		panic(err)
	}
	if err = tm.AddTopicSubstream(ctx, sinkStream.TopicName(), 0); err != nil {
		panic(err)
	}
	if err = tm.AddTopicSubstream(ctx, srcStream1.TopicName(), 0); err != nil {
		panic(err)
	}
	if err = tm.AddTopicSubstream(ctx, srcStream2.TopicName(), 0); err != nil {
		panic(err)
	}
	joinProc(ctx, src1, sink, trackParFunc, oneJoinTwo)
	joinProc(ctx, src2, sink, trackParFunc, twoJoinOne)
	if err = tm.CommitTransaction(ctx, nil, nil); err != nil {
		panic(err)
	}
	tm.RecordTopicStreams(sinkStream.TopicName(), sinkStream)
	got, err := readMsgs(ctx, outMsgSerde, payloadArrSerde, commtypes.JSON, sinkStream)
	if err != nil {
		panic(err)
	}
	expected_join := []commtypes.Message{
		{Key: 0, Value: "A0+a0", Timestamp: 0},
		{Key: 1, Value: "A1+a1", Timestamp: 0},
	}
	if !reflect.DeepEqual(expected_join, got) {
		panic(fmt.Sprintf("should equal. expected: %v, got: %v", expected_join, got))
	}

}
*/

func (h *joinHandler) testStreamStreamJoinMem(ctx context.Context) {
	srcStream1, err := sharedlog_stream.NewShardedSharedLogStream(h.env, "src1", 1, commtypes.JSON)
	if err != nil {
		panic(err)
	}
	srcStream2, err := sharedlog_stream.NewShardedSharedLogStream(h.env, "src2", 1, commtypes.JSON)
	if err != nil {
		panic(err)
	}
	sinkStream, err := sharedlog_stream.NewShardedSharedLogStream(h.env, "sink", 1, commtypes.JSON)
	if err != nil {
		panic(err)
	}
	inMsgSerde := commtypes.MessageJSONSerde{
		KeySerde: commtypes.IntSerde{},
		ValSerde: strTsJSONSerde{},
	}
	outMsgSerde := commtypes.MessageJSONSerde{
		KeySerde: commtypes.IntSerde{},
		ValSerde: commtypes.StringSerde{},
	}
	src1, src2, sink, err := h.getSrcSink(ctx, common.FlushDuration, inMsgSerde,
		outMsgSerde, srcStream1, srcStream2, sinkStream)
	if err != nil {
		panic(err)
	}

	compare := concurrent_skiplist.CompareFunc(func(lhs, rhs interface{}) int {
		l, ok := lhs.(int)
		if ok {
			r := rhs.(int)
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
			lvk := lv.Key.(int)
			rvk := rv.Key.(int)
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
	})
	joinWindows, err := processor.NewJoinWindowsWithGrace(time.Duration(50)*time.Millisecond,
		time.Duration(50)*time.Millisecond)
	if err != nil {
		panic(err)
	}
	toWinTab1, winTab1, err := processor.ToInMemWindowTable("tab1", joinWindows, compare)
	if err != nil {
		panic(err)
	}
	toWinTab2, winTab2, err := processor.ToInMemWindowTable("tab2", joinWindows, compare)
	if err != nil {
		panic(err)
	}
	joiner := processor.ValueJoinerWithKeyTsFunc(
		func(readOnlyKey interface{},
			leftValue interface{}, rightValue interface{}, leftTs int64, rightTs int64,
		) interface{} {
			debug.Fprintf(os.Stderr, "left val: %v, ts: %d, right val: %v, ts: %d\n", leftValue, leftTs, rightValue, rightTs)
			return fmt.Sprintf("%s+%s", leftValue.(strTs).Val, rightValue.(strTs).Val)
		})
	sharedTimeTracker := processor.NewTimeTracker()
	oneJoinTwoProc := processor.NewStreamStreamJoinProcessor("oneJoinTwo", winTab2, joinWindows, joiner, false, true, sharedTimeTracker)
	twoJoinOneProc := processor.NewStreamStreamJoinProcessor("twoJoinOne", winTab1, joinWindows, processor.ReverseValueJoinerWithKeyTs(joiner), false, false, sharedTimeTracker)
	oneJoinTwo := func(ctx context.Context, m commtypes.Message, sink *producer_consumer.ShardedSharedLogStreamProducer,
		trackParFunc exactly_once_intr.TrackProdSubStreamFunc,
	) error {
		_, err := toWinTab1.ProcessAndReturn(ctx, m)
		if err != nil {
			return err
		}
		joinedMsgs, err := oneJoinTwoProc.ProcessAndReturn(ctx, m)
		if err != nil {
			return err
		}
		return pushMsgsToSink(ctx, sink, joinedMsgs, trackParFunc)
	}
	twoJoinOne := func(ctx context.Context, m commtypes.Message, sink *producer_consumer.ShardedSharedLogStreamProducer,
		trackParFunc exactly_once_intr.TrackProdSubStreamFunc,
	) error {
		_, err := toWinTab2.ProcessAndReturn(ctx, m)
		if err != nil {
			return err
		}
		joinedMsgs, err := twoJoinOneProc.ProcessAndReturn(ctx, m)
		if err != nil {
			return err
		}
		return pushMsgsToSink(ctx, sink, joinedMsgs, trackParFunc)
	}

	payloadArrSerde := sharedlog_stream.DEFAULT_PAYLOAD_ARR_SERDE
	streamTaskArgs := benchutil.UpdateStreamTaskArgs(&common.QueryInput{},
		stream_task.NewStreamTaskArgsBuilder(h.env, nil, "joinTestMem")).Build()
	tm, err := stream_task.SetupTransactionManager(ctx, streamTaskArgs)
	if err != nil {
		panic(err)
	}
	trackParFunc := func(ctx context.Context,
		key interface{},
		keySerde commtypes.Serde,
		topicName string,
		substreamId uint8,
	) error {
		err := tm.AddTopicSubstream(ctx, topicName, substreamId)
		if err != nil {
			return err
		}
		return err
	}
	expected_keys := []int{0, 1, 2, 3}

	tm.RecordTopicStreams(src1.TopicName(), srcStream1)

	if err = tm.BeginTransaction(ctx); err != nil {
		panic(err)
	}
	if err = tm.AddTopicSubstream(ctx, srcStream1.TopicName(), 0); err != nil {
		panic(err)
	}
	for i := 0; i < 2; i++ {
		err := pushMsgToStream(ctx, expected_keys[i],
			&strTs{Val: fmt.Sprintf("A%d", expected_keys[i]), Ts: 0},
			inMsgSerde, srcStream1, tm.GetProducerId())
		if err != nil {
			panic(err)
		}
	}

	if err = tm.CommitTransaction(ctx, nil, nil); err != nil {
		panic(err)
	}

	got, err := readMsgs(ctx, inMsgSerde, payloadArrSerde, commtypes.JSON, srcStream1)
	if err != nil {
		panic(err)
	}
	expected := []commtypes.Message{
		{Key: 0, Value: strTs{Val: "A0", Ts: 0}, Timestamp: 0},
		{Key: 1, Value: strTs{Val: "A1", Ts: 0}, Timestamp: 0},
	}
	if !reflect.DeepEqual(expected, got) {
		panic(fmt.Sprintf("should equal. expected: %v, got: %v", expected, got))
	}
	srcStream1.SetCursor(0, 0)

	tm.RecordTopicStreams(src2.TopicName(), srcStream2)
	if err = tm.BeginTransaction(ctx); err != nil {
		panic(err)
	}
	if err = tm.AddTopicSubstream(ctx, srcStream2.TopicName(), 0); err != nil {
		panic(err)
	}
	for i := 0; i < 2; i++ {
		err := pushMsgToStream(ctx, expected_keys[i],
			&strTs{Val: fmt.Sprintf("a%d", expected_keys[i]), Ts: 0},
			inMsgSerde, srcStream2, tm.GetProducerId())
		if err != nil {
			panic(err)
		}
	}
	if err = tm.CommitTransaction(ctx, nil, nil); err != nil {
		panic(err)
	}

	got, err = readMsgs(ctx, inMsgSerde, payloadArrSerde, commtypes.JSON, srcStream2)
	if err != nil {
		panic(err)
	}
	expected = []commtypes.Message{
		{Key: 0, Value: strTs{Val: "a0", Ts: 0}, Timestamp: 0},
		{Key: 1, Value: strTs{Val: "a1", Ts: 0}, Timestamp: 0},
	}
	if !reflect.DeepEqual(expected, got) {
		panic(fmt.Sprintf("should equal. expected: %v, got: %v", expected, got))
	}
	srcStream2.SetCursor(0, 0)

	tm.RecordTopicStreams(src1.TopicName(), srcStream1)
	tm.RecordTopicStreams(src2.TopicName(), srcStream2)
	tm.RecordTopicStreams(sinkStream.TopicName(), sinkStream)
	if err = tm.BeginTransaction(ctx); err != nil {
		panic(err)
	}
	if err = tm.AddTopicSubstream(ctx, sinkStream.TopicName(), 0); err != nil {
		panic(err)
	}
	if err = tm.AddTopicSubstream(ctx, srcStream1.TopicName(), 0); err != nil {
		panic(err)
	}
	if err = tm.AddTopicSubstream(ctx, srcStream2.TopicName(), 0); err != nil {
		panic(err)
	}
	joinProc(ctx, src1, sink, trackParFunc, oneJoinTwo)
	joinProc(ctx, src2, sink, trackParFunc, twoJoinOne)
	if err = tm.CommitTransaction(ctx, nil, nil); err != nil {
		panic(err)
	}
	got, err = readMsgs(ctx, outMsgSerde, payloadArrSerde, commtypes.JSON, sinkStream)
	if err != nil {
		panic(err)
	}
	expected_join := []commtypes.Message{
		{Key: 0, Value: "A0+a0", Timestamp: 0},
		{Key: 1, Value: "A1+a1", Timestamp: 0},
	}
	if !reflect.DeepEqual(expected_join, got) {
		panic(fmt.Sprintf("should equal. expected: %v, got: %v", expected_join, got))
	}
}

func joinProc(ctx context.Context,
	src *producer_consumer.ShardedSharedLogStreamConsumer,
	sink *producer_consumer.ShardedSharedLogStreamProducer,
	trackParFunc func(ctx context.Context,
		key interface{},
		keySerde commtypes.Serde,
		topicName string,
		substreamId uint8,
	) error,
	runner func(ctx context.Context, m commtypes.Message, sink *producer_consumer.ShardedSharedLogStreamProducer,
		trackParFunc exactly_once_intr.TrackProdSubStreamFunc,
	) error,
) {
	for {
		gotMsgs, err := src.Consume(ctx, 0)
		if err != nil {
			if common_errors.IsStreamEmptyError(err) || common_errors.IsStreamTimeoutError(err) || err == common_errors.ErrStreamSourceTimeout {
				return
			}
			panic(err)
		}
		debug.Fprintf(os.Stderr, "joinProc: %s got %v\n", src.TopicName(), gotMsgs)
		for _, msg := range gotMsgs.Msgs {
			if msg.MsgArr != nil {
				for _, subMsg := range msg.MsgArr {
					err = runner(ctx, subMsg, sink, trackParFunc)
					if err != nil {
						panic(err)
					}
				}
			} else {
				err = runner(ctx, msg.Msg, sink, trackParFunc)
				if err != nil {
					panic(err)
				}
			}
		}
	}
}

func readMsgs(ctx context.Context,
	msgSerde commtypes.MessageSerde,
	payloadArrSerde commtypes.Serde,
	serdeFormat commtypes.SerdeFormat,
	log *sharedlog_stream.ShardedSharedLogStream,
) ([]commtypes.Message, error) {
	ret := make([]commtypes.Message, 0)
	tac, err := producer_consumer.NewTransactionAwareConsumer(log, serdeFormat)
	if err != nil {
		return nil, err
	}
	for {
		msg, err := tac.ReadNext(ctx, 0)
		if common_errors.IsStreamEmptyError(err) {
			return ret, nil
		} else if err != nil {
			return ret, err
		}

		msgAndSeq, err := commtypes.DecodeRawMsg(msg, msgSerde, payloadArrSerde)
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

func pushMsgToStream(ctx context.Context, key int, val *strTs, msgSerde commtypes.Serde,
	log *sharedlog_stream.ShardedSharedLogStream, producerId commtypes.ProducerId,
) error {
	msg := commtypes.Message{Key: key, Value: val}
	encoded, err := msgSerde.Encode(&msg)
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

func pushMsgsToSink(ctx context.Context, sink producer_consumer.Producer,
	msgs []commtypes.Message, trackParFunc exactly_once_intr.TrackProdSubStreamFunc,
) error {
	for _, msg := range msgs {
		key := msg.Key.(int)
		err := trackParFunc(ctx, key, sink.KeySerde(), sink.TopicName(), 0)
		if err != nil {
			return fmt.Errorf("add topic partition failed: %v", err)
		}
		err = sink.Produce(ctx, msg, 0, false)
		if err != nil {
			return err
		}
	}
	return nil
}
