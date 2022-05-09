package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/benchmark/tests/pkg/tests/test_types"
	"sharedlog-stream/pkg/concurrent_skiplist"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/errors"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"
	"sharedlog-stream/pkg/transaction"
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
	case "streamStreamJoinMongo":
		h.testStreamStreamJoinMongoDB(ctx)
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
	stream1 *sharedlog_stream.ShardedSharedLogStream,
	stream2 *sharedlog_stream.ShardedSharedLogStream,
	outputStream *sharedlog_stream.ShardedSharedLogStream,
) (*processor.MeteredSource, /* src1 */
	*processor.MeteredSource, /* src2 */
	*sharedlog_stream.ConcurrentMeteredSink, error,
) {
	msgSerde, err := commtypes.GetMsgSerde(uint8(commtypes.JSON))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("get msg serde err: %v", err)
	}
	src1Config := &sharedlog_stream.StreamSourceConfig{
		Timeout:      common.SrcConsumeTimeout,
		KeyDecoder:   commtypes.IntSerde{},
		ValueDecoder: strTsJSONSerde{},
		MsgDecoder:   msgSerde,
	}
	src2Config := &sharedlog_stream.StreamSourceConfig{
		Timeout:      common.SrcConsumeTimeout,
		KeyDecoder:   commtypes.IntSerde{},
		ValueDecoder: strTsJSONSerde{},
		MsgDecoder:   msgSerde,
	}
	outConfig := &sharedlog_stream.StreamSinkConfig{
		KeySerde:      commtypes.IntSerde{},
		ValueSerde:    commtypes.StringSerde{},
		MsgSerde:      msgSerde,
		FlushDuration: flush,
	}
	src1 := processor.NewMeteredSource(sharedlog_stream.NewShardedSharedLogStreamSource(stream1, src1Config), 0)
	src2 := processor.NewMeteredSource(sharedlog_stream.NewShardedSharedLogStreamSource(stream2, src2Config), 0)
	sink := sharedlog_stream.NewConcurrentMeteredSink(sharedlog_stream.NewShardedSharedLogStreamSink(outputStream, outConfig), 0)
	return src1, src2, sink, nil
}

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
	srcStream1.SetInTransaction(true)
	srcStream2.SetInTransaction(true)
	sinkStream.SetInTransaction(true)
	src1, src2, sink, err := h.getSrcSink(ctx, common.FlushDuration, srcStream1, srcStream2, sinkStream)
	if err != nil {
		panic(err)
	}
	joinWindows, err := processor.NewJoinWindowsWithGrace(time.Duration(50)*time.Millisecond,
		time.Duration(50)*time.Millisecond)
	if err != nil {
		panic(err)
	}
	mongoAddr := "mongodb://localhost:27017"
	msgSerde := commtypes.MessageSerializedJSONSerde{}
	kSerde := commtypes.IntSerde{}
	vSerde := strTsJSONSerde{}
	client, err := store.InitMongoDBClient(ctx, mongoAddr)
	if err != nil {
		panic(err)
	}
	toWinTab1, winTab1, err := processor.ToMongoDBWindowTable(ctx, "tab1Mongo", client, joinWindows, kSerde, vSerde, 0)
	if err != nil {
		panic(err)
	}
	toWinTab2, winTab2, err := processor.ToMongoDBWindowTable(ctx, "tab2Mongo", client, joinWindows, kSerde, vSerde, 0)
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
	oneJoinTwoProc := processor.NewStreamStreamJoinProcessor(winTab2, joinWindows, joiner, false, true, sharedTimeTracker)
	twoJoinOneProc := processor.NewStreamStreamJoinProcessor(winTab1, joinWindows, processor.ReverseValueJoinerWithKeyTs(joiner), false, false, sharedTimeTracker)
	oneJoinTwo := func(ctx context.Context, m commtypes.Message, sink *sharedlog_stream.ConcurrentMeteredSink,
		trackParFunc transaction.TrackKeySubStreamFunc,
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
	twoJoinOne := func(ctx context.Context, m commtypes.Message, sink *sharedlog_stream.ConcurrentMeteredSink,
		trackParFunc transaction.TrackKeySubStreamFunc,
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

	sp := &common.QueryInput{
		SerdeFormat: uint8(commtypes.JSON),
	}
	streamTaskArgs := &transaction.StreamTaskArgsTransaction{
		Env:             h.env,
		QueryInput:      sp,
		MsgSerde:        msgSerde,
		OutputStreams:   []*sharedlog_stream.ShardedSharedLogStream{sinkStream},
		TransactionalId: "joinTestMongo",
	}
	tm, err := transaction.SetupTransactionManager(ctx, streamTaskArgs)
	if err != nil {
		panic(err)
	}
	debug.Fprintf(os.Stderr, "task id: %x, task epoch: %d\n", tm.CurrentTaskId, tm.CurrentEpoch)
	trackParFunc := func(ctx context.Context,
		key interface{},
		keySerde commtypes.Serde,
		topicName string,
		substreamId uint8,
	) error {
		err := tm.AddTopicPartition(ctx, topicName, []uint8{substreamId})
		if err != nil {
			return err
		}
		return err
	}
	expected_keys := []int{0, 1, 2, 3}

	tm.RecordTopicStreams(src1.TopicName(), srcStream1)

	if err = tm.BeginTransaction(ctx, nil, nil); err != nil {
		panic(err)
	}
	if err = tm.AddTopicPartition(ctx, srcStream1.TopicName(), []uint8{0}); err != nil {
		panic(err)
	}
	for i := 0; i < 2; i++ {
		err := pushMsgToStream(ctx, expected_keys[i],
			&strTs{Val: fmt.Sprintf("A%d", expected_keys[i]), Ts: 0},
			kSerde, vSerde, msgSerde, srcStream1)
		if err != nil {
			panic(err)
		}
	}

	if err = tm.CommitTransaction(ctx, nil, nil); err != nil {
		panic(err)
	}

	/*
		got, err := readMsgs(ctx, kSerde, vSerde, msgSerde, srcStream1)
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
	*/

	tm.RecordTopicStreams(src2.TopicName(), srcStream2)
	if err = tm.BeginTransaction(ctx, nil, nil); err != nil {
		panic(err)
	}
	if err = tm.AddTopicPartition(ctx, srcStream2.TopicName(), []uint8{0}); err != nil {
		panic(err)
	}
	for i := 0; i < 2; i++ {
		err := pushMsgToStream(ctx, expected_keys[i],
			&strTs{Val: fmt.Sprintf("a%d", expected_keys[i]), Ts: 0},
			kSerde, vSerde, msgSerde, srcStream2)
		if err != nil {
			panic(err)
		}
	}
	if err = tm.CommitTransaction(ctx, nil, nil); err != nil {
		panic(err)
	}

	/*
		got, err = readMsgs(ctx, kSerde, vSerde, msgSerde, srcStream2)
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
	*/
	tm.RecordTopicStreams(src1.TopicName(), srcStream1)
	tm.RecordTopicStreams(src2.TopicName(), srcStream2)
	tm.RecordTopicStreams(sinkStream.TopicName(), sinkStream)
	if err = tm.BeginTransaction(ctx, nil, nil); err != nil {
		panic(err)
	}
	if err = tm.AddTopicPartition(ctx, sinkStream.TopicName(), []uint8{0}); err != nil {
		panic(err)
	}
	if err = tm.AddTopicPartition(ctx, srcStream1.TopicName(), []uint8{0}); err != nil {
		panic(err)
	}
	if err = tm.AddTopicPartition(ctx, srcStream2.TopicName(), []uint8{0}); err != nil {
		panic(err)
	}
	joinProc(ctx, src1, sink, trackParFunc, oneJoinTwo)
	joinProc(ctx, src2, sink, trackParFunc, twoJoinOne)
	if err = tm.CommitTransaction(ctx, nil, nil); err != nil {
		panic(err)
	}
	tm.RecordTopicStreams(sinkStream.TopicName(), sinkStream)
	got, err := readMsgs(ctx, kSerde, commtypes.StringSerde{}, msgSerde, sinkStream)
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
	srcStream1.SetInTransaction(true)
	srcStream2.SetInTransaction(true)
	sinkStream.SetInTransaction(true)
	src1, src2, sink, err := h.getSrcSink(ctx, common.FlushDuration, srcStream1, srcStream2, sinkStream)
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
	toWinTab1, winTab1, err := processor.ToInMemWindowTable("tab1", joinWindows, compare, 0)
	if err != nil {
		panic(err)
	}
	toWinTab2, winTab2, err := processor.ToInMemWindowTable("tab2", joinWindows, compare, 0)
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
	oneJoinTwoProc := processor.NewStreamStreamJoinProcessor(winTab2, joinWindows, joiner, false, true, sharedTimeTracker)
	twoJoinOneProc := processor.NewStreamStreamJoinProcessor(winTab1, joinWindows, processor.ReverseValueJoinerWithKeyTs(joiner), false, false, sharedTimeTracker)
	oneJoinTwo := func(ctx context.Context, m commtypes.Message, sink *sharedlog_stream.ConcurrentMeteredSink,
		trackParFunc transaction.TrackKeySubStreamFunc,
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
	twoJoinOne := func(ctx context.Context, m commtypes.Message, sink *sharedlog_stream.ConcurrentMeteredSink,
		trackParFunc transaction.TrackKeySubStreamFunc,
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

	msgSerde := commtypes.MessageSerializedJSONSerde{}
	kSerde := commtypes.IntSerde{}
	vSerde := strTsJSONSerde{}

	sp := &common.QueryInput{
		SerdeFormat: uint8(commtypes.JSON),
	}
	streamTaskArgs := &transaction.StreamTaskArgsTransaction{
		Env:             h.env,
		QueryInput:      sp,
		MsgSerde:        msgSerde,
		OutputStreams:   []*sharedlog_stream.ShardedSharedLogStream{sinkStream},
		TransactionalId: "joinTestsMem",
	}
	tm, err := transaction.SetupTransactionManager(ctx, streamTaskArgs)
	if err != nil {
		panic(err)
	}
	trackParFunc := func(ctx context.Context,
		key interface{},
		keySerde commtypes.Serde,
		topicName string,
		substreamId uint8,
	) error {
		err := tm.AddTopicPartition(ctx, topicName, []uint8{substreamId})
		if err != nil {
			return err
		}
		return err
	}
	expected_keys := []int{0, 1, 2, 3}

	tm.RecordTopicStreams(src1.TopicName(), srcStream1)

	if err = tm.BeginTransaction(ctx, nil, nil); err != nil {
		panic(err)
	}
	if err = tm.AddTopicPartition(ctx, srcStream1.TopicName(), []uint8{0}); err != nil {
		panic(err)
	}
	for i := 0; i < 2; i++ {
		err := pushMsgToStream(ctx, expected_keys[i],
			&strTs{Val: fmt.Sprintf("A%d", expected_keys[i]), Ts: 0},
			kSerde, vSerde, msgSerde, srcStream1)
		if err != nil {
			panic(err)
		}
	}

	if err = tm.CommitTransaction(ctx, nil, nil); err != nil {
		panic(err)
	}

	/*
		got, err := readMsgs(ctx, kSerde, vSerde, msgSerde, srcStream1)
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
	*/

	tm.RecordTopicStreams(src2.TopicName(), srcStream2)
	if err = tm.BeginTransaction(ctx, nil, nil); err != nil {
		panic(err)
	}
	if err = tm.AddTopicPartition(ctx, srcStream2.TopicName(), []uint8{0}); err != nil {
		panic(err)
	}
	for i := 0; i < 2; i++ {
		err := pushMsgToStream(ctx, expected_keys[i],
			&strTs{Val: fmt.Sprintf("a%d", expected_keys[i]), Ts: 0},
			kSerde, vSerde, msgSerde, srcStream2)
		if err != nil {
			panic(err)
		}
	}
	if err = tm.CommitTransaction(ctx, nil, nil); err != nil {
		panic(err)
	}

	/*
		got, err = readMsgs(ctx, kSerde, vSerde, msgSerde, srcStream2)
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
	*/
	tm.RecordTopicStreams(src1.TopicName(), srcStream1)
	tm.RecordTopicStreams(src2.TopicName(), srcStream2)
	tm.RecordTopicStreams(sinkStream.TopicName(), sinkStream)
	if err = tm.BeginTransaction(ctx, nil, nil); err != nil {
		panic(err)
	}
	if err = tm.AddTopicPartition(ctx, sinkStream.TopicName(), []uint8{0}); err != nil {
		panic(err)
	}
	if err = tm.AddTopicPartition(ctx, srcStream1.TopicName(), []uint8{0}); err != nil {
		panic(err)
	}
	if err = tm.AddTopicPartition(ctx, srcStream2.TopicName(), []uint8{0}); err != nil {
		panic(err)
	}
	joinProc(ctx, src1, sink, trackParFunc, oneJoinTwo)
	joinProc(ctx, src2, sink, trackParFunc, twoJoinOne)
	if err = tm.CommitTransaction(ctx, nil, nil); err != nil {
		panic(err)
	}
	got, err := readMsgs(ctx, kSerde, commtypes.StringSerde{}, msgSerde, sinkStream)
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
	src *processor.MeteredSource,
	sink *sharedlog_stream.ConcurrentMeteredSink,
	trackParFunc func(ctx context.Context,
		key interface{},
		keySerde commtypes.Serde,
		topicName string,
		substreamId uint8,
	) error,
	runner func(ctx context.Context, m commtypes.Message, sink *sharedlog_stream.ConcurrentMeteredSink,
		trackParFunc transaction.TrackKeySubStreamFunc,
	) error,
) {
	gotMsgs, err := src.Consume(ctx, 0)
	if err != nil {
		if errors.IsStreamEmptyError(err) || errors.IsStreamTimeoutError(err) || err == errors.ErrStreamSourceTimeout {
			return
		}
		panic(err)
	}
	debug.Fprintf(os.Stderr, "%s got %v\n", src.TopicName(), gotMsgs)
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

func readMsgs(ctx context.Context,
	kSerde commtypes.Serde, vSerde commtypes.Serde,
	msgSerde commtypes.MsgSerde, log *sharedlog_stream.ShardedSharedLogStream,
) ([]commtypes.Message, error) {
	ret := make([]commtypes.Message, 0)
	for {
		_, msgs, err := log.ReadNext(ctx, 0)
		debug.Fprintf(os.Stderr, "%s got %v\n", log.TopicName(), msgs)
		if errors.IsStreamEmptyError(err) {
			return ret, nil
		} else if err != nil {
			return ret, err
		}
		for _, msg := range msgs {
			kBytes, valBytes, err := msgSerde.Decode(msg.Payload)
			if err != nil {
				return nil, fmt.Errorf("msgSerde err: %v", err)
			}
			key, err := kSerde.Decode(kBytes)
			if err != nil {
				return nil, fmt.Errorf("kSerde err: %v", err)
			}
			val, err := vSerde.Decode(valBytes)
			if err != nil {
				return nil, fmt.Errorf("vSerde err: %v", err)
			}
			/*
				sx := val.(commtypes.StreamTimeExtractor)
				ts, err := sx.ExtractStreamTime()
				if err != nil {
					return nil, err
				}
			*/
			ret = append(ret, commtypes.Message{Key: key, Value: val, Timestamp: 0})
		}
	}
}

func pushMsgToStream(ctx context.Context, key int, val *strTs, kSerde commtypes.Serde, vSerde commtypes.Serde, msgSerde commtypes.MsgSerde,
	log *sharedlog_stream.ShardedSharedLogStream,
) error {
	keyBytes, err := kSerde.Encode(key)
	if err != nil {
		return err
	}
	valBytes, err := vSerde.Encode(val)
	if err != nil {
		return err
	}
	encoded, err := msgSerde.Encode(keyBytes, valBytes)
	if err != nil {
		return err
	}
	debug.Fprintf(os.Stderr, "%s encoded: \n", log.TopicName())
	debug.PrintByteSlice(encoded)
	_, err = log.Push(ctx, encoded, 0, false, false)
	return err
}

func pushMsgsToSink(ctx context.Context, sink *sharedlog_stream.ConcurrentMeteredSink,
	msgs []commtypes.Message, trackParFunc transaction.TrackKeySubStreamFunc,
) error {
	for _, msg := range msgs {
		key := msg.Key.(int)
		err := trackParFunc(ctx, key, sink.KeySerde(), sink.TopicName(), 0)
		if err != nil {
			return fmt.Errorf("add topic partition failed: %v", err)
		}
		err = sink.Sink(ctx, msg, 0, false)
		if err != nil {
			return err
		}
	}
	return nil
}
