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
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/concurrent_skiplist"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/errors"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/source_sink"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/transaction"
	"sharedlog-stream/pkg/transaction/tran_interface"
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
	inKVMsgSerdes commtypes.KVMsgSerdes,
	outKVMsgSerdes commtypes.KVMsgSerdes,
	stream1 *sharedlog_stream.ShardedSharedLogStream,
	stream2 *sharedlog_stream.ShardedSharedLogStream,
	outputStream *sharedlog_stream.ShardedSharedLogStream,
) (*source_sink.ShardedSharedLogStreamSource, /* src1 */
	*source_sink.ShardedSharedLogStreamSource, /* src2 */
	*source_sink.ShardedSharedLogStreamSyncSink,
	error,
) {
	src1Config := &source_sink.StreamSourceConfig{
		Timeout:     common.SrcConsumeTimeout,
		KVMsgSerdes: inKVMsgSerdes,
	}
	src2Config := &source_sink.StreamSourceConfig{
		Timeout:     common.SrcConsumeTimeout,
		KVMsgSerdes: inKVMsgSerdes,
	}
	outConfig := &source_sink.StreamSinkConfig{
		KVMsgSerdes:   outKVMsgSerdes,
		FlushDuration: flush,
	}
	src1 := source_sink.NewShardedSharedLogStreamSource(stream1, src1Config)
	src2 := source_sink.NewShardedSharedLogStreamSource(stream2, src2Config)
	sink := source_sink.NewShardedSharedLogStreamSyncSink(outputStream, outConfig)
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
	msgSerde := commtypes.MessageSerializedJSONSerde{}
	kSerde := commtypes.IntSerde{}
	vSerde := strTsJSONSerde{}
	inKVMsgSerdes := commtypes.KVMsgSerdes{
		KeySerde: kSerde,
		ValSerde: vSerde,
		MsgSerde: msgSerde,
	}
	outKVMsgSerdes := commtypes.KVMsgSerdes{
		KeySerde: kSerde,
		ValSerde: commtypes.StringSerde{},
		MsgSerde: msgSerde,
	}
	src1, src2, sink, err := h.getSrcSink(ctx, common.FlushDuration, inKVMsgSerdes,
		outKVMsgSerdes, srcStream1, srcStream2, sinkStream)
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
	oneJoinTwo := func(ctx context.Context, m commtypes.Message, sink *source_sink.ShardedSharedLogStreamSyncSink,
		trackParFunc tran_interface.TrackKeySubStreamFunc,
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
	twoJoinOne := func(ctx context.Context, m commtypes.Message, sink *source_sink.ShardedSharedLogStreamSyncSink,
		trackParFunc tran_interface.TrackKeySubStreamFunc,
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

	streamTaskArgs := transaction.NewStreamTaskArgsTransaction(h.env, "joinTestMongo", nil,
		[]source_sink.Source{src1, src2}, []source_sink.Sink{sink})

	tm, err := transaction.SetupTransactionManager(ctx, streamTaskArgs)
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
			inKVMsgSerdes, srcStream1,
			tm.GetCurrentTaskId(), tm.GetCurrentEpoch(), tm.GetTransactionID())
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
			inKVMsgSerdes, srcStream2,
			tm.GetCurrentTaskId(), tm.GetCurrentEpoch(), tm.GetTransactionID())
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
	got, err := readMsgs(ctx, outKVMsgSerdes, payloadArrSerde, commtypes.JSON, sinkStream)
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
	msgSerde := commtypes.MessageSerializedJSONSerde{}
	kSerde := commtypes.IntSerde{}
	vSerde := strTsJSONSerde{}
	inKVMsgSerdes := commtypes.KVMsgSerdes{
		KeySerde: kSerde,
		ValSerde: vSerde,
		MsgSerde: msgSerde,
	}
	outKVMsgSerdes := commtypes.KVMsgSerdes{
		KeySerde: kSerde,
		ValSerde: commtypes.StringSerde{},
		MsgSerde: msgSerde,
	}
	src1, src2, sink, err := h.getSrcSink(ctx, common.FlushDuration, inKVMsgSerdes,
		outKVMsgSerdes, srcStream1, srcStream2, sinkStream)
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
	oneJoinTwo := func(ctx context.Context, m commtypes.Message, sink *source_sink.ShardedSharedLogStreamSyncSink,
		trackParFunc tran_interface.TrackKeySubStreamFunc,
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
	twoJoinOne := func(ctx context.Context, m commtypes.Message, sink *source_sink.ShardedSharedLogStreamSyncSink,
		trackParFunc tran_interface.TrackKeySubStreamFunc,
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

	streamTaskArgs := transaction.NewStreamTaskArgsTransaction(h.env, "joinTestMem", nil,
		[]source_sink.Source{src1, src2}, []source_sink.Sink{sink})
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
			inKVMsgSerdes, srcStream1,
			tm.GetCurrentTaskId(), tm.GetCurrentEpoch(), tm.GetTransactionID())
		if err != nil {
			panic(err)
		}
	}

	if err = tm.CommitTransaction(ctx, nil, nil); err != nil {
		panic(err)
	}

	got, err := readMsgs(ctx, inKVMsgSerdes, payloadArrSerde, commtypes.JSON, srcStream1)
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
	if err = tm.BeginTransaction(ctx, nil, nil); err != nil {
		panic(err)
	}
	if err = tm.AddTopicPartition(ctx, srcStream2.TopicName(), []uint8{0}); err != nil {
		panic(err)
	}
	for i := 0; i < 2; i++ {
		err := pushMsgToStream(ctx, expected_keys[i],
			&strTs{Val: fmt.Sprintf("a%d", expected_keys[i]), Ts: 0},
			inKVMsgSerdes, srcStream2,
			tm.GetCurrentTaskId(), tm.GetCurrentEpoch(), tm.GetTransactionID())
		if err != nil {
			panic(err)
		}
	}
	if err = tm.CommitTransaction(ctx, nil, nil); err != nil {
		panic(err)
	}

	got, err = readMsgs(ctx, inKVMsgSerdes, payloadArrSerde, commtypes.JSON, srcStream2)
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
	got, err = readMsgs(ctx, outKVMsgSerdes, payloadArrSerde, commtypes.JSON, sinkStream)
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
	src *source_sink.ShardedSharedLogStreamSource,
	sink *source_sink.ShardedSharedLogStreamSyncSink,
	trackParFunc func(ctx context.Context,
		key interface{},
		keySerde commtypes.Serde,
		topicName string,
		substreamId uint8,
	) error,
	runner func(ctx context.Context, m commtypes.Message, sink *source_sink.ShardedSharedLogStreamSyncSink,
		trackParFunc tran_interface.TrackKeySubStreamFunc,
	) error,
) {
	for {
		gotMsgs, err := src.Consume(ctx, 0)
		if err != nil {
			if errors.IsStreamEmptyError(err) || errors.IsStreamTimeoutError(err) || err == errors.ErrStreamSourceTimeout {
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
	kvmsgSerdes commtypes.KVMsgSerdes,
	payloadArrSerde commtypes.Serde,
	serdeFormat commtypes.SerdeFormat,
	log *sharedlog_stream.ShardedSharedLogStream,
) ([]commtypes.Message, error) {
	ret := make([]commtypes.Message, 0)
	tac, err := source_sink.NewTransactionAwareConsumer(log, serdeFormat)
	if err != nil {
		return nil, err
	}
	for {
		msg, err := tac.ReadNext(ctx, 0)
		if errors.IsStreamEmptyError(err) {
			return ret, nil
		} else if err != nil {
			return ret, err
		}
		debug.Fprintf(os.Stderr, "readMsgs: %s got %v\n", log.TopicName(), msg)
		debug.Fprintf(os.Stderr, "readMsgs: payload: %v\n", string(msg.Payload))
		msgAndSeq, err := commtypes.DecodeRawMsg(
			msg, kvmsgSerdes, payloadArrSerde, commtypes.DecodeMsg)
		if err != nil {
			return nil, fmt.Errorf("DecodeRawMsg err: %v", err)
		}
		if msgAndSeq.MsgArr != nil {
			ret = append(ret, msgAndSeq.MsgArr...)
		} else {
			ret = append(ret, msgAndSeq.Msg)
		}
	}
}

func pushMsgToStream(ctx context.Context, key int, val *strTs, kvmsgSerdes commtypes.KVMsgSerdes,
	log *sharedlog_stream.ShardedSharedLogStream, taskId uint64, taskEpoch uint16, transactionID uint64,
) error {
	encoded, err := commtypes.EncodeMsg(commtypes.Message{Key: key, Value: val}, kvmsgSerdes)
	if err != nil {
		return err
	}
	if encoded != nil {
		debug.Fprintf(os.Stderr, "%s encoded: \n", log.TopicName())
		debug.Fprintf(os.Stderr, "%s\n", string(encoded))
		debug.PrintByteSlice(encoded)
		_, err = log.Push(ctx, encoded, 0, false, false, taskId, taskEpoch, transactionID)
		return err
	}
	return nil
}

func pushMsgsToSink(ctx context.Context, sink *source_sink.ShardedSharedLogStreamSyncSink,
	msgs []commtypes.Message, trackParFunc tran_interface.TrackKeySubStreamFunc,
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
