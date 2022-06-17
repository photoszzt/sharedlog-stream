package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/benchmark/tests/pkg/tests/test_types"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/producer_consumer"
	"sharedlog-stream/pkg/stream_task"
	"sharedlog-stream/pkg/transaction"
	"sharedlog-stream/pkg/exactly_once_intr"

	"cs.utexas.edu/zjia/faas/types"
)

type multiProducerHandler struct {
	env types.Environment
}

func NewMultiProducerHandler(env types.Environment) types.FuncHandler {
	return &multiProducerHandler{
		env: env,
	}
}

func (h *multiProducerHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
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

func (h *multiProducerHandler) tests(ctx context.Context, sp *test_types.TestInput) *common.FnOutput {
	if sp.TestName == "multiProducer" {
		h.testMultiProducer(ctx)
	} else {
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

func (h *multiProducerHandler) getProduceTransactionManager(
	ctx context.Context, transactionalID string, sink producer_consumer.Producer,
) (*transaction.TransactionManager, exactly_once_intr.TrackProdSubStreamFunc) {
	args1 := benchutil.UpdateStreamTaskArgs(&common.QueryInput{},
		stream_task.NewStreamTaskArgsBuilder(h.env, nil, transactionalID)).Build()
	tm1, err := stream_task.SetupTransactionManager(ctx, args1)
	if err != nil {
		panic(err)
	}
	trackParFunc1 := func(ctx context.Context,
		key interface{},
		keySerde commtypes.Serde,
		topicName string,
		substreamId uint8,
	) error {
		err := tm1.AddTopicSubstream(ctx, topicName, substreamId)
		if err != nil {
			return err
		}
		return err
	}
	return tm1, trackParFunc1
}

func (h *multiProducerHandler) getConsumeTransactionManager(
	ctx context.Context, transactionalID string, src producer_consumer.Consumer,
) (*transaction.TransactionManager, exactly_once_intr.TrackProdSubStreamFunc) {
	args1 := benchutil.UpdateStreamTaskArgs(&common.QueryInput{},
		stream_task.NewStreamTaskArgsBuilder(h.env, nil, transactionalID)).Build()
	tm1, err := stream_task.SetupTransactionManager(ctx, args1)
	if err != nil {
		panic(err)
	}
	trackParFunc1 := func(ctx context.Context,
		key interface{},
		keySerde commtypes.Serde,
		topicName string,
		substreamId uint8,
	) error {
		err := tm1.AddTopicSubstream(ctx, topicName, substreamId)
		if err != nil {
			return err
		}
		return err
	}
	return tm1, trackParFunc1
}

func (h *multiProducerHandler) beginTransaction(ctx context.Context,
	tm *transaction.TransactionManager, stream1 *sharedlog_stream.ShardedSharedLogStream) {
	if err := tm.BeginTransaction(ctx, nil, nil); err != nil {
		panic(err)
	}
	if err := tm.AddTopicSubstream(ctx, stream1.TopicName(), 0); err != nil {
		panic(err)
	}
}

func (h *multiProducerHandler) testMultiProducer(ctx context.Context) {
	// two producer push to the same stream
	stream1, err := sharedlog_stream.NewShardedSharedLogStream(h.env, "test", 1, commtypes.JSON)
	if err != nil {
		panic(err)
	}
	stream1Copy, err := sharedlog_stream.NewShardedSharedLogStream(h.env, "test", 1, commtypes.JSON)
	if err != nil {
		panic(err)
	}

	kvmsgSerdes := commtypes.KVMsgSerdes{
		KeySerde: commtypes.IntSerde{},
		ValSerde: commtypes.StringSerde{},
		MsgSerde: commtypes.MessageSerializedJSONSerde{},
	}
	produceSinkConfig := &producer_consumer.StreamSinkConfig{
		KVMsgSerdes:   kvmsgSerdes,
		FlushDuration: common.FlushDuration,
	}

	produceSink := producer_consumer.NewShardedSharedLogStreamProducer(stream1, produceSinkConfig)
	produceSinkCopy := producer_consumer.NewShardedSharedLogStreamProducer(stream1Copy, produceSinkConfig)

	tm1, trackParFunc1 := h.getProduceTransactionManager(ctx, "prod1", produceSink)
	tm2, trackParFunc2 := h.getProduceTransactionManager(ctx, "prod2", produceSinkCopy)
	tm1.RecordTopicStreams(stream1.TopicName(), stream1)
	tm2.RecordTopicStreams(stream1.TopicName(), stream1)
	produceSink.ConfigExactlyOnce(tm1, exactly_once_intr.TWO_PHASE_COMMIT)
	produceSinkCopy.ConfigExactlyOnce(tm2, exactly_once_intr.TWO_PHASE_COMMIT)

	h.beginTransaction(ctx, tm1, stream1)
	h.beginTransaction(ctx, tm2, stream1Copy)

	// producer1 push 1 msg to sink
	msgForTm1 := []commtypes.Message{
		{
			Key:   1,
			Value: "tm1_a",
		},
	}
	err = pushMsgsToSink(ctx, produceSink, msgForTm1, trackParFunc1)
	if err != nil {
		panic(err)
	}
	produceSink.Flush(ctx)

	// then producer2 produce 1 msg to sink
	msgForTm2 := []commtypes.Message{
		{
			Key:   2,
			Value: "tm2_a",
		},
	}
	err = pushMsgsToSink(ctx, produceSinkCopy, msgForTm2, trackParFunc2)
	if err != nil {
		panic(err)
	}
	produceSinkCopy.Flush(ctx)

	// producer1 push 1 msg to sink
	msgForTm1 = []commtypes.Message{
		{
			Key:   3,
			Value: "tm1_b",
		},
	}
	err = pushMsgsToSink(ctx, produceSink, msgForTm1, trackParFunc1)
	if err != nil {
		panic(err)
	}
	produceSink.Flush(ctx)

	// producer1 commits
	if err = tm1.CommitTransaction(ctx, nil, nil); err != nil {
		panic(err)
	}

	// producer2 outputs two message
	msgForTm2 = []commtypes.Message{
		{
			Key:   4,
			Value: "tm2_b",
		},
		{
			Key:   5,
			Value: "tm2_c",
		},
	}
	err = pushMsgsToSink(ctx, produceSinkCopy, msgForTm2, trackParFunc2)
	if err != nil {
		panic(err)
	}
	produceSinkCopy.Flush(ctx)

	// producer2 commits
	if err = tm2.CommitTransaction(ctx, nil, nil); err != nil {
		panic(err)
	}

	stream1ForRead, err := sharedlog_stream.NewShardedSharedLogStream(h.env, "test", 1, commtypes.JSON)
	if err != nil {
		panic(err)
	}

	srcConfig := &producer_consumer.StreamConsumerConfig{
		Timeout:     common.SrcConsumeTimeout,
		KVMsgSerdes: kvmsgSerdes,
	}

	src1 := producer_consumer.NewShardedSharedLogStreamConsumer(stream1ForRead, srcConfig)
	src1.ConfigExactlyOnce(commtypes.JSON, exactly_once_intr.TWO_PHASE_COMMIT)
	payloadArrSerde := sharedlog_stream.DEFAULT_PAYLOAD_ARR_SERDE
	got, err := readMsgs(ctx, kvmsgSerdes, payloadArrSerde, commtypes.JSON, stream1ForRead)
	if err != nil {
		panic(err)
	}
	expected_msgs := []commtypes.Message{
		{
			Key:   1,
			Value: "tm1_a",
		},
		{
			Key:   2,
			Value: "tm2_a",
		},
		{
			Key:   3,
			Value: "tm1_b",
		},
		{
			Key:   4,
			Value: "tm2_b",
		},
		{
			Key:   5,
			Value: "tm2_c",
		},
	}
	if !reflect.DeepEqual(expected_msgs, got) {
		panic(fmt.Sprintf("should equal. expected: %v, got: %v", expected_msgs, got))
	}
}
