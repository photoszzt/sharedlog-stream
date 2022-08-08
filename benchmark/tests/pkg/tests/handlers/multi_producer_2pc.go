package handlers

import (
	"context"
	"fmt"
	"reflect"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/exactly_once_intr"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/producer_consumer"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream_task"
	"sharedlog-stream/pkg/transaction"

	"cs.utexas.edu/zjia/faas/types"
)

func getProduceTransactionManager(
	ctx context.Context, env types.Environment, transactionalID string,
	sink *producer_consumer.ShardedSharedLogStreamProducer[int, string],
	sp *common.QueryInput,
) (*transaction.TransactionManager, exactly_once_intr.TrackProdSubStreamFunc) {
	ectx := processor.NewExecutionContext([]producer_consumer.MeteredConsumerIntr{},
		[]producer_consumer.MeteredProducerIntr{producer_consumer.NewMeteredProducer(sink, 0)},
		"prodConsume", sp.ScaleEpoch, sp.ParNum)
	args1 := benchutil.UpdateStreamTaskArgs(sp, stream_task.NewStreamTaskArgsBuilder(env,
		&ectx, transactionalID)).Build()
	tm1, err := stream_task.SetupTransactionManager(ctx, args1)
	if err != nil {
		panic(err)
	}
	trackParFunc1 := func(ctx context.Context,
		key interface{},
		keySerde commtypes.Encoder,
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

/*
func getConsumeTransactionManager(
	ctx context.Context, env types.Environment, transactionalID string,
	src *producer_consumer.ShardedSharedLogStreamConsumer, sp *common.QueryInput,
) (*transaction.TransactionManager, exactly_once_intr.TrackProdSubStreamFunc) {
	ectx := processor.NewExecutionContext(
		[]producer_consumer.MeteredConsumerIntr{producer_consumer.NewMeteredConsumer(src, 0)},
		nil, "prodConsume", sp.ScaleEpoch, sp.ParNum)
	args1 := benchutil.UpdateStreamTaskArgs(sp, stream_task.NewStreamTaskArgsBuilder(env, &ectx, transactionalID)).Build()
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
*/

func (h *produceConsumeHandler) beginTransaction(ctx context.Context,
	tm *transaction.TransactionManager, stream1 *sharedlog_stream.ShardedSharedLogStream) {
	if err := tm.BeginTransaction(ctx); err != nil {
		panic(err)
	}
	if err := tm.AddTopicSubstream(ctx, stream1.TopicName(), 0); err != nil {
		panic(err)
	}
}

func (h *produceConsumeHandler) testMultiProducer2pc(ctx context.Context) {
	// two producer push to the same stream
	stream1, err := sharedlog_stream.NewShardedSharedLogStream(h.env, "test", 1, commtypes.JSON)
	if err != nil {
		panic(err)
	}
	stream1Copy, err := sharedlog_stream.NewShardedSharedLogStream(h.env, "test", 1, commtypes.JSON)
	if err != nil {
		panic(err)
	}

	msgSerdes := commtypes.MessageJSONSerdeG[int, string]{
		KeySerde: commtypes.IntSerdeG{},
		ValSerde: commtypes.StringSerdeG{},
	}
	produceSinkConfig := &producer_consumer.StreamSinkConfig[int, string]{
		MsgSerde:      msgSerdes,
		FlushDuration: common.FlushDuration,
	}
	sp := &common.QueryInput{
		AppId:         "prodConsume",
		GuaranteeMth:  uint8(exactly_once_intr.TWO_PHASE_COMMIT),
		CommitEveryMs: 5,
		FlushMs:       5,
		WarmupS:       0,
		ParNum:        0,
		ScaleEpoch:    1,
	}

	produceSink := producer_consumer.NewShardedSharedLogStreamProducer(stream1, produceSinkConfig)
	produceSinkCopy := producer_consumer.NewShardedSharedLogStreamProducer(stream1Copy, produceSinkConfig)

	tm1, trackParFunc1 := getProduceTransactionManager(ctx, h.env, "prod1", produceSink, sp)
	tm2, trackParFunc2 := getProduceTransactionManager(ctx, h.env, "prod2", produceSinkCopy, sp)
	tm1.RecordTopicStreams(stream1.TopicName(), stream1)
	tm2.RecordTopicStreams(stream1Copy.TopicName(), stream1Copy)
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
	if err = tm1.CommitTransaction(ctx); err != nil {
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
	if err = tm2.CommitTransaction(ctx); err != nil {
		panic(err)
	}

	stream1ForRead, err := sharedlog_stream.NewShardedSharedLogStream(h.env, "test", 1, commtypes.JSON)
	if err != nil {
		panic(err)
	}

	srcConfig := &producer_consumer.StreamConsumerConfigG[int, string]{
		Timeout:     common.SrcConsumeTimeout,
		MsgSerde:    msgSerdes,
		SerdeFormat: commtypes.JSON,
	}

	src1, err := producer_consumer.NewShardedSharedLogStreamConsumerG(stream1ForRead, srcConfig, 1, 0)
	if err != nil {
		panic(err)
	}
	err = src1.ConfigExactlyOnce(exactly_once_intr.TWO_PHASE_COMMIT)
	if err != nil {
		panic(err)
	}
	payloadArrSerde := sharedlog_stream.DEFAULT_PAYLOAD_ARR_SERDEG
	got, err := readMsgs[int, string](ctx, msgSerdes, payloadArrSerde, commtypes.JSON, stream1ForRead)
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
