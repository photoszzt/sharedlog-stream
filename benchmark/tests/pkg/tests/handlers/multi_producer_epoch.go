package handlers

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/exactly_once_intr"
	"sharedlog-stream/pkg/producer_consumer"
	"sharedlog-stream/pkg/sharedlog_stream"
)

func (h *produceConsumeHandler) testMultiProducerEpoch(ctx context.Context) {
	debug.Fprintf(os.Stderr, "multi producer epoch test\n")
	stream1, err := sharedlog_stream.NewShardedSharedLogStream(h.env, "test3", 1, commtypes.JSON)
	if err != nil {
		panic(err)
	}

	stream2, err := sharedlog_stream.NewShardedSharedLogStream(h.env, "test3", 1, commtypes.JSON)
	if err != nil {
		panic(err)
	}

	kvmsgSerdes := commtypes.KVMsgSerdes{
		KeySerde: commtypes.IntSerde{},
		ValSerde: commtypes.StringSerde{},
		MsgSerde: commtypes.MessageSerializedJSONSerde{},
	}
	meteredProducer1 := producer_consumer.NewMeteredProducer(
		producer_consumer.NewShardedSharedLogStreamProducer(stream1,
			&producer_consumer.StreamSinkConfig{
				KVMsgSerdes:   kvmsgSerdes,
				FlushDuration: common.FlushDuration,
			}), 0)
	meteredProducer2 := producer_consumer.NewMeteredProducer(
		producer_consumer.NewShardedSharedLogStreamProducer(stream2,
			&producer_consumer.StreamSinkConfig{
				KVMsgSerdes:   kvmsgSerdes,
				FlushDuration: common.FlushDuration,
			}), 0)
	em1, trackParFunc1, err := getEpochManager(ctx, h.env, "prod1")
	if err != nil {
		panic(err)
	}
	em2, trackParFunc2, err := getEpochManager(ctx, h.env, "prod2")
	if err != nil {
		panic(err)
	}
	meteredProducer1.ConfigExactlyOnce(em1, exactly_once_intr.EPOCH_MARK)
	meteredProducer2.ConfigExactlyOnce(em2, exactly_once_intr.EPOCH_MARK)

	// producer1 push 1 msg to sink
	msgForTm1 := []commtypes.Message{
		{
			Key:   1,
			Value: "tm1_a",
		},
	}
	err = pushMsgsToSink(ctx, meteredProducer1, msgForTm1, trackParFunc1)
	if err != nil {
		panic(err)
	}
	meteredProducer1.Flush(ctx)

	// then producer2 produce 1 msg to sink
	msgForTm2 := []commtypes.Message{
		{
			Key:   2,
			Value: "tm2_a",
		},
	}
	err = pushMsgsToSink(ctx, meteredProducer2, msgForTm2, trackParFunc2)
	if err != nil {
		panic(err)
	}
	meteredProducer2.Flush(ctx)

	// producer1 push 1 msg to sink
	msgForTm1 = []commtypes.Message{
		{
			Key:   3,
			Value: "tm1_b",
		},
	}
	err = pushMsgsToSink(ctx, meteredProducer1, msgForTm1, trackParFunc1)
	if err != nil {
		panic(err)
	}
	meteredProducer1.Flush(ctx)

	// producer1 mark
	producers1 := []producer_consumer.MeteredProducerIntr{meteredProducer1}
	err = em1.MarkEpoch(ctx, nil, producers1)
	if err != nil {
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
	err = pushMsgsToSink(ctx, meteredProducer2, msgForTm2, trackParFunc2)
	if err != nil {
		panic(err)
	}
	meteredProducer2.Flush(ctx)
	producers2 := []producer_consumer.MeteredProducerIntr{meteredProducer2}
	err = em2.MarkEpoch(ctx, nil, producers2)
	if err != nil {
		panic(err)
	}

	srcConfig := &producer_consumer.StreamConsumerConfig{
		Timeout:     common.SrcConsumeTimeout,
		KVMsgSerdes: kvmsgSerdes,
	}
	stream1ForRead, err := sharedlog_stream.NewShardedSharedLogStream(h.env, "test3", 1, commtypes.JSON)
	if err != nil {
		panic(err)
	}
	src1 := producer_consumer.NewShardedSharedLogStreamConsumer(stream1ForRead, srcConfig)
	err = src1.ConfigExactlyOnce(commtypes.JSON, exactly_once_intr.EPOCH_MARK)
	if err != nil {
		panic(err)
	}
	got, err := readMsgsEpoch(ctx, src1)
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
