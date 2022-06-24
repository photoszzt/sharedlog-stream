package handlers

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/epoch_manager"
	"sharedlog-stream/pkg/exactly_once_intr"
	"sharedlog-stream/pkg/producer_consumer"
	"sharedlog-stream/pkg/sharedlog_stream"

	"cs.utexas.edu/zjia/faas/types"
	"golang.org/x/xerrors"
)

func getEpochManager(ctx context.Context, env types.Environment,
	transactionalID string,
) (*epoch_manager.EpochManager, exactly_once_intr.TrackProdSubStreamFunc, error) {
	em, err := epoch_manager.NewEpochManager(env, transactionalID, commtypes.JSON)
	if err != nil {
		return nil, nil, err
	}
	_, _, err = em.Init(ctx)
	if err != nil {
		return nil, nil, err
	}
	trackParFunc := exactly_once_intr.TrackProdSubStreamFunc(
		func(ctx context.Context, key interface{}, keySerde commtypes.Serde,
			topicName string, substreamId uint8,
		) error {
			_ = em.AddTopicSubstream(ctx, topicName, substreamId)
			return nil
		})
	return em, trackParFunc, nil
}

func (h *produceConsumeHandler) testSingleProduceConsumeEpoch(ctx context.Context) {
	debug.Fprintf(os.Stderr, "single produce consume epoch test\n")
	stream1, err := sharedlog_stream.NewShardedSharedLogStream(h.env, "test2", 1, commtypes.JSON)
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
	meteredProducer := producer_consumer.NewMeteredProducer(producer_consumer.NewShardedSharedLogStreamProducer(stream1, produceSinkConfig), 0)
	em1, trackParFunc1, err := getEpochManager(ctx, h.env, "prod1")
	if err != nil {
		panic(err)
	}
	meteredProducer.ConfigExactlyOnce(em1, exactly_once_intr.EPOCH_MARK)
	msgForTm1 := []commtypes.Message{
		{
			Key:   1,
			Value: "tm1_a",
		},
	}
	err = pushMsgsToSink(ctx, meteredProducer, msgForTm1, trackParFunc1)
	if err != nil {
		panic(err)
	}
	meteredProducer.Flush(ctx)
	msgForTm2 := []commtypes.Message{
		{
			Key:   2,
			Value: "tm2_a",
		},
	}
	err = pushMsgsToSink(ctx, meteredProducer, msgForTm2, trackParFunc1)
	if err != nil {
		panic(err)
	}
	err = meteredProducer.Flush(ctx)
	if err != nil {
		panic(err)
	}
	producers := []producer_consumer.MeteredProducerIntr{meteredProducer}
	err = em1.MarkEpoch(ctx, nil, producers)
	if err != nil {
		panic(err)
	}

	srcConfig := &producer_consumer.StreamConsumerConfig{
		Timeout:     common.SrcConsumeTimeout,
		KVMsgSerdes: kvmsgSerdes,
	}
	stream1ForRead, err := sharedlog_stream.NewShardedSharedLogStream(h.env, "test2", 1, commtypes.JSON)
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
	}
	if !reflect.DeepEqual(expected_msgs, got) {
		panic(fmt.Sprintf("should equal. expected: %v, got: %v", expected_msgs, got))
	}

	debug.Fprintf(os.Stderr, "single producer produces two epochs\n")
	// two epochs
	msgForTm1 = []commtypes.Message{
		{
			Key:   3,
			Value: "tm1_c",
		},
		{
			Key:   4,
			Value: "tm1_d",
		},
	}
	err = pushMsgsToSink(ctx, meteredProducer, msgForTm1, trackParFunc1)
	if err != nil {
		panic(err)
	}
	err = em1.MarkEpoch(ctx, nil, producers)
	if err != nil {
		panic(err)
	}

	msgForTm1 = []commtypes.Message{
		{
			Key:   5,
			Value: "tm1_e",
		},
		{
			Key:   6,
			Value: "tm1_f",
		},
	}
	err = pushMsgsToSink(ctx, meteredProducer, msgForTm1, trackParFunc1)
	if err != nil {
		panic(err)
	}
	err = em1.MarkEpoch(ctx, nil, producers)
	if err != nil {
		panic(err)
	}

	got, err = readMsgsEpoch(ctx, src1)
	if err != nil {
		panic(err)
	}
	expected_msgs = []commtypes.Message{
		{
			Key:   3,
			Value: "tm1_c",
		},
		{
			Key:   4,
			Value: "tm1_d",
		},
		{
			Key:   5,
			Value: "tm1_e",
		},
		{
			Key:   6,
			Value: "tm1_f",
		},
	}
	if !reflect.DeepEqual(expected_msgs, got) {
		panic(fmt.Sprintf("should equal. expected: %v, got: %v", expected_msgs, got))
	}
}

func readMsgsEpoch(ctx context.Context, consumer *producer_consumer.ShardedSharedLogStreamConsumer) ([]commtypes.Message, error) {
	ret := make([]commtypes.Message, 0)
	for {
		gotMsgs, err := consumer.Consume(ctx, 0)
		if xerrors.Is(err, common_errors.ErrStreamSourceTimeout) {
			return ret, nil
		} else if err != nil {
			return ret, err
		}
		for _, msgAndSeq := range gotMsgs.Msgs {
			if msgAndSeq.MsgArr != nil {
				ret = append(ret, msgAndSeq.MsgArr...)
			} else {
				ret = append(ret, msgAndSeq.Msg)
			}
		}
	}
}