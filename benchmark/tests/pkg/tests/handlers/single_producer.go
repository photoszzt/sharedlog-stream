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
	trackParFunc := exactly_once_intr.TrackProdSubStreamFunc(
		func(ctx context.Context, key interface{}, keySerde commtypes.Serde,
			topicName string, substreamId uint8,
		) error {
			_ = em.AddTopicSubstream(ctx, topicName, substreamId)
			return nil
		})
	return em, trackParFunc, nil
}

func (h *produceConsumeHandler) testSingleProducerEpoch(ctx context.Context) {
	debug.Fprintf(os.Stderr, "single producer epoch test\n")
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
	produceSink :=
		producer_consumer.NewShardedSharedLogStreamProducer(stream1, produceSinkConfig)
	meteredProducer := producer_consumer.NewMeteredProducer(produceSink, 0)
	em1, trackParFunc1, err := getEpochManager(ctx, h.env, "prod1")
	produceSink.ConfigExactlyOnce(em1, exactly_once_intr.EPOCH_MARK)
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
	msgForTm2 := []commtypes.Message{
		{
			Key:   2,
			Value: "tm2_a",
		},
	}
	err = pushMsgsToSink(ctx, produceSink, msgForTm2, trackParFunc1)
	if err != nil {
		panic(err)
	}
	err = produceSink.Flush(ctx)
	if err != nil {
		panic(err)
	}
	err = em1.MarkEpoch(ctx, nil, []producer_consumer.MeteredProducerIntr{meteredProducer})
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
	src1.ConfigExactlyOnce(commtypes.JSON, exactly_once_intr.EPOCH_MARK)
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
