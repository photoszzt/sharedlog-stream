package handlers

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/epoch_manager"
	"sharedlog-stream/pkg/exactly_once_intr"
	"sharedlog-stream/pkg/optional"
	"sharedlog-stream/pkg/producer_consumer"
	"sharedlog-stream/pkg/stream_task"

	"cs.utexas.edu/zjia/faas/types"
	"golang.org/x/xerrors"
)

func getEpochManager(ctx context.Context, env types.Environment,
	transactionalID string, serdeFormat commtypes.SerdeFormat,
) (*epoch_manager.EpochManager, exactly_once_intr.TrackProdSubStreamFunc, error) {
	em, err := epoch_manager.NewEpochManager(env, transactionalID, serdeFormat)
	if err != nil {
		return nil, nil, err
	}
	_, _, err = em.Init(ctx)
	if err != nil {
		return nil, nil, err
	}
	trackParFunc := exactly_once_intr.TrackProdSubStreamFunc(
		func(topicName string, substreamId uint8,
		) {
			em.AddTopicSubstream(topicName, substreamId)
		})
	return em, trackParFunc, nil
}

func (h *produceConsumeHandler) testSingleProduceConsumeEpoch(ctx context.Context,
	serdeFormat commtypes.SerdeFormat, topicName string,
) {
	debug.Fprintf(os.Stderr, "single produce consume epoch test\n")
	default_max_buf := uint32(32 * 1024)
	srcStreams, sinkStreams, err := benchutil.GetShardedInputOutputStreamsTest(ctx, h.env, &common.TestParam{
		InStreamParam: []common.TestStreamParam{
			{TopicName: topicName, NumPartition: 1},
		},
		OutStreamParam: []common.TestStreamParam{
			{TopicName: topicName, NumPartition: 1},
		},
		AppId:         "testStreamStreamJoinMem",
		CommitEveryMs: 100,
		FlushMs:       100,
		BufMaxSize:    default_max_buf,
		SerdeFormat:   uint8(commtypes.JSON),
	})
	if err != nil {
		panic(err)
	}
	produceSinkConfig := &producer_consumer.StreamSinkConfig{
		FlushDuration: common.FlushDuration,
		Format:        serdeFormat,
	}
	meteredProducer, err := producer_consumer.NewMeteredProducer(
		producer_consumer.NewShardedSharedLogStreamProducer(sinkStreams[0],
			produceSinkConfig), 0)
	if err != nil {
		panic(err)
	}
	em1, trackParFunc1, err := getEpochManager(ctx, h.env,
		"prod1_single"+serdeFormat.String(), serdeFormat)
	if err != nil {
		panic(err)
	}
	meteredProducer.ConfigExactlyOnce(em1, exactly_once_intr.EPOCH_MARK)
	msgForTm1 := []commtypes.MessageG[int, string]{
		{
			Key:   optional.Some(1),
			Value: optional.Some("tm1_a"),
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
	_, err = meteredProducer.Flush(ctx)
	if err != nil {
		panic(err)
	}
	producers := []producer_consumer.MeteredProducerIntr{meteredProducer}
	epochMarker, epochMarkerTags, epochMarkerTopics, err := stream_task.CaptureEpochStateAndCleanupExplicit(ctx, em1, nil, producers, nil, nil)
	if err != nil {
		panic(err)
	}
	err = em1.MarkEpoch(ctx, epochMarker, epochMarkerTags, epochMarkerTopics)
	if err != nil {
		panic(err)
	}

	srcConfig := &producer_consumer.StreamConsumerConfig{
		Timeout:     common.SrcConsumeTimeout,
		SerdeFormat: commtypes.JSON,
	}
	src1, err := producer_consumer.NewShardedSharedLogStreamConsumer(srcStreams[0], srcConfig, 1, 0)
	if err != nil {
		panic(err)
	}
	src1.ConfigExactlyOnce(exactly_once_intr.EPOCH_MARK)
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
	epochMarker, epochMarkerTags, epochMarkerTopics, err = stream_task.CaptureEpochStateAndCleanupExplicit(ctx, em1, nil, producers, nil, nil)
	if err != nil {
		panic(err)
	}
	err = em1.MarkEpoch(ctx, epochMarker, epochMarkerTags, epochMarkerTopics)
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
	epochMarker, epochMarkerTags, epochMarkerTopics, err = stream_task.CaptureEpochStateAndCleanupExplicit(ctx, em1, nil, producers, nil, nil)
	if err != nil {
		panic(err)
	}
	err = em1.MarkEpoch(ctx, epochMarker, epochMarkerTags, epochMarkerTopics)
	if err != nil {
		panic(err)
	}

	got, err = readMsgsEpoch(ctx, src1)
	if err != nil {
		panic(err)
	}
	expected_msgs = []commtypes.Message{
		{Key: 3, Value: "tm1_c"},
		{Key: 4, Value: "tm1_d"},
		{Key: 5, Value: "tm1_e"},
		{Key: 6, Value: "tm1_f"},
	}
	if !reflect.DeepEqual(expected_msgs, got) {
		panic(fmt.Sprintf("should equal. expected: %v, got: %v", expected_msgs, got))
	}
}

func readMsgsEpoch[KIn, VIn any](ctx context.Context,
	consumer *producer_consumer.ShardedSharedLogStreamConsumer,
) ([]commtypes.Message, error) {
	ret := make([]commtypes.Message, 0)
	for {
		gotMsgs, err := consumer.Consume(ctx, 0)
		if xerrors.Is(err, common_errors.ErrStreamSourceTimeout) {
			return ret, nil
		} else if err != nil {
			return ret, err
		}
		commtypes.DecodeRawMsgSeqG(gotMsgs)
		if gotMsgs.MsgArr != nil {
			ret = append(ret, msgAndSeq.MsgArr...)
		} else {
			ret = append(ret, msgAndSeq.Msg)
		}
	}
}
