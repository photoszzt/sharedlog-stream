package common

import (
	"context"
	"fmt"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/rs/zerolog/log"
)

func CreateTopic(ctx context.Context, topics []kafka.TopicSpecification, bootstrapServer string) error {
	conf := kafka.ConfigMap{"bootstrap.servers": bootstrapServer}
	adminClient, err := kafka.NewAdminClient(&conf)
	if err != nil {
		return err
	}
	result, err := adminClient.CreateTopics(ctx, topics)
	if err != nil {
		return err
	}
	for _, res := range result {
		switch res.Error.Code() {
		case kafka.ErrTopicAlreadyExists:
			log.Error().Msgf("Failed to create topic %s: %v", res.Topic, res.Error)
		case kafka.ErrNoError:
			log.Error().Msgf("Succeed to create topic %s", res.Topic)
		default:
			return fmt.Errorf("failed to create topic %s: %v", res.Topic, res.Error)
		}
	}
	return nil
}

func StringToSerdeFormat(format string) commtypes.SerdeFormat {
	var serdeFormat commtypes.SerdeFormat
	if format == "json" {
		serdeFormat = commtypes.JSON
	} else if format == "msgp" {
		serdeFormat = commtypes.MSGP
	} else {
		log.Error().Msgf("serde format is not recognized; default back to JSON")
		serdeFormat = commtypes.JSON
	}
	return serdeFormat
}

type PayloadToPush struct {
	Payload    []byte
	Partitions []uint8
	IsControl  bool
}

type StreamPush struct {
	FlushTimer    time.Time
	MsgChan       chan PayloadToPush
	MsgErrChan    chan error
	Stream        *sharedlog_stream.ShardedSharedLogStream
	FlushDuration time.Duration
	BufPush       bool
}

func (h *StreamPush) AsyncStreamPush(ctx context.Context, wg *sync.WaitGroup,
) {
	defer wg.Done()
	for msg := range h.MsgChan {
		if msg.IsControl {
			if h.BufPush {
				err := h.Stream.Flush(ctx)
				if err != nil {
					h.MsgErrChan <- err
					return
				}
			}
			for _, i := range msg.Partitions {
				_, err := h.Stream.Push(ctx, msg.Payload, i, true, false)
				if err != nil {
					h.MsgErrChan <- err
					return
				}
			}
		} else {
			if h.BufPush {
				err := h.Stream.BufPushNoLock(ctx, msg.Payload, uint8(msg.Partitions[0]))
				if err != nil {
					h.MsgErrChan <- err
					return
				}
				if time.Since(h.FlushTimer) >= h.FlushDuration {
					h.Stream.FlushNoLock(ctx)
					h.FlushTimer = time.Now()
				}
			} else {
				debug.Assert(len(msg.Partitions) == 1, "should only have one partition")
				_, err := h.Stream.Push(ctx, msg.Payload, uint8(msg.Partitions[0]), false, false)
				if err != nil {
					h.MsgErrChan <- err
					return
				}
			}

		}
	}
}
