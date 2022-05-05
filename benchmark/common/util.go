package common

import (
	"context"
	"fmt"
	"sharedlog-stream/pkg/stream/processor/commtypes"

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
