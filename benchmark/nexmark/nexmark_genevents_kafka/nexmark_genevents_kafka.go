package main

import (
	"context"
	"flag"
	"time"

	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/generator"

	"sharedlog-stream/pkg/stream/processor/commtypes"

	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/rs/zerolog/log"
)

var (
	FLAGS_num_events    int
	FLAGS_duration      int
	FLAGS_broker        string
	FLAGS_stream_prefix string
	FLAGS_serdeFormat   string
)

func main() {
	flag.IntVar(&FLAGS_num_events, "num_events", 0, "")
	flag.IntVar(&FLAGS_duration, "duration", 60, "")
	flag.StringVar(&FLAGS_broker, "broker", "127.0.0.1", "")
	flag.StringVar(&FLAGS_stream_prefix, "stream_prefix", "nexmark", "")
	flag.StringVar(&FLAGS_serdeFormat, "serde", "json", "serde format: json or msgp")
	flag.Parse()

	var serdeFormat commtypes.SerdeFormat
	var valueEncoder commtypes.Encoder
	if FLAGS_serdeFormat == "json" {
		serdeFormat = commtypes.JSON
		valueEncoder = ntypes.EventJSONSerde{}
	} else if FLAGS_serdeFormat == "msgp" {
		serdeFormat = commtypes.MSGP
		valueEncoder = ntypes.EventMsgpSerde{}
	} else {
		log.Error().Msgf("serde format is not recognized; default back to JSON")
		serdeFormat = commtypes.JSON
		valueEncoder = ntypes.EventJSONSerde{}
	}

	topic := FLAGS_stream_prefix + "_src"
	newTopic := []kafka.TopicSpecification{
		{Topic: topic,
			NumPartitions:     1,
			ReplicationFactor: 1},
	}
	ctx := context.Background()
	err := common.CreateTopic(ctx, newTopic, FLAGS_broker)
	if err != nil {
		log.Fatal().Msgf("Failed to create topic: %s", err)
	}
	nexmarkConfigInput := ntypes.NewNexMarkConfigInput(topic, serdeFormat)
	nexmarkConfig, err := ntypes.ConvertToNexmarkConfiguration(nexmarkConfigInput)
	if err != nil {
		log.Fatal().Msgf("Failed to convert to nexmark configuration: %s", err)
	}
	generatorConfig := generator.NewGeneratorConfig(nexmarkConfig, uint64(time.Now().UnixMilli()), 1, uint64(nexmarkConfig.NumEvents), 1)
	eventGenerator := generator.NewSimpleNexmarkGenerator(generatorConfig)
	channel_url_cache := make(map[uint32]*generator.ChannelUrl)

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": FLAGS_broker})
	if err != nil {
		log.Fatal().Msgf("Failed to create producer: %s\n", err)
	}
	defer p.Close()

	deliveryChan := make(chan kafka.Event)

	idx := 0
	duration := time.Duration(FLAGS_duration) * time.Second
	start := time.Now()
	for {
		if time.Since(start) >= duration {
			break
		}
		if FLAGS_num_events != 0 && idx > FLAGS_num_events {
			break
		}
		now := time.Now().Unix()
		nextEvent, err := eventGenerator.NextEvent(ctx, channel_url_cache)
		if err != nil {
			log.Fatal().Msgf("next event failed: %s", err)
		}
		wtsSec := nextEvent.WallclockTimestamp / 1000.0
		if wtsSec > uint64(now) {
			time.Sleep(time.Duration(wtsSec-uint64(now)) * time.Second)
		}
		encoded, err := valueEncoder.Encode(nextEvent.Event)
		if err != nil {
			log.Fatal().Msgf("event serialization failed: %s", err)
		}
		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          encoded,
		}, deliveryChan)
		if err != nil {
			log.Fatal().Err(err)
		}
		idx += 1
	}
	replies := 0
	for e := range deliveryChan {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				log.Error().Msgf("Delivery failed: %v\n", ev.TopicPartition)
			} else {
				log.Debug().Msgf("Delivered message to %v\n", ev.TopicPartition)
			}
		}
		replies += 1
		if replies == idx {
			break
		}
	}
	remaining := p.Flush(30)
	log.Info().Msgf("producer: %d messages remaining in queue.", remaining)
	p.Close()
	close(deliveryChan)
}
