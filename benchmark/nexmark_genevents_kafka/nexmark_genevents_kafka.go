package main

import (
	"context"
	"flag"
	"time"

	"cs.utexas.edu/zhitingz/sharedlog-stream/pkg/nexmark/generator"
	ntypes "cs.utexas.edu/zhitingz/sharedlog-stream/pkg/nexmark/types"
	"cs.utexas.edu/zhitingz/sharedlog-stream/pkg/stream/processor"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/rs/zerolog/log"
)

var (
	FLAGS_num_events    int
	FLAGS_broker        string
	FLAGS_stream_prefix string
	FLAGS_serdeFormat   string
)

func main() {
	flag.IntVar(&FLAGS_num_events, "num_events", 10, "")
	flag.StringVar(&FLAGS_broker, "broker", "127.0.0.1", "")
	flag.StringVar(&FLAGS_stream_prefix, "stream_prefix", "nexmark", "")
	flag.StringVar(&FLAGS_serdeFormat, "serde", "json", "serde format: json or msgp")
	flag.Parse()

	var serdeFormat ntypes.SerdeFormat
	var valueEncoder processor.Encoder
	if FLAGS_serdeFormat == "json" {
		serdeFormat = ntypes.JSON
		valueEncoder = ntypes.EventJSONEncoder{}
	} else if FLAGS_serdeFormat == "msgp" {
		serdeFormat = ntypes.MSGP
		valueEncoder = ntypes.EventMsgpEncoder{}
	} else {
		log.Error().Msgf("serde format is not recognized; default back to JSON")
		serdeFormat = ntypes.JSON
		valueEncoder = ntypes.EventJSONEncoder{}
	}

	nexmarkConfigInput := ntypes.NewNexMarkConfigInput(FLAGS_stream_prefix+"_src", serdeFormat)
	nexmarkConfig, err := ntypes.ConvertToNexmarkConfiguration(nexmarkConfigInput)
	if err != nil {
		log.Fatal().Msgf("Failed to convert to nexmark configuration: %s", err)
	}
	generatorConfig := generator.NewGeneratorConfig(nexmarkConfig, uint64(time.Now().Unix()*1000), 1, uint64(nexmarkConfig.NumEvents), 1)
	eventGenerator := generator.NewSimpleNexmarkGenerator(generatorConfig)
	channel_url_cache := make(map[uint32]*generator.ChannelUrl)

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": FLAGS_broker})
	if err != nil {
		log.Fatal().Msgf("Failed to create producer: %s\n", err)
	}
	defer p.Close()

	deliveryChan := make(chan kafka.Event)
	topic := FLAGS_stream_prefix + "_src"
	ctx := context.Background()

	for i := 0; i < FLAGS_num_events; i++ {
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
		if replies == FLAGS_num_events {
			break
		}
	}
}
