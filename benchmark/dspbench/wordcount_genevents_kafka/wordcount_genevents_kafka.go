package main

import (
	"context"
	"flag"
	"sharedlog-stream/benchmark/common/kafka_utils"
	"sharedlog-stream/benchmark/dspbench/pkg/handlers/wordcount"
	"sharedlog-stream/pkg/commtypes"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/rs/zerolog/log"
)

var (
	FLAGS_num_events int
	FLAGS_duration   int
	FLAGS_broker     string
	FLAGS_file_name  string
)

func main() {
	flag.IntVar(&FLAGS_num_events, "num_events", 0, "")
	flag.IntVar(&FLAGS_duration, "duration", 60, "")
	flag.StringVar(&FLAGS_broker, "broker", "127.0.0.1", "")
	flag.StringVar(&FLAGS_file_name, "in_fname", "/mnt/data/books.dat", "data source file name")
	flag.Parse()

	topic := "wc_src"
	newTopic := []kafka.TopicSpecification{
		{Topic: topic,
			NumPartitions:     1,
			ReplicationFactor: 1},
	}
	ctx := context.Background()
	err := kafka_utils.CreateTopic(ctx, newTopic, FLAGS_broker)
	if err != nil {
		panic(err)
	}

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": FLAGS_broker})
	if err != nil {
		log.Fatal().Msgf("Failed to create producer: %s\n", err)
	}
	defer p.Close()

	deliveryChan := make(chan kafka.Event)

	strSerde := commtypes.StringSerde{}

	lines := make([]string, 0, 128)

	err = wordcount.ParseFile(FLAGS_file_name, &lines)
	if err != nil {
		log.Fatal().Msgf("fail to parse file: %s", err)
	}
	if len(lines) == 0 {
		log.Fatal().Msgf("fail to read files back")
	}
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
		lineNum := idx % len(lines)
		encoded, err := strSerde.Encode(lines[lineNum])
		if err != nil {
			log.Fatal().Msgf("fail to serialize: %s", err)
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
