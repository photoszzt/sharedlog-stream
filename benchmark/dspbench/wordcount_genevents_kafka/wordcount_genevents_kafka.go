package wordcountgeneventskafka

import (
	"flag"
	"sharedlog-stream/benchmark/dspbench/pkg/handlers/wordcount"
	"sharedlog-stream/pkg/stream/processor"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/rs/zerolog/log"
)

var (
	FLAGS_num_events  int
	FLAGS_broker      string
	FLAGS_serdeFormat string
	FLAGS_file_name   string
)

func main() {
	flag.IntVar(&FLAGS_num_events, "num_events", 10, "")
	flag.StringVar(&FLAGS_broker, "broker", "127.0.0.1", "")
	flag.StringVar(&FLAGS_serdeFormat, "serde", "json", "serde format: json or msgp")
	flag.StringVar(&FLAGS_file_name, "in_fname", "/mnt/data/books.dat", "data source file name")
	flag.Parse()

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": FLAGS_broker})
	if err != nil {
		log.Fatal().Msgf("Failed to create producer: %s\n", err)
	}
	defer p.Close()

	deliveryChan := make(chan kafka.Event)
	topic := "wc_src"
	strSerde := processor.StringSerde{}

	lines := make([]string, 0, 128)
	err = wordcount.ParseFile(FLAGS_file_name, lines)
	if err != nil {
		log.Fatal().Msgf("fail to parse file: %s", err)
	}
	for i := 0; i < FLAGS_num_events; i++ {
		idx := i
		if idx >= len(lines) {
			idx = idx % len(lines)
		}
		encoded, err := strSerde.Encode(lines[idx])
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
