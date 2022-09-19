package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"sort"
	"time"

	"sharedlog-stream/benchmark/common/kafka_utils"
	"sharedlog-stream/pkg/stats"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/rs/zerolog"
)

var (
	FLAGS_broker       string
	FLAGS_inTopicName  string
	FLAGS_outTopicName string
	FLAGS_duration     int
	FLAGS_events_num   int
	FLAGS_flushms      int
)

func init() {
	logLevel := os.Getenv("LOG_LEVEL")
	if level, err := zerolog.ParseLevel(logLevel); err == nil {
		zerolog.SetGlobalLevel(level)
	} else {
		zerolog.SetGlobalLevel(zerolog.WarnLevel)
	}
}

func main() {
	flag.StringVar(&FLAGS_broker, "broker", "127.0.0.1", "")
	flag.IntVar(&FLAGS_events_num, "events_num", 100000000, "number of events")
	flag.StringVar(&FLAGS_inTopicName, "inTopicName", "src", "topic name")
	flag.StringVar(&FLAGS_outTopicName, "outTopicName", "src", "topic name")
	flag.IntVar(&FLAGS_duration, "duration", 80, "")
	flag.IntVar(&FLAGS_flushms, "flushms", 0, "flush interval in ms")
	flag.Parse()

	ctx := context.Background()
	newTopic := kafka_utils.CreateTopicSpecification(FLAGS_outTopicName, 1)
	err := kafka_utils.CreateTopic(ctx, newTopic, FLAGS_broker)
	if err != nil {
		panic(fmt.Sprintf("Failed to create topic: %s", err))
	}

	consumerConfig := &kafka.ConfigMap{
		"client.id":              "processor",
		"bootstrap.servers":      FLAGS_broker,
		"group.id":               "bench",
		"auto.offset.reset":      "earliest",
		"enable.auto.commit":     false,
		"fetch.wait.max.ms":      5,
		"fetch.error.backoff.ms": 5,
	}
	producers := make(map[int32]*kafka.Producer)

	consumer, err := kafka.NewConsumer(consumerConfig)
	if err != nil {
		panic(err)
	}

	err = consumer.Subscribe(FLAGS_inTopicName, func(c *kafka.Consumer, e kafka.Event) error {
		return kafka_utils.GroupRebalance(FLAGS_broker, FLAGS_flushms, producers, c, e)
	})
	commitEvery := time.Duration(100) * time.Millisecond
	duration := time.Duration(FLAGS_duration) * time.Second
	handleConsume := func(w http.ResponseWriter, req *http.Request) {
		rest := FLAGS_events_num
		idx := 0
		commitTimer := time.NewTicker(commitEvery)
		commitTimes := make([]int64, 0, 4096)
		start := time.Now()
		for {
			select {
			case <-commitTimer.C:
				cBeg := time.Now()
				err = kafka_utils.CommitTransactionForInputPartition(producers, consumer, FLAGS_inTopicName, 0)
				cElapsed := time.Since(cBeg)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Failed to commit transaction: %s", err)
				}
				commitTimes = append(commitTimes, cElapsed.Microseconds())
			default:
			}
			if (duration > 0 && time.Since(start) > duration) ||
				(rest > 0 && idx >= rest) {
				commitTimer.Stop()
				break
			}
			ev := consumer.Poll(5)
			if ev == nil {
				continue
			}
			switch ev := ev.(type) {
			case *kafka.Message:
				producers[0].Produce(&kafka.Message{
					TopicPartition: kafka.TopicPartition{
						Topic:     &FLAGS_outTopicName,
						Partition: kafka.PartitionAny,
					},
					Value: ev.Value,
				}, nil)
			case kafka.Error:
				fmt.Fprintf(os.Stderr, "%% Error: %v\n", ev)
				break
			}
		}
		consumer.Close()
		for _, producer := range producers {
			producer.AbortTransaction(nil)
			producer.Close()
		}
		fmt.Fprintf(os.Stderr, "\n%v\n", commitTimes)
		ts := stats.Int64Slice(commitTimes)
		sort.Sort(ts)
		fmt.Fprintf(os.Stderr, "p50: %d, p99: %d\n",
			stats.P(ts, 0.5), stats.P(ts, 0.99))
	}
	http.HandleFunc("/consume", handleConsume)
	_ = http.ListenAndServe(":8090", nil)
}
