package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sharedlog-stream/benchmark/common"
	"sync/atomic"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

var (
	FLAGS_broker       string
	FLAGS_topicName    string
	FLAGS_numPartition int
	FLAGS_events_num   int
	FLAGS_duration     int
	FLAGS_payloadFile  string
	FLAGS_statsFile    string
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
	flag.IntVar(&FLAGS_events_num, "events_num", 100000000, "events.num param for nexmark")
	flag.IntVar(&FLAGS_duration, "duration", 60, "")
	flag.StringVar(&FLAGS_broker, "broker", "127.0.0.1", "")
	flag.StringVar(&FLAGS_topicName, "topicName", "src", "topic name")
	flag.IntVar(&FLAGS_numPartition, "npar", 1, "number of partition")
	flag.StringVar(&FLAGS_payloadFile, "payload", "", "payload file name")
	flag.StringVar(&FLAGS_statsFile, "statsFile", "", "path to store stats")
	flag.Parse()

	fmt.Fprintf(os.Stderr, "duration: %d, events_num: %d, broker: %s, topicName: %s, nPar: %d, payload: %s, statsFile: %s\n",
		FLAGS_duration, FLAGS_events_num, FLAGS_broker, FLAGS_topicName, FLAGS_numPartition,
		FLAGS_payloadFile, FLAGS_statsFile)
	if FLAGS_payloadFile == "" {
		fmt.Fprintf(os.Stderr, "payload filename cannot be empty\n")
		return
	}
	if FLAGS_statsFile == "" {
		fmt.Fprintf(os.Stderr, "stats filename cannot be empty\n")
		return
	}
	content, err := os.ReadFile(FLAGS_payloadFile)
	if err != nil {
		panic(fmt.Sprintf("fail to read file: %v", err))
	}
	newTopic := []kafka.TopicSpecification{
		{
			Topic:             FLAGS_topicName,
			NumPartitions:     FLAGS_numPartition,
			ReplicationFactor: 3,
			Config: map[string]string{
				"min.insync.replicas": "3",
			},
		},
	}
	ctx := context.Background()
	err = common.CreateTopic(ctx, newTopic, FLAGS_broker)
	if err != nil {
		panic(fmt.Sprintf("Failed to create topic: %s", err))
	}
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":                     FLAGS_broker,
		"go.produce.channel.size":               100000,
		"go.events.channel.size":                100000,
		"acks":                                  "all",
		"batch.size":                            16384,
		"linger.ms":                             0,
		"max.in.flight.requests.per.connection": 5,
		"statistics.interval.ms":                5000,
	})
	if err != nil {
		panic(fmt.Sprintf("Failed to create producer: %s\n", err))
	}
	defer p.Close()
	idx := int32(0)
	duration := time.Duration(FLAGS_duration) * time.Second
	replies := int32(0)
	stats_arr := make([]string, 0, 128)
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Error().Msgf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					log.Debug().Msgf("Delivered message to %v, ts %v\n", ev.TopicPartition, ev.Timestamp)
				}
			case *kafka.Stats:
				stats_arr = append(stats_arr, ev.String())
			default:
			}
			atomic.AddInt32(&replies, 1)
		}
	}()
	start := time.Now()
	events_num := int32(FLAGS_events_num)
	num_par := int32(FLAGS_numPartition)
	for {
		if (duration != 0 && time.Since(start) >= duration) ||
			(FLAGS_events_num != 0 && idx >= events_num) {
			break
		}
		idx += 1
		parNum := idx % num_par
		p.ProduceChannel() <- &kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &FLAGS_topicName, Partition: int32(parNum)},
			Value:          content,
		}
		if err != nil {
			panic(err)
		}
	}
	remaining := p.Flush(30 * 1000)
	for remaining != 0 {
		remaining = p.Flush(30 * 1000)
	}
	ret := atomic.LoadInt32(&replies)
	fmt.Fprintf(os.Stderr, "%d event acked\n", ret)
	totalTime := time.Since(start).Seconds()
	fmt.Fprintf(os.Stderr, "produce %d events, time: %v, throughput: %v\n",
		idx, totalTime, float64(idx)/totalTime)
	file, err := os.Create(FLAGS_statsFile)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	for _, s := range stats_arr {
		file.WriteString(s + "\n")
	}
}
