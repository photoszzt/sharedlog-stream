package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/generator"

	"sharedlog-stream/pkg/stream/processor/commtypes"

	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

var (
	FLAGS_events_num    int
	FLAGS_duration      int
	FLAGS_broker        string
	FLAGS_stream_prefix string
	FLAGS_serdeFormat   string
	FLAGS_numPartition  int
	FLAGS_tps           int
	FLAGS_instanceId    int
	FLAGS_srcInstance   int
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
	flag.StringVar(&FLAGS_stream_prefix, "stream_prefix", "nexmark", "")
	flag.StringVar(&FLAGS_serdeFormat, "serde", "json", "serde format: json or msgp")
	flag.IntVar(&FLAGS_numPartition, "npar", 1, "number of partition")
	flag.IntVar(&FLAGS_tps, "tps", 10000000, "tps param for nexmark")
	flag.IntVar(&FLAGS_instanceId, "iid", 1, "instance id")
	flag.IntVar(&FLAGS_srcInstance, "srcIns", 1, "number of source instance")
	flag.Parse()

	fmt.Fprintf(os.Stderr, "duration: %d, events_num: %d, serde: %s, nPar: %d\n",
		FLAGS_duration, FLAGS_events_num, FLAGS_serdeFormat, FLAGS_numPartition)

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
		{
			Topic:             topic,
			NumPartitions:     FLAGS_numPartition,
			ReplicationFactor: 3,
			Config: map[string]string{
				"min.insync.replicas": "3",
			},
		},
	}
	ctx := context.Background()
	err := common.CreateTopic(ctx, newTopic, FLAGS_broker)
	if err != nil {
		log.Fatal().Msgf("Failed to create topic: %s", err)
	}
	nexmarkConfigInput := ntypes.NewNexMarkConfigInput(topic, serdeFormat)
	nexmarkConfigInput.Duration = uint32(FLAGS_duration)
	nexmarkConfigInput.FirstEventRate = uint32(FLAGS_tps)
	nexmarkConfigInput.NextEventRate = uint32(FLAGS_tps)
	nexmarkConfigInput.EventsNum = uint64(FLAGS_events_num)
	nexmarkConfigInput.NumOutPartition = uint8(FLAGS_numPartition)
	nexmarkConfigInput.NumSrcInstance = uint8(FLAGS_srcInstance)
	nexmarkConfig, err := ntypes.ConvertToNexmarkConfiguration(nexmarkConfigInput)
	if err != nil {
		log.Fatal().Msgf("Failed to convert to nexmark configuration: %s", err)
	}
	generatorConfig := generator.NewGeneratorConfig(nexmarkConfig, uint64(time.Now().UnixMilli()), 1, uint64(nexmarkConfig.NumEvents), 1)
	eventGenerator := generator.NewSimpleNexmarkGenerator(generatorConfig, FLAGS_instanceId)
	channel_url_cache := make(map[uint32]*generator.ChannelUrl)

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":                     FLAGS_broker,
		"go.produce.channel.size":               100000,
		"go.events.channel.size":                100000,
		"acks":                                  "all",
		"max.in.flight.requests.per.connection": 5,
	})
	if err != nil {
		log.Fatal().Msgf("Failed to create producer: %s\n", err)
	}
	defer p.Close()

	idx := int32(0)
	duration := time.Duration(FLAGS_duration) * time.Second
	replies := int32(0)
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Error().Msgf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					log.Debug().Msgf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
			atomic.AddInt32(&replies, 1)
		}
	}()
	start := time.Now()
	events_num := int32(FLAGS_events_num)
	num_par := int32(FLAGS_numPartition)
	for {
		if (duration != 0 && time.Since(start) >= duration) ||
			(FLAGS_events_num != 0 && idx > events_num) {
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
		idx += 1
		parNum := idx % num_par
		p.ProduceChannel() <- &kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: int32(parNum)},
			Value:          encoded,
		}
		if err != nil {
			log.Fatal().Err(err)
		}
	}
	fmt.Fprintf(os.Stderr, "out of the produce loop; now looking at the delivery channel, produced: %d\n", idx)
	remaining := p.Flush(30 * 1000)
	fmt.Fprintf(os.Stderr, "producer: %d messages remaining in queue.", remaining)
	for remaining != 0 {
		remaining = p.Flush(30 * 1000)
	}
	ret := atomic.LoadInt32(&replies)
	fmt.Fprintf(os.Stderr, "%d event acked\n", ret)
	totalTime := time.Since(start).Seconds()
	fmt.Fprintf(os.Stderr, "source processed %d events, time %v, throughput %v\n", idx, totalTime, float64(idx)/totalTime)
}
