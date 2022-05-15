package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strconv"
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
	FLAGS_srcInstance   int
	FLAGS_port          int
	FLAGS_flushms       int
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
	flag.IntVar(&FLAGS_srcInstance, "srcIns", 1, "number of source instance")
	flag.IntVar(&FLAGS_port, "port", 8080, "port to listen")
	flag.IntVar(&FLAGS_flushms, "flushms", 100, "flush inverval in ms")
	flag.Parse()

	fmt.Fprintf(os.Stderr, "duration: %d, events_nm: %d, serde: %s, nPar: %d, sourceInstances: %d, tps: %d, port: %d, flushMs: %d\n",
		FLAGS_duration, FLAGS_events_num, FLAGS_serdeFormat, FLAGS_numPartition,
		FLAGS_srcInstance, FLAGS_tps, FLAGS_port, FLAGS_flushms)

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

	iid_str := os.Getenv("IID")
	instanceId := 0
	var err error
	if iid_str != "" {
		instanceId, err = strconv.Atoi(iid_str)
		if err != nil {
			panic(err)
		}
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
	err = common.CreateTopic(ctx, newTopic, FLAGS_broker)
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
	generatorConfig := generator.NewGeneratorConfig(nexmarkConfig, time.Now().UnixMilli(), 1, uint64(nexmarkConfig.NumEvents), 1)
	eventGenerator := generator.NewSimpleNexmarkGenerator(generatorConfig, instanceId)
	channel_url_cache := make(map[uint32]*generator.ChannelUrl)

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":                     FLAGS_broker,
		"go.produce.channel.size":               100000,
		"go.events.channel.size":                100000,
		"acks":                                  "all",
		"batch.size":                            131072,
		"linger.ms":                             FLAGS_flushms,
		"max.in.flight.requests.per.connection": 5,
		// "statistics.interval.ms":                5000,
	})
	if err != nil {
		log.Fatal().Msgf("Failed to create producer: %s\n", err)
	}
	defer p.Close()
	duration := time.Duration(FLAGS_duration) * time.Second
	events_num := int32(FLAGS_events_num)
	num_par := int32(FLAGS_numPartition)
	parNum := instanceId % int(num_par)

	handler := func(w http.ResponseWriter, req *http.Request) {
		idx := int32(0)
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
		for {
			elapsed := time.Since(start)
			// fmt.Fprintf(os.Stderr, "elapsed: %v, idx: %v\n", elapsed, idx)
			if (duration != 0 && elapsed >= duration) ||
				(events_num != 0 && idx >= events_num) {
				break
			}
			nextEvent, err := eventGenerator.NextEvent(ctx, channel_url_cache)
			if err != nil {
				log.Fatal().Msgf("next event failed: %s", err)
			}
			encoded, err := valueEncoder.Encode(nextEvent.Event)
			if err != nil {
				log.Fatal().Msgf("event serialization failed: %s", err)
			}
			nowMs := time.Now().UnixMilli()
			if nextEvent.WallclockTimestamp > nowMs {
				time.Sleep(time.Duration(nextEvent.WallclockTimestamp-nowMs) * time.Millisecond)
			}

			idx += 1
			p.ProduceChannel() <- &kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: int32(parNum)},
				Value:          encoded,
			}
			if err != nil {
				log.Fatal().Err(err)
			}
		}
		remaining := p.Flush(30 * 1000)
		for remaining != 0 {
			remaining = p.Flush(30 * 1000)
		}
		ret := atomic.LoadInt32(&replies)
		fmt.Fprintf(os.Stderr, "%d event acked\n", ret)
		totalTime := time.Since(start).Seconds()
		fmt.Fprintf(os.Stderr, "source produced %d events, time %v, throughput %v\n",
			idx, totalTime, float64(idx)/totalTime)
		for _, s := range stats_arr {
			fmt.Fprintf(os.Stderr, s+"\n")
		}
		fmt.Fprintf(w, "done kproduce")
	}
	http.HandleFunc("/kproduce", handler)
	fmt.Fprintf(os.Stderr, "kproduce listening %d\n", FLAGS_port)
	_ = http.ListenAndServe(fmt.Sprintf(":%d", FLAGS_port), nil)
}
