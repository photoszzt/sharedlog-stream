package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	datatype "sharedlog-stream/benchmark/lat_tp/pkg/data_type"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/rs/zerolog"
)

var (
	FLAGS_broker     string
	FLAGS_topicName  string
	FLAGS_duration   int
	FLAGS_events_num int
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
	flag.StringVar(&FLAGS_topicName, "topicName", "src", "topic name")
	flag.IntVar(&FLAGS_duration, "duration", 60, "")
	flag.Parse()

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": FLAGS_broker,
		"group.id":          "bench",
		"auto.offset.reset": "earliest"})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	err = c.SubscribeTopics([]string{FLAGS_topicName}, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fail to subscribe to topic: %s\n", err)
		os.Exit(1)
	}
	var ptSerde datatype.PayloadTsMsgpSerde
	prod_to_con_lat := make([]int64, 0, 4096)
	duration := time.Duration(FLAGS_duration) * time.Second
	start := time.Now()
	idx := 0
	for {
		if (duration != 0 && time.Since(start) > duration) ||
			(FLAGS_events_num != 0 && idx >= FLAGS_events_num) {
			break
		}
		ev := c.Poll(100)
		if ev == nil {
			continue
		}

		switch e := ev.(type) {
		case *kafka.Message:
			ptTmp, err := ptSerde.Decode(e.Value)
			if err != nil {
				panic(err)
			}
			pt := ptTmp.(datatype.PayloadTs)
			nowTs := time.Now().UnixMicro()
			lat := nowTs - pt.Ts
			prod_to_con_lat = append(prod_to_con_lat, lat)
			idx += 1
		}
	}
	fmt.Fprintf(os.Stderr, "%v\n", prod_to_con_lat)
}
