package main

import (
	"flag"
	"net/http"
	"sharedlog-stream/benchmark/common"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

var (
	FLAGS_faas_gateway string
	FLAGS_duration     int
	FLAGS_tps          int
	FLAGS_payload      string
	FLAGS_flushms      int
	FLAGS_local        bool
	FLAGS_events_num   int
	FLAGS_npar         int
	FLAGS_nprod        int
	FLAGS_serdeFormat  string
)

func main() {
	flag.StringVar(&FLAGS_faas_gateway, "faas_gateway", "127.0.0.1:8081", "")
	flag.IntVar(&FLAGS_duration, "duration", 60, "")
	flag.IntVar(&FLAGS_events_num, "events_num", 100000000, "events.num param for nexmark")
	flag.StringVar(&FLAGS_payload, "payload", "", "payload path")
	flag.IntVar(&FLAGS_npar, "npar", 1, "number of partition")
	flag.IntVar(&FLAGS_nprod, "nprod", 1, "number of producer")
	flag.StringVar(&FLAGS_serdeFormat, "serde", "json", "serde format: json or msgp")

	serdeFormat := common.StringToSerdeFormat(FLAGS_serdeFormat)
	spProd := &common.BenchSourceParam{
		TopicName:       "src",
		Duration:        uint32(FLAGS_duration),
		SerdeFormat:     uint8(serdeFormat),
		NumEvents:       uint32(FLAGS_events_num),
		FileName:        FLAGS_payload,
		NumOutPartition: uint8(FLAGS_npar),
		Tps:             uint32(FLAGS_tps),
		FlushMs:         uint32(FLAGS_flushms),
	}
	spTran := &common.TranProcessBenchParam{
		InTopicName:   "src",
		OutTopicName:  "out",
		SerdeFormat:   uint8(serdeFormat),
		NumPartition:  uint8(FLAGS_npar),
		Duration:      uint32(FLAGS_duration),
		CommitEveryMs: uint64(FLAGS_flushms),
		FlushMs:       uint32(FLAGS_flushms),
	}
	client := &http.Client{
		Transport: &http.Transport{
			IdleConnTimeout: 30 * time.Second,
		},
		Timeout: time.Duration(FLAGS_duration*2) * time.Second,
	}
	var wg sync.WaitGroup
	invoke := func(name string, response *common.FnOutput, sp interface{}, nodeConstraint string) {
		defer wg.Done()
		url := common.BuildFunctionUrl(FLAGS_faas_gateway, name)
		if err := common.JsonPostRequest(client, url, nodeConstraint, sp, response); err != nil {
			log.Error().Msgf("%s request failed: %v", name, err)
		} else if !response.Success {
			log.Error().Msgf("%s request failed: %s", name, response.Message)
		}
	}
	prodCons := "1"
	if FLAGS_local {
		prodCons = ""
	}
	consumeCons := "2"
	if FLAGS_local {
		consumeCons = ""
	}
	var prodResponse common.FnOutput
	var consumeResponse common.FnOutput
	wg.Add(1)
	go invoke("tranDataGen", &prodResponse, spProd, prodCons)
	wg.Add(1)
	go invoke("tranProcess", &consumeResponse, spTran, consumeCons)
	wg.Wait()
}
