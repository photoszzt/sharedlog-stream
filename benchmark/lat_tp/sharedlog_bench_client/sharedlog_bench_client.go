package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"path"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"time"

	"github.com/rs/zerolog/log"
)

var (
	FLAGS_faas_gateway string
	FLAGS_duration     int
	FLAGS_events_num   int
	FLAGS_stat_dir     string
	FLAGS_serdeFormat  string
	FLAGS_payload      string
	FLAGS_npar         int
	FLAGS_nprod        int
)

func main() {
	flag.StringVar(&FLAGS_faas_gateway, "faas_gateway", "127.0.0.1:8081", "")
	flag.IntVar(&FLAGS_duration, "duration", 60, "")
	flag.IntVar(&FLAGS_events_num, "events_num", 100000000, "events.num param for nexmark")
	flag.StringVar(&FLAGS_stat_dir, "stat_dir", "", "stats dir to dump")
	flag.StringVar(&FLAGS_payload, "payload", "", "payload path")
	flag.IntVar(&FLAGS_npar, "npar", 1, "number of partition")
	flag.IntVar(&FLAGS_nprod, "nprod", 1, "number of producer")
	flag.StringVar(&FLAGS_serdeFormat, "serde", "json", "serde format: json or msgp")
	flag.Parse()

	if FLAGS_stat_dir == "" {
		panic("should specify non empty stats dir")
	}
	serdeFormat := common.StringToSerdeFormat(FLAGS_serdeFormat)
	sp := &common.SourceParam{
		TopicName:       "src",
		Duration:        uint32(FLAGS_duration),
		SerdeFormat:     uint8(serdeFormat),
		NumEvents:       uint32(FLAGS_events_num),
		FileName:        FLAGS_payload,
		NumOutPartition: uint8(FLAGS_npar),
	}
	client := &http.Client{
		Transport: &http.Transport{
			IdleConnTimeout: 30 * time.Second,
		},
		Timeout: time.Duration(FLAGS_duration*2) * time.Second,
	}
	var response common.FnOutput

	url := utils.BuildFunctionUrl(FLAGS_faas_gateway, "slproduce")
	if err := utils.JsonPostRequest(client, url, sp, response); err != nil {
		log.Error().Msgf("slproduce request failed: %v", err)
	} else if !response.Success {
		log.Error().Msgf("slproduce request failed: %s", response.Message)
	}
	path := path.Join(FLAGS_stat_dir, "stats.json")
	if err := os.MkdirAll(FLAGS_stat_dir, 0750); err != nil {
		panic(fmt.Sprintf("Fail to create stat dir: %v", err))
	}
	stats, err := json.Marshal(response.Latencies)
	if err != nil {
		panic(err)
	}
	err = os.WriteFile(path, stats, 0660)
	if err != nil {
		panic(err)
	}
}
