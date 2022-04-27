package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

var (
	FLAGS_faas_gateway string
	FLAGS_duration     int
	FLAGS_events_num   int
	FLAGS_serdeFormat  string
	FLAGS_payload      string
	FLAGS_npar         int
	FLAGS_nprod        int
)

type prodConsumeLatencies struct {
	ProdLatencies        []int
	ProdConsumeLatencies []int
}

func main() {
	flag.StringVar(&FLAGS_faas_gateway, "faas_gateway", "127.0.0.1:8081", "")
	flag.IntVar(&FLAGS_duration, "duration", 60, "")
	flag.IntVar(&FLAGS_events_num, "events_num", 100000000, "events.num param for nexmark")
	flag.StringVar(&FLAGS_payload, "payload", "", "payload path")
	flag.IntVar(&FLAGS_npar, "npar", 1, "number of partition")
	flag.IntVar(&FLAGS_nprod, "nprod", 1, "number of producer")
	flag.StringVar(&FLAGS_serdeFormat, "serde", "json", "serde format: json or msgp")
	flag.Parse()

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
	var prodResponse common.FnOutput
	var consumeResponse common.FnOutput
	var wg sync.WaitGroup
	invoke := func(name string, response *common.FnOutput) {
		defer wg.Done()
		url := utils.BuildFunctionUrl(FLAGS_faas_gateway, name)
		if err := utils.JsonPostRequest(client, url, sp, response); err != nil {
			log.Error().Msgf("%s request failed: %v", name, err)
		} else if !response.Success {
			log.Error().Msgf("%s request failed: %s", name, response.Message)
		}
	}
	wg.Add(1)
	go invoke("produce", &prodResponse)
	wg.Add(1)
	go invoke("consume", &consumeResponse)
	wg.Wait()
	lat := prodConsumeLatencies{
		ProdLatencies:        prodResponse.Latencies["e2e"],
		ProdConsumeLatencies: consumeResponse.Latencies["e2e"],
	}
	stats, err := json.Marshal(&lat)
	if err != nil {
		panic(err)
	}
	fmt.Fprintf(os.Stderr, "%s\n", stats)
}
