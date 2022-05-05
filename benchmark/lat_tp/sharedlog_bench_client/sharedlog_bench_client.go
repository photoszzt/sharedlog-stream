package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sort"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

var (
	FLAGS_faas_gateway  string
	FLAGS_duration      int
	FLAGS_tps           int
	FLAGS_warmup_time   int
	FLAGS_warmup_events int
	FLAGS_events_num    int
	FLAGS_serdeFormat   string
	FLAGS_payload       string
	FLAGS_npar          int
	FLAGS_nprod         int
	FLAGS_flushms       int
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
	flag.IntVar(&FLAGS_warmup_time, "warmup_time", 0, "warm up time in sec")
	flag.IntVar(&FLAGS_warmup_events, "warmup_events", 0, "number of events consumed for warmup")
	flag.IntVar(&FLAGS_tps, "tps", 1000, "events per second")
	flag.IntVar(&FLAGS_flushms, "flushms", 5, "flush every <n> ms")
	flag.Parse()

	serdeFormat := common.StringToSerdeFormat(FLAGS_serdeFormat)
	spProd := &common.BenchSourceParam{
		TopicName:       "src",
		Duration:        uint32(FLAGS_duration),
		SerdeFormat:     uint8(serdeFormat),
		NumEvents:       uint32(FLAGS_events_num),
		FileName:        FLAGS_payload,
		NumOutPartition: uint8(FLAGS_npar),
		WarmUpTime:      uint32(FLAGS_warmup_time),
		WarmUpEvents:    uint32(FLAGS_warmup_events),
		Tps:             uint32(FLAGS_tps),
		FlushMs:         uint32(FLAGS_flushms),
	}
	spConsume := &common.BenchSourceParam{
		TopicName:       "src",
		Duration:        uint32(FLAGS_duration) + 5,
		SerdeFormat:     uint8(serdeFormat),
		NumEvents:       uint32(FLAGS_events_num),
		FileName:        FLAGS_payload,
		NumOutPartition: uint8(FLAGS_npar),
		WarmUpTime:      uint32(FLAGS_warmup_time),
		WarmUpEvents:    uint32(FLAGS_warmup_events),
		Tps:             uint32(FLAGS_tps),
		FlushMs:         spProd.FlushMs,
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
	invoke := func(name string, response *common.FnOutput, sp *common.BenchSourceParam) {
		defer wg.Done()
		url := utils.BuildFunctionUrl(FLAGS_faas_gateway, name)
		if err := utils.JsonPostRequest(client, url, sp, response); err != nil {
			log.Error().Msgf("%s request failed: %v", name, err)
		} else if !response.Success {
			log.Error().Msgf("%s request failed: %s", name, response.Message)
		}
	}
	wg.Add(1)
	go invoke("produce", &prodResponse, spProd)
	wg.Add(1)
	go invoke("consume", &consumeResponse, spConsume)
	wg.Wait()
	if !prodResponse.Success {
		fmt.Fprintf(os.Stderr, "produce failed\n")
	} else if !consumeResponse.Success {
		fmt.Fprintf(os.Stderr, "consume failed\n")
	} else {
		lat := prodConsumeLatencies{
			ProdLatencies:        prodResponse.Latencies["e2e"],
			ProdConsumeLatencies: consumeResponse.Latencies["e2e"],
		}
		produced := len(lat.ProdLatencies)
		prodTime := prodResponse.Duration
		consumed := len(lat.ProdConsumeLatencies)
		consumeTime := consumeResponse.Duration
		stats, err := json.Marshal(&lat)
		if err != nil {
			panic(err)
		}
		fmt.Fprintf(os.Stderr, "%s\n", stats)
		ts := common.TimeSlice(lat.ProdConsumeLatencies)
		sort.Sort(ts)
		fmt.Fprintf(os.Stderr, "produced %d events in %f s, tp: %f\n",
			produced, prodTime, float64(produced)/prodTime)
		fmt.Fprintf(os.Stderr, "consumed %d events in %f s, tp: %f, p50: %d, p99: %d\n",
			consumed, consumeTime, float64(consumed)/consumeTime, ts.P(0.5), ts.P(0.99))
	}
}
