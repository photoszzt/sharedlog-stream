package main

import (
	"flag"
	"fmt"
	"net/http"
	"sync"
	"time"

	"sharedlog-stream/benchmark/common"
	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"

	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"

	"github.com/rs/zerolog/log"
)

var (
	FLAGS_faas_gateway  string
	FLAGS_fn_name       string
	FLAGS_stream_prefix string
	FLAGS_duration      int
	FLAGS_events_num    int
	FLAGS_tps           int
	FLAGS_serdeFormat   string
)

func invokeSourceFunc(client *http.Client, response *ntypes.FnOutput, wg *sync.WaitGroup) {
	defer wg.Done()
	var serdeFormat common.SerdeFormat
	if FLAGS_serdeFormat == "json" {
		serdeFormat = common.JSON
	} else if FLAGS_serdeFormat == "msgp" {
		serdeFormat = common.MSGP
	} else {
		log.Error().Msgf("serde format is not recognized; default back to JSON")
		serdeFormat = common.JSON
	}
	nexmarkConfig := ntypes.NewNexMarkConfigInput(FLAGS_stream_prefix+"_src", serdeFormat)
	nexmarkConfig.Duration = uint32(FLAGS_duration)
	nexmarkConfig.FirstEventRate = uint32(FLAGS_tps)
	nexmarkConfig.NextEventRate = uint32(FLAGS_tps)
	nexmarkConfig.EventsNum = uint64(FLAGS_events_num)
	url := utils.BuildFunctionUrl(FLAGS_faas_gateway, "source")
	fmt.Printf("func source url is %v\n", url)
	if err := utils.JsonPostRequest(client, url, nexmarkConfig, response); err != nil {
		log.Error().Msgf("source request failed: %v", err)
	} else if !response.Success {
		log.Error().Msgf("source request failed: %s", response.Message)
	}
}

func invokeQuery(client *http.Client, response *ntypes.FnOutput, wg *sync.WaitGroup) {
	defer wg.Done()
	queryInput := &ntypes.QueryInput{
		Duration:        uint32(FLAGS_duration),
		InputTopicName:  FLAGS_stream_prefix + "_src",
		OutputTopicName: FLAGS_stream_prefix + "_" + FLAGS_fn_name + "_output",
	}
	url := utils.BuildFunctionUrl(FLAGS_faas_gateway, FLAGS_fn_name)
	fmt.Printf("func url is %v\n", url)
	if err := utils.JsonPostRequest(client, url, queryInput, response); err != nil {
		log.Error().Msgf("%v request failed: %v", FLAGS_fn_name, err)
	} else if !response.Success {
		log.Error().Msgf("%v request failed: %v", FLAGS_fn_name, err)
	}
}

func main() {
	flag.StringVar(&FLAGS_faas_gateway, "faas_gateway", "127.0.0.1:8081", "")
	flag.StringVar(&FLAGS_fn_name, "fn_name", "query1", "")
	flag.StringVar(&FLAGS_stream_prefix, "stream_prefix", "nexmark", "")
	flag.IntVar(&FLAGS_duration, "duration", 60, "")
	flag.IntVar(&FLAGS_events_num, "events_num", 100000000, "events.num param for nexmark")
	flag.IntVar(&FLAGS_tps, "tps", 10000000, "tps param for nexmark")
	flag.StringVar(&FLAGS_serdeFormat, "serde", "json", "serde format: json or msgp")
	flag.Parse()

	client := &http.Client{
		Transport: &http.Transport{
			IdleConnTimeout: 30 * time.Second,
		},
		Timeout: time.Duration(FLAGS_duration*2) * time.Second,
	}
	var wg sync.WaitGroup
	var sourceOutput, queryOutput ntypes.FnOutput
	wg.Add(1)
	go invokeSourceFunc(client, &sourceOutput, &wg)
	wg.Add(1)
	go invokeQuery(client, &queryOutput, &wg)
	wg.Wait()
}
