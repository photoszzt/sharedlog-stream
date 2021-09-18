package main

import (
	"flag"
	"fmt"
	"net/http"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

var (
	FLAGS_faas_gateway  string
	FLAGS_fn_name       string
	FLAGS_stream_prefix string
	FLAGS_file_name     string
	FLAGS_duration      int
	FLAGS_events_num    int
	FLAGS_serdeFormat   string
)

func invokeSourceFunc(client *http.Client, response *common.FnOutput, wg *sync.WaitGroup) {
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
	sp := &common.SourceParam{
		TopicName:   FLAGS_stream_prefix + "_src",
		FileName:    FLAGS_file_name,
		Duration:    uint32(FLAGS_duration),
		SerdeFormat: uint8(serdeFormat),
	}
	fmt.Printf("input file name is %v\n", FLAGS_file_name)
	url := utils.BuildFunctionUrl(FLAGS_faas_gateway, "sdsource")
	if err := utils.JsonPostRequest(client, url, sp, response); err != nil {
		log.Error().Msgf("sdsource request failed: %v", err)
	} else if !response.Success {
		log.Error().Msgf("sdsource request failed: %s", response.Message)
	}
}

func invokeQuery(client *http.Client, response *common.FnOutput, wg *sync.WaitGroup) {
	defer wg.Done()
	queryInput := &common.QueryInput{
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
	flag.StringVar(&FLAGS_fn_name, "fn_name", "spike_detection_handler", "")
	flag.StringVar(&FLAGS_stream_prefix, "stream_prefix", "dspbench", "")
	flag.StringVar(&FLAGS_file_name, "in_fname", "/mnt/data/data.txt", "data source file name")
	flag.IntVar(&FLAGS_duration, "duration", 60, "")
	flag.StringVar(&FLAGS_serdeFormat, "serde", "json", "serde format: json or msgp")
	flag.Parse()

	client := &http.Client{
		Transport: &http.Transport{
			IdleConnTimeout: 30 * time.Second,
		},
		Timeout: time.Duration(FLAGS_duration*2) * time.Second,
	}
	var wg sync.WaitGroup
	var sourceOutput, queryOutput common.FnOutput
	wg.Add(1)
	go invokeSourceFunc(client, &sourceOutput, &wg)
	wg.Add(1)
	go invokeQuery(client, &queryOutput, &wg)
	wg.Wait()
}
