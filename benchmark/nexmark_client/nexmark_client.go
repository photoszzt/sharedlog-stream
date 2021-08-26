package main

import (
	"flag"
	"fmt"
	"net/http"
	"sync"
	"time"

	ntypes "cs.utexas.edu/zhitingz/sharedlog-stream/pkg/nexmark/types"
	"cs.utexas.edu/zhitingz/sharedlog-stream/pkg/nexmark/utils"
	"github.com/rs/zerolog/log"
)

var (
	FLAGS_faas_gateway  string
	FLAGS_fn_name       string
	FLAGS_stream_prefix string
	FLAGS_duration      int
)

func invokeSourceFunc(client *http.Client, response *ntypes.FnOutput, wg *sync.WaitGroup) {
	defer wg.Done()
	nexmarkConfig := ntypes.NewNexMarkConfigInput(FLAGS_stream_prefix + "_src")
	nexmarkConfig.Duration = uint32(FLAGS_duration)
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
