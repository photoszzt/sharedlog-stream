package main

import (
	"flag"
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
	url := utils.BuildFunctionUrl(FLAGS_faas_gateway, "source")
	if err := utils.JsonPostRequest(client, url, nexmarkConfig, response); err != nil {
		log.Error().Msgf("source request failed: %v", err)
	} else if !response.Success {
		log.Error().Msgf("source request failed: %s", response.Message)
	}
}

func invokeQuery(client *http.Client, response *ntypes.FnOutput, funcName string, wg *sync.WaitGroup) {
	defer wg.Done()
	queryInput := &ntypes.QueryInput{
		InputTopicName:  FLAGS_stream_prefix + "_src",
		OutputTopicName: FLAGS_stream_prefix + "_" + funcName + "_output",
	}
	url := utils.BuildFunctionUrl(FLAGS_faas_gateway, funcName)
	if err := utils.JsonPostRequest(client, url, queryInput, response); err != nil {
		log.Error().Msgf("%v request failed: %v", funcName, err)
	} else if !response.Success {
		log.Error().Msgf("%v request failed: %v", funcName, err)
	}
}

func main() {
	flag.StringVar(&FLAGS_faas_gateway, "faas_gateway", "127.0.0.1:8081", "")
	flag.StringVar(&FLAGS_fn_name, "fn_prefix", "source", "")
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
	go invokeQuery(client, &queryOutput, FLAGS_fn_name, &wg)
	wg.Wait()
}
