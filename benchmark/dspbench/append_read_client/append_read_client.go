package main

import (
	"flag"
	"net/http"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

var (
	FLAGS_faas_gateway string
	FLAGS_duration     int
)

func invokeFunc(client *http.Client, response *common.FnOutput, wg *sync.WaitGroup, funcName string) {
	defer wg.Done()

	inp := &common.QueryInput{}
	url := utils.BuildFunctionUrl(FLAGS_faas_gateway, funcName)
	if err := utils.JsonPostRequest(client, url, inp, response); err != nil {
		log.Error().Msgf("%s request failed: %v", funcName, err)
	} else if !response.Success {
		log.Error().Msgf("%s request failed: %v", funcName, response.Message)
	}
}

func main() {
	flag.StringVar(&FLAGS_faas_gateway, "faas_gateway", "127.0.0.1:8081", "")
	flag.IntVar(&FLAGS_duration, "duration", 60, "")
	flag.Parse()

	client := &http.Client{
		Transport: &http.Transport{
			IdleConnTimeout: 30 * time.Second,
		},
		Timeout: time.Duration(FLAGS_duration*3) * time.Second,
	}
	var wg sync.WaitGroup

	var appendRet common.FnOutput
	var readRet common.FnOutput
	wg.Add(1)
	go invokeFunc(client, &appendRet, &wg, "streamAppend")
	time.Sleep(5 * time.Second)

	wg.Add(1)
	go invokeFunc(client, &readRet, &wg, "streamRead")
	wg.Wait()
}
