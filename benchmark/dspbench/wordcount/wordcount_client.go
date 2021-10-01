package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/stream/processor"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

var (
	FLAGS_faas_gateway string
	FLAGS_duration     int
	FLAGS_events_num   int
	FLAGS_serdeFormat  string
	FLAGS_file_name    string
)

func invokeSourceFunc(client *http.Client, response *common.FnOutput, wg *sync.WaitGroup, serdeFormat processor.SerdeFormat) {
	defer wg.Done()

	sp := &common.SourceParam{
		TopicName:   "wc_src",
		Duration:    uint32(FLAGS_duration),
		SerdeFormat: uint8(serdeFormat),
		NumEvents:   uint32(FLAGS_events_num),
		FileName:    FLAGS_file_name,
	}
	url := utils.BuildFunctionUrl(FLAGS_faas_gateway, "wcsource")
	if err := utils.JsonPostRequest(client, url, sp, response); err != nil {
		log.Error().Msgf("wcsource request failed: %v", err)
	} else if !response.Success {
		log.Error().Msgf("wcsource request failed: %s", response.Message)
	}
}

func main() {
	flag.StringVar(&FLAGS_faas_gateway, "faas_gateway", "127.0.0.1:8081", "")
	flag.IntVar(&FLAGS_duration, "duration", 60, "")
	flag.IntVar(&FLAGS_events_num, "nevent", 0, "number of events")
	flag.StringVar(&FLAGS_serdeFormat, "serde", "json", "serde format: json or msgp")
	flag.StringVar(&FLAGS_file_name, "in_fname", "/mnt/data/books.dat", "data source file name")
	flag.Parse()

	var serdeFormat processor.SerdeFormat
	if FLAGS_serdeFormat == "json" {
		serdeFormat = processor.JSON
	} else if FLAGS_serdeFormat == "msgp" {
		serdeFormat = processor.MSGP
	} else {
		log.Error().Msgf("serde format is not recognized; default back to JSON")
		serdeFormat = processor.JSON
	}

	numCountInstance := 5
	splitNodeConfig := &processor.ClientNodeConfig{
		FuncName:    "wordcountsplit",
		GatewayUrl:  FLAGS_faas_gateway,
		NumInstance: 1,
	}
	countNodeConfig := &processor.ClientNodeConfig{
		FuncName:    "wordcountcounter",
		GatewayUrl:  FLAGS_faas_gateway,
		NumInstance: uint32(numCountInstance),
	}
	splitInputParams := make([]*common.QueryInput, splitNodeConfig.NumInstance)
	for i := 0; i < int(splitNodeConfig.NumInstance); i++ {
		splitInputParams[i] = &common.QueryInput{
			Duration:        uint32(FLAGS_duration),
			InputTopicName:  "wc_src",
			OutputTopicName: "split_out",
			SerdeFormat:     uint8(serdeFormat),
			NumInPartition:  1,
			NumOutPartition: uint16(numCountInstance),
		}
		fmt.Fprintf(os.Stderr, "splitInputParams of %d is %v\n", i, splitInputParams[i])
	}
	fmt.Fprintf(os.Stderr, "splitInputParams: %v, len: %v\n", splitInputParams, len(splitInputParams))
	split := processor.NewClientNode(splitNodeConfig)

	countInputParams := make([]*common.QueryInput, countNodeConfig.NumInstance)
	for i := 0; i < int(countNodeConfig.NumInstance); i++ {
		countInputParams[i] = &common.QueryInput{
			Duration:        uint32(FLAGS_duration),
			InputTopicName:  "split_out",
			OutputTopicName: "wc_out",
			SerdeFormat:     uint8(serdeFormat),
			NumInPartition:  uint16(numCountInstance),
			NumOutPartition: uint16(numCountInstance),
		}
	}
	count := processor.NewClientNode(countNodeConfig)

	client := &http.Client{
		Transport: &http.Transport{
			IdleConnTimeout: 30 * time.Second,
		},
		Timeout: time.Duration(FLAGS_duration*3) * time.Second,
	}
	var wg sync.WaitGroup
	var sourceOutput common.FnOutput

	splitOutput := make([]common.FnOutput, splitNodeConfig.NumInstance)
	countOutput := make([]common.FnOutput, countNodeConfig.NumInstance)

	wg.Add(1)
	go invokeSourceFunc(client, &sourceOutput, &wg, serdeFormat)

	fmt.Fprintf(os.Stderr, "splitInputParams: %v\n", splitInputParams)
	for i := 0; i < int(splitNodeConfig.NumInstance); i++ {
		wg.Add(1)
		splitInputParams[i].ParNum = 0
		go split.Invoke(client, &splitOutput[i], &wg, splitInputParams[i])
	}

	for i := 0; i < int(countNodeConfig.NumInstance); i++ {
		wg.Add(1)
		countInputParams[i].ParNum = uint16(i)
		go count.Invoke(client, &countOutput[i], &wg, countInputParams[i])
	}

	wg.Wait()
}