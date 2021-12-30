package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/pkg/stream/processor"
	"sync"
	"time"
)

func query1() {
	serdeFormat := getSerdeFormat()

	jsonFile, err := os.Open(FLAGS_workload_config)
	if err != nil {
		panic(err)
	}
	// defer the closing of our jsonFile so that we can parse it later on
	defer jsonFile.Close()

	byteVal, err := ioutil.ReadAll(jsonFile)
	if err != nil {
		panic(err)
	}
	var q1conf Q1WorkloadConfig
	err = json.Unmarshal(byteVal, &q1conf)
	if err != nil {
		panic(err)
	}

	q1NodeConfig := &processor.ClientNodeConfig{
		FuncName:    "q1",
		GatewayUrl:  FLAGS_faas_gateway,
		NumInstance: q1conf.NumSrcPartition,
	}
	q1InputParams := make([]*common.QueryInput, q1NodeConfig.NumInstance)
	for i := 0; i < int(q1NodeConfig.NumInstance); i++ {
		q1InputParams[i] = &common.QueryInput{
			Duration:        uint32(FLAGS_duration),
			InputTopicName:  q1conf.SrcOutTopic,
			OutputTopicName: q1conf.SinkOutTopic,
			SerdeFormat:     uint8(serdeFormat),
			NumInPartition:  q1conf.NumSrcPartition,
			NumOutPartition: q1conf.NumSrcPartition,
		}
	}
	q1Node := processor.NewClientNode(q1NodeConfig)

	client := &http.Client{
		Transport: &http.Transport{
			IdleConnTimeout: 30 * time.Second,
		},
		Timeout: time.Duration(FLAGS_duration*2) * time.Second,
	}

	var wg sync.WaitGroup

	sourceOutput := make([]common.FnOutput, q1conf.NumSrcPartition)
	q1Output := make([]common.FnOutput, q1conf.NumSrcPartition)

	for i := 0; i < int(q1conf.NumSrcPartition); i++ {
		wg.Add(1)
		go invokeSourceFunc(client, uint8(i), &sourceOutput[i], &wg)
	}

	for i := 0; i < int(q1conf.NumSrcPartition); i++ {
		wg.Add(1)
		q1InputParams[i].ParNum = uint8(i)
		go q1Node.Invoke(client, &q1Output[i], &wg, q1InputParams[i])
	}
	wg.Wait()

	for i := 0; i < int(q1conf.NumSrcPartition); i++ {
		if sourceOutput[i].Success {
			common.ProcessThroughputLat(fmt.Sprintf("source-%d", i),
				sourceOutput[i].Latencies, sourceOutput[i].Duration)
		}
	}

	for i := 0; i < int(q1conf.NumSrcPartition); i++ {
		if q1Output[i].Success {
			common.ProcessThroughputLat(fmt.Sprintf("q1-%d", i), q1Output[i].Latencies, q1Output[i].Duration)
		}
	}
}
