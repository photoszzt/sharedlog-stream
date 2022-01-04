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

func windowedAvg() {
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
	var wconf WindowedAvgConfig
	err = json.Unmarshal(byteVal, &wconf)
	if err != nil {
		panic(err)
	}

	groupByNodeConfig := &processor.ClientNodeConfig{
		FuncName:    "windowavggroupby",
		GatewayUrl:  FLAGS_faas_gateway,
		NumInstance: wconf.NumSrcPartition,
	}
	groupByInputParams := make([]*common.QueryInput, groupByNodeConfig.NumInstance)
	for i := 0; i < int(groupByNodeConfig.NumInstance); i++ {
		groupByInputParams[i] = &common.QueryInput{
			Duration:        uint32(FLAGS_duration),
			InputTopicName:  wconf.SrcOutTopic,
			OutputTopicName: wconf.GroupByOutTopic,
			SerdeFormat:     uint8(serdeFormat),
			NumInPartition:  wconf.NumSrcPartition,
			NumOutPartition: wconf.NumInstance,
		}
	}
	bidGroupBy := processor.NewClientNode(groupByNodeConfig)

	avgNodeConfig := &processor.ClientNodeConfig{
		FuncName:    "windowavgagg",
		GatewayUrl:  FLAGS_faas_gateway,
		NumInstance: wconf.NumInstance,
	}
	avgNodeInputParams := make([]*common.QueryInput, avgNodeConfig.NumInstance)
	for i := 0; i < int(avgNodeConfig.NumInstance); i++ {
		avgNodeInputParams[i] = &common.QueryInput{
			Duration:        uint32(FLAGS_duration),
			InputTopicName:  wconf.GroupByOutTopic,
			OutputTopicName: wconf.AvgOutTopic,
			SerdeFormat:     uint8(serdeFormat),
			NumInPartition:  wconf.NumInstance,
			NumOutPartition: wconf.NumInstance,
		}
	}
	avg := processor.NewClientNode(avgNodeConfig)

	client := &http.Client{
		Transport: &http.Transport{
			IdleConnTimeout: 30 * time.Second,
		},
		Timeout: time.Duration(FLAGS_duration*2) * time.Second,
	}

	var wg sync.WaitGroup
	sourceOutput := make([]common.FnOutput, wconf.NumSrcInstance)
	groupByOutput := make([]common.FnOutput, groupByNodeConfig.NumInstance)
	avgOutput := make([]common.FnOutput, avgNodeConfig.NumInstance)

	for i := 0; i < int(wconf.NumSrcInstance); i++ {
		wg.Add(1)
		idx := i
		go invokeSourceFunc(client, wconf.NumSrcPartition, &sourceOutput[idx], &wg)
	}

	for i := 0; i < int(groupByNodeConfig.NumInstance); i++ {
		wg.Add(1)
		idx := i
		groupByInputParams[i].ParNum = uint8(idx)
		go bidGroupBy.Invoke(client, &groupByOutput[i], &wg, groupByInputParams[i])
	}

	for i := 0; i < int(avgNodeConfig.NumInstance); i++ {
		wg.Add(1)
		idx := i
		avgNodeInputParams[i].ParNum = uint8(idx)
		go avg.Invoke(client, &avgOutput[i], &wg, avgNodeInputParams[i])
	}
	wg.Wait()

	for i := 0; i < int(wconf.NumSrcPartition); i++ {
		idx := i
		if sourceOutput[idx].Success {
			common.ProcessThroughputLat(fmt.Sprintf("source %d", idx), sourceOutput[idx].Latencies, sourceOutput[idx].Duration)
		}
	}
	for i := 0; i < int(groupByNodeConfig.NumInstance); i++ {
		idx := i
		if groupByOutput[idx].Success {
			common.ProcessThroughputLat(fmt.Sprintf("groupby %d", idx), groupByOutput[idx].Latencies, groupByOutput[idx].Duration)
		}
	}
	for i := 0; i < int(avgNodeConfig.NumInstance); i++ {
		idx := i
		if avgOutput[idx].Success {
			common.ProcessThroughputLat(fmt.Sprintf("avg %d", idx), avgOutput[idx].Latencies, avgOutput[idx].Duration)
		}
	}
}
