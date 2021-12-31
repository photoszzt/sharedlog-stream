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

func query7() {
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
	var q7conf Q7WorkloadConfig
	err = json.Unmarshal(byteVal, &q7conf)
	if err != nil {
		panic(err)
	}

	q7BidKeyedByPriceNodeConfig := &processor.ClientNodeConfig{
		FuncName:    "q7bidkeyedbyprice",
		GatewayUrl:  FLAGS_faas_gateway,
		NumInstance: q7conf.NumSrcPartition,
	}
	q7BidKeyedByPriceInputParams := make([]*common.QueryInput, q7BidKeyedByPriceNodeConfig.NumInstance)
	for i := 0; i < int(q7BidKeyedByPriceNodeConfig.NumInstance); i++ {
		q7BidKeyedByPriceInputParams[i] = &common.QueryInput{
			Duration:          uint32(FLAGS_duration),
			InputTopicName:    q7conf.SrcOutTopic,
			OutputTopicName:   q7conf.BidKeyedByPriceOutTopic,
			SerdeFormat:       uint8(serdeFormat),
			NumInPartition:    q7conf.NumSrcPartition,
			NumOutPartition:   q7conf.NumInstance,
			EnableTransaction: FLAGS_tran,
			CommitEveryMs:     FLAGS_commit_every,
		}
	}
	q7BidKeyedByPrice := processor.NewClientNode(q7BidKeyedByPriceNodeConfig)

	q7TransNodeConfig := &processor.ClientNodeConfig{
		FuncName:    "query7",
		GatewayUrl:  FLAGS_faas_gateway,
		NumInstance: q7conf.NumInstance,
	}
	q7TransInputParams := make([]*common.QueryInput, q7TransNodeConfig.NumInstance)
	for i := 0; i < int(q7TransNodeConfig.NumInstance); i++ {
		q7TransInputParams[i] = &common.QueryInput{
			Duration:          uint32(FLAGS_duration),
			InputTopicName:    q7conf.BidKeyedByPriceOutTopic,
			OutputTopicName:   q7conf.Q7TransOutTopic,
			SerdeFormat:       uint8(serdeFormat),
			NumInPartition:    q7conf.NumInstance,
			NumOutPartition:   q7conf.NumInstance,
			EnableTransaction: FLAGS_tran,
			CommitEveryMs:     FLAGS_commit_every,
		}
	}
	q7Trans := processor.NewClientNode(q7TransNodeConfig)

	client := &http.Client{
		Transport: &http.Transport{
			IdleConnTimeout: 30 * time.Second,
		},
		Timeout: time.Duration(FLAGS_duration*3) * time.Second,
	}

	var wg sync.WaitGroup
	sourceOutput := make([]common.FnOutput, q7conf.NumSrcPartition)
	q7BidKeyedByPriceOutput := make([]common.FnOutput, q7BidKeyedByPriceNodeConfig.NumInstance)
	q7TransOutput := make([]common.FnOutput, q7TransNodeConfig.NumInstance)

	for i := 0; i < int(q7conf.NumSrcPartition); i++ {
		wg.Add(1)
		idx := i
		go invokeSourceFunc(client, q7conf.NumSrcPartition, uint8(idx), &sourceOutput[idx], &wg)
	}

	for i := 0; i < int(q7BidKeyedByPriceNodeConfig.NumInstance); i++ {
		wg.Add(1)
		idx := i
		q7BidKeyedByPriceInputParams[idx].ParNum = uint8(idx)
		go q7BidKeyedByPrice.Invoke(client, &q7BidKeyedByPriceOutput[idx], &wg, q7BidKeyedByPriceInputParams[idx])
	}

	for i := 0; i < int(q7TransNodeConfig.NumInstance); i++ {
		wg.Add(1)
		idx := i
		q7TransInputParams[idx].ParNum = uint8(i)
		go q7Trans.Invoke(client, &q7TransOutput[idx], &wg, q7TransInputParams[idx])
	}
	wg.Wait()

	for i := 0; i < int(q7conf.NumSrcPartition); i++ {
		idx := i
		if sourceOutput[idx].Success {
			common.ProcessThroughputLat(fmt.Sprintf("source-%d", idx),
				sourceOutput[idx].Latencies, sourceOutput[idx].Duration)
		}
	}

	for i := 0; i < int(q7BidKeyedByPriceNodeConfig.NumInstance); i++ {
		idx := i
		if q7BidKeyedByPriceOutput[idx].Success {
			common.ProcessThroughputLat(fmt.Sprintf("q7-bid-keyed-by-price-%d", idx),
				q7BidKeyedByPriceOutput[idx].Latencies, q7BidKeyedByPriceOutput[idx].Duration)
		}
	}

	for i := 0; i < int(q7TransNodeConfig.NumInstance); i++ {
		idx := i
		if q7TransOutput[idx].Success {
			common.ProcessThroughputLat(fmt.Sprintf("q7-transform-%d", idx),
				q7TransOutput[idx].Latencies, q7TransOutput[idx].Duration)
		}
	}
}
