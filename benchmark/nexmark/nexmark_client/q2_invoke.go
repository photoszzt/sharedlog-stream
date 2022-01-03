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

func query2() {
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

	var q2conf BasicWorkloadConfig
	err = json.Unmarshal(byteVal, &q2conf)
	if err != nil {
		panic(err)
	}

	q2NodeConfig := &processor.ClientNodeConfig{
		FuncName:    "query2",
		GatewayUrl:  FLAGS_faas_gateway,
		NumInstance: q2conf.NumSrcPartition,
	}
	q2InputParams := make([]*common.QueryInput, q2NodeConfig.NumInstance)
	for i := 0; i < int(q2NodeConfig.NumInstance); i++ {
		q2InputParams[i] = &common.QueryInput{
			Duration:          uint32(FLAGS_duration),
			InputTopicName:    q2conf.SrcOutTopic,
			OutputTopicName:   q2conf.SinkOutTopic,
			SerdeFormat:       uint8(serdeFormat),
			NumInPartition:    q2conf.NumSrcPartition,
			NumOutPartition:   q2conf.NumSrcPartition,
			EnableTransaction: FLAGS_tran,
			CommitEveryMs:     FLAGS_commit_every,
		}
	}
	q2Node := processor.NewClientNode(q2NodeConfig)

	client := &http.Client{
		Transport: &http.Transport{
			IdleConnTimeout: 30 * time.Second,
		},
		Timeout: time.Duration(FLAGS_duration*2) * time.Second,
	}

	var wg sync.WaitGroup

	sourceOutput := make([]common.FnOutput, q2conf.NumSrcPartition)
	q2Output := make([]common.FnOutput, q2conf.NumSrcPartition)

	for i := 0; i < int(q2conf.NumSrcPartition); i++ {
		wg.Add(1)
		fmt.Fprintf(os.Stderr, "invoking src %d\n", i)
		go invokeSourceFunc(client, q2conf.NumSrcPartition, uint8(i), &sourceOutput[i], &wg)
	}

	for i := 0; i < int(q2conf.NumSrcPartition); i++ {
		wg.Add(1)
		q2InputParams[i].ParNum = uint8(i)
		fmt.Fprintf(os.Stderr, "invoking q1 %d\n", i)
		go q2Node.Invoke(client, &q2Output[i], &wg, q2InputParams[i])
	}
	wg.Wait()

	for i := 0; i < int(q2conf.NumSrcPartition); i++ {
		if sourceOutput[i].Success {
			common.ProcessThroughputLat(fmt.Sprintf("source-%d", i),
				sourceOutput[i].Latencies, sourceOutput[i].Duration)
		}
	}

	for i := 0; i < int(q2conf.NumSrcPartition); i++ {
		if q2Output[i].Success {
			common.ProcessThroughputLat(fmt.Sprintf("q1-%d", i),
				q2Output[i].Latencies, q2Output[i].Duration)
		}
	}
}
