package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"sharedlog-stream/benchmark/common"
	configscale "sharedlog-stream/benchmark/common/config_scale"
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
	var q1conf BasicWorkloadConfig
	err = json.Unmarshal(byteVal, &q1conf)
	if err != nil {
		panic(err)
	}
	// fmt.Fprintf(os.Stderr, "q1 config is %v\n", q1conf)

	scaleConfig := &ScaleConfig{
		Config: make(map[string]uint8),
		AppId:  "q1",
	}
	scaleConfig.Config["query1"] = q1conf.NumSrcPartition
	scaleConfig.Config["nexmark_src"] = q1conf.NumSrcInstance
	scaleConfig.Config[q1conf.SrcOutTopic] = q1conf.NumSrcPartition
	scaleConfig.Config[q1conf.SinkOutTopic] = q1conf.NumSrcPartition

	q1NodeConfig := &processor.ClientNodeConfig{
		FuncName:    "query1",
		GatewayUrl:  FLAGS_faas_gateway,
		NumInstance: q1conf.NumSrcPartition,
	}
	q1InputParams := make([]*common.QueryInput, q1NodeConfig.NumInstance)
	for i := 0; i < int(q1NodeConfig.NumInstance); i++ {
		q1InputParams[i] = NewQueryInput(uint8(serdeFormat))
		q1InputParams[i].InputTopicNames = []string{q1conf.SrcOutTopic}
		q1InputParams[i].OutputTopicName = q1conf.SinkOutTopic
		q1InputParams[i].NumInPartition = q1conf.NumSrcPartition
		q1InputParams[i].NumOutPartition = q1conf.NumSrcPartition
	}
	q1Node := processor.NewClientNode(q1NodeConfig)

	client := &http.Client{
		Transport: &http.Transport{
			IdleConnTimeout: 30 * time.Second,
		},
		Timeout: time.Duration(FLAGS_duration*2) * time.Second,
	}

	var wg sync.WaitGroup

	sourceOutput := make([]common.FnOutput, q1conf.NumSrcInstance)
	q1Output := make([]common.FnOutput, q1conf.NumSrcPartition)

	var scaleOut common.FnOutput
	configscale.InvokeConfigScale(client, scaleConfig.Config,
		scaleConfig.AppId, FLAGS_faas_gateway, uint8(serdeFormat),
		&scaleOut)

	for i := 0; i < int(q1conf.NumSrcInstance); i++ {
		wg.Add(1)
		fmt.Fprintf(os.Stderr, "invoking src %d\n", i)
		go invokeSourceFunc(client, q1conf.NumSrcPartition, &sourceOutput[i], &wg)
	}

	for i := 0; i < int(q1conf.NumSrcPartition); i++ {
		wg.Add(1)
		q1InputParams[i].ParNum = uint8(i)
		fmt.Fprintf(os.Stderr, "invoking q1 %d\n", i)
		go q1Node.Invoke(client, &q1Output[i], &wg, q1InputParams[i])
	}
	time.Sleep(time.Duration(90) * time.Second)

	fmt.Fprint(os.Stderr, "scaling up\n")
	// scales up
	scaleConfig = &ScaleConfig{
		Config: make(map[string]uint8),
		AppId:  "q1",
	}
	scaleConfig.Config["query1"] = q1conf.NumSrcPartition
	scaleConfig.Config["nexmark_src"] = q1conf.NumSrcInstance + 2
	scaleConfig.Config[q1conf.SrcOutTopic] = q1conf.NumSrcPartition + 2
	scaleConfig.Config[q1conf.SinkOutTopic] = q1conf.NumSrcPartition + 2

	configscale.InvokeConfigScale(client, scaleConfig.Config,
		scaleConfig.AppId, FLAGS_faas_gateway, uint8(serdeFormat),
		&scaleOut)

	/*
		for i := q1conf.NumSrcInstance; i < q1conf.NumSrcInstance+2; i++ {
			wg.Add(1)
			sourceOutput = append(sourceOutput, common.FnOutput{})
			fmt.Fprintf(os.Stderr, "invoking src %d\n", i)
			go invokeSourceFunc(client, q1conf.NumSrcPartition+2, &sourceOutput[i], &wg)
		}
	*/
	newQ1Output := make([]common.FnOutput, 2)
	newQ1InputParams := make([]*common.QueryInput, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		newQ1InputParams[i] = NewQueryInput(uint8(serdeFormat))
		newQ1InputParams[i].InputTopicNames = []string{q1conf.SrcOutTopic}
		newQ1InputParams[i].OutputTopicName = q1conf.SinkOutTopic
		newQ1InputParams[i].NumInPartition = q1conf.NumSrcPartition + 2
		newQ1InputParams[i].NumOutPartition = q1conf.NumSrcPartition + 2
		q1Output = append(q1Output, common.FnOutput{})
		newQ1InputParams[i].ParNum = uint8(i)
		fmt.Fprintf(os.Stderr, "invoking q1 %d\n", i+int(q1conf.NumSrcPartition))
		go q1Node.Invoke(client, &newQ1Output[i], &wg, newQ1InputParams[i])
	}

	time.Sleep(time.Duration(90) * time.Second)

	// scales down
	fmt.Fprint(os.Stderr, "scaling down\n")
	scaleConfig = &ScaleConfig{
		Config: make(map[string]uint8),
		AppId:  "q1",
	}
	scaleConfig.Config["query1"] = q1conf.NumSrcPartition
	scaleConfig.Config["nexmark_src"] = q1conf.NumSrcInstance
	scaleConfig.Config[q1conf.SrcOutTopic] = q1conf.NumSrcPartition
	scaleConfig.Config[q1conf.SinkOutTopic] = q1conf.NumSrcPartition

	configscale.InvokeConfigScale(client, scaleConfig.Config,
		scaleConfig.AppId, FLAGS_faas_gateway, uint8(serdeFormat),
		&scaleOut)

	wg.Wait()

	for i := 0; i < int(q1conf.NumSrcInstance); i++ {
		if sourceOutput[i].Success {
			common.ProcessThroughputLat(fmt.Sprintf("source-%d", i),
				sourceOutput[i].Latencies, sourceOutput[i].Duration)
		}
	}

	for i := 0; i < int(q1conf.NumSrcPartition); i++ {
		if q1Output[i].Success {
			common.ProcessThroughputLat(fmt.Sprintf("q1-%d", i), q1Output[i].Latencies, q1Output[i].Duration)
		} else {
			fmt.Fprintf(os.Stderr, "output: %v\n", q1Output[i])
		}
	}
}
