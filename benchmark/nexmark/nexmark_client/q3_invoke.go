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

func query3() {
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
	var q3conf Q3WorkloadConfig
	err = json.Unmarshal(byteVal, &q3conf)
	if err != nil {
		panic(err)
	}

	auctionsBySellerIDNodeConfig := &processor.ClientNodeConfig{
		FuncName:    "q3AuctionsBySellerID",
		GatewayUrl:  FLAGS_faas_gateway,
		NumInstance: q3conf.NumSrcPartition,
	}
	auctionsBySellerIDInputParams := make([]*common.QueryInput, auctionsBySellerIDNodeConfig.NumInstance)
	for i := uint8(0); i < auctionsBySellerIDNodeConfig.NumInstance; i++ {
		auctionsBySellerIDInputParams[i] = NewQueryInput(uint8(serdeFormat))
		auctionsBySellerIDInputParams[i].InputTopicNames = []string{q3conf.SrcOutTopic}
		auctionsBySellerIDInputParams[i].OutputTopicName = q3conf.AucsBySellerIDOutTopic
		auctionsBySellerIDInputParams[i].NumInPartition = q3conf.NumSrcPartition
		auctionsBySellerIDInputParams[i].NumOutPartition = q3conf.NumInstance
	}
	auctionsBySellerID := processor.NewClientNode(auctionsBySellerIDNodeConfig)

	personsByIDNodeConfig := &processor.ClientNodeConfig{
		FuncName:    "q3PersonsByID",
		GatewayUrl:  FLAGS_faas_gateway,
		NumInstance: q3conf.NumSrcPartition,
	}
	personsByIDInputParams := make([]*common.QueryInput, personsByIDNodeConfig.NumInstance)
	for i := uint8(0); i < personsByIDNodeConfig.NumInstance; i++ {
		personsByIDInputParams[i] = NewQueryInput(uint8(serdeFormat))
		personsByIDInputParams[i].InputTopicNames = []string{q3conf.SrcOutTopic}
		personsByIDInputParams[i].OutputTopicName = q3conf.PersonsByIDOutTopic
		personsByIDInputParams[i].NumInPartition = q3conf.NumSrcPartition
		personsByIDInputParams[i].NumOutPartition = q3conf.NumInstance
	}
	personsByID := processor.NewClientNode(personsByIDNodeConfig)

	joinTableNodeConfig := &processor.ClientNodeConfig{
		FuncName:    "q3JoinTable",
		GatewayUrl:  FLAGS_faas_gateway,
		NumInstance: q3conf.NumInstance,
	}
	joinTableInputParams := make([]*common.QueryInput, joinTableNodeConfig.NumInstance)
	for i := uint8(0); i < joinTableNodeConfig.NumInstance; i++ {
		joinTableInputParams[i] = NewQueryInput(uint8(serdeFormat))
		joinTableInputParams[i].InputTopicNames = []string{q3conf.AucsBySellerIDOutTopic, q3conf.PersonsByIDOutTopic}
		joinTableInputParams[i].OutputTopicName = q3conf.JoinTableOutTopic
		joinTableInputParams[i].NumInPartition = q3conf.NumInstance
		joinTableInputParams[i].NumOutPartition = q3conf.NumInstance

	}
	joinTable := processor.NewClientNode(joinTableNodeConfig)

	client := &http.Client{
		Transport: &http.Transport{
			IdleConnTimeout: 30 * time.Second,
		},
		Timeout: time.Duration(FLAGS_duration*3) * time.Second,
	}

	var wg sync.WaitGroup
	sourceOutput := make([]common.FnOutput, q3conf.NumSrcInstance)
	personsByIDOutput := make([]common.FnOutput, personsByIDNodeConfig.NumInstance)
	auctionsBySellerIDOutput := make([]common.FnOutput, auctionsBySellerIDNodeConfig.NumInstance)
	joinTableOutput := make([]common.FnOutput, joinTableNodeConfig.NumInstance)

	for i := uint8(0); i < q3conf.NumSrcInstance; i++ {
		wg.Add(1)
		idx := i
		go invokeSourceFunc(client, q3conf.NumSrcPartition, &sourceOutput[idx], &wg)
	}

	for i := uint8(0); i < q3conf.NumInstance; i++ {
		wg.Add(1)
		idx := i
		personsByIDInputParams[idx].ParNum = idx
		go personsByID.Invoke(client, &personsByIDOutput[idx], &wg, personsByIDInputParams[idx])
	}

	for i := uint8(0); i < q3conf.NumInstance; i++ {
		wg.Add(1)
		idx := i
		auctionsBySellerIDInputParams[idx].ParNum = idx
		go auctionsBySellerID.Invoke(client, &auctionsBySellerIDOutput[idx], &wg, auctionsBySellerIDInputParams[idx])
	}

	for i := uint8(0); i < q3conf.NumInstance; i++ {
		wg.Add(1)
		idx := i
		joinTableInputParams[idx].ParNum = idx
		go joinTable.Invoke(client, &joinTableOutput[idx], &wg, joinTableInputParams[idx])
	}
	wg.Wait()

	for i := uint8(0); i < q3conf.NumSrcInstance; i++ {
		idx := i
		if sourceOutput[idx].Success {
			common.ProcessThroughputLat(fmt.Sprintf("source-%d", idx),
				sourceOutput[idx].Latencies, sourceOutput[idx].Duration)
		}
	}

	for i := uint8(0); i < q3conf.NumInstance; i++ {
		idx := i
		if personsByIDOutput[idx].Success {
			common.ProcessThroughputLat(fmt.Sprintf("q3-persons-by-id-%d", idx),
				personsByIDOutput[idx].Latencies, personsByIDOutput[idx].Duration)
		}
	}

	for i := uint8(0); i < q3conf.NumInstance; i++ {
		idx := i
		if auctionsBySellerIDOutput[idx].Success {
			common.ProcessThroughputLat(fmt.Sprintf("q3-auctions-by-sellerid-%d", idx),
				auctionsBySellerIDOutput[idx].Latencies, auctionsBySellerIDOutput[idx].Duration)
		}
	}

	for i := uint8(0); i < q3conf.NumInstance; i++ {
		idx := i
		if joinTableOutput[idx].Success {
			common.ProcessThroughputLat(fmt.Sprintf("q3-join-table-%d", idx),
				joinTableOutput[idx].Latencies, joinTableOutput[idx].Duration)
		}
	}
}
