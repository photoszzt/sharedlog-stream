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

func query8() {
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
	var q8conf Q8WorkloadConfig
	err = json.Unmarshal(byteVal, &q8conf)
	if err != nil {
		panic(err)
	}

	aucsBySellerIDNodeConfig := &processor.ClientNodeConfig{
		FuncName:    "q8AucsBySellerID",
		GatewayUrl:  FLAGS_faas_gateway,
		NumInstance: q8conf.NumSrcPartition,
	}
	aucsBySellerIDInputParams := make([]*common.QueryInput, aucsBySellerIDNodeConfig.NumInstance)
	for i := uint8(0); i < aucsBySellerIDNodeConfig.NumInstance; i++ {
		aucsBySellerIDInputParams[i] = &common.QueryInput{
			Duration:          uint32(FLAGS_duration),
			InputTopicNames:   []string{q8conf.SrcOutTopic},
			OutputTopicName:   q8conf.AucsBySellerIDOutTopic,
			SerdeFormat:       uint8(serdeFormat),
			NumInPartition:    q8conf.NumSrcPartition,
			NumOutPartition:   q8conf.NumInstance,
			EnableTransaction: FLAGS_tran,
			CommitEveryMs:     FLAGS_commit_every,
		}
	}
	aucsBySellerID := processor.NewClientNode(aucsBySellerIDNodeConfig)

	personsByIDNodeConfig := &processor.ClientNodeConfig{
		FuncName:    "q8PersonsByID",
		GatewayUrl:  FLAGS_faas_gateway,
		NumInstance: q8conf.NumSrcPartition,
	}
	personsByIDInputParams := make([]*common.QueryInput, personsByIDNodeConfig.NumInstance)
	for i := uint8(0); i < personsByIDNodeConfig.NumInstance; i++ {
		personsByIDInputParams[i] = &common.QueryInput{
			Duration:          uint32(FLAGS_duration),
			InputTopicNames:   []string{q8conf.SrcOutTopic},
			OutputTopicName:   q8conf.PersonsByIDOutTopic,
			SerdeFormat:       uint8(serdeFormat),
			NumInPartition:    q8conf.NumSrcPartition,
			NumOutPartition:   q8conf.NumInstance,
			EnableTransaction: FLAGS_tran,
			CommitEveryMs:     FLAGS_commit_every,
		}
	}
	personsByID := processor.NewClientNode(personsByIDNodeConfig)

	joinStreamNodeConfig := &processor.ClientNodeConfig{
		FuncName:    "q8JoinStream",
		GatewayUrl:  FLAGS_faas_gateway,
		NumInstance: q8conf.NumInstance,
	}
	joinStreamInputParams := make([]*common.QueryInput, joinStreamNodeConfig.NumInstance)
	for i := uint8(0); i < joinStreamNodeConfig.NumInstance; i++ {
		joinStreamInputParams[i] = &common.QueryInput{
			Duration:          uint32(FLAGS_duration),
			InputTopicNames:   []string{q8conf.AucsBySellerIDOutTopic, q8conf.PersonsByIDOutTopic},
			OutputTopicName:   q8conf.JoinStreamOutTopic,
			SerdeFormat:       uint8(serdeFormat),
			NumInPartition:    q8conf.NumInstance,
			NumOutPartition:   q8conf.NumInstance,
			EnableTransaction: FLAGS_tran,
			CommitEveryMs:     FLAGS_commit_every,
		}
	}
	joinStream := processor.NewClientNode(joinStreamNodeConfig)

	client := &http.Client{
		Transport: &http.Transport{
			IdleConnTimeout: 30 * time.Second,
		},
		Timeout: time.Duration(FLAGS_duration*3) * time.Second,
	}

	var wg sync.WaitGroup
	sourceOutput := make([]common.FnOutput, q8conf.NumSrcInstance)
	personsByIDOutput := make([]common.FnOutput, personsByIDNodeConfig.NumInstance)
	auctionsBySellerIDOutput := make([]common.FnOutput, aucsBySellerIDNodeConfig.NumInstance)
	joinStreamOutput := make([]common.FnOutput, joinStreamNodeConfig.NumInstance)

	for i := uint8(0); i < q8conf.NumSrcInstance; i++ {
		wg.Add(1)
		idx := i
		go invokeSourceFunc(client, q8conf.NumSrcPartition, &sourceOutput[idx], &wg)
	}

	for i := uint8(0); i < q8conf.NumInstance; i++ {
		wg.Add(1)
		idx := i
		personsByIDInputParams[idx].ParNum = idx
		go personsByID.Invoke(client, &personsByIDOutput[idx], &wg, personsByIDInputParams[idx])
	}

	for i := uint8(0); i < q8conf.NumInstance; i++ {
		wg.Add(1)
		idx := i
		aucsBySellerIDInputParams[idx].ParNum = idx
		go aucsBySellerID.Invoke(client, &auctionsBySellerIDOutput[idx], &wg, aucsBySellerIDInputParams[idx])
	}

	for i := uint8(0); i < q8conf.NumInstance; i++ {
		wg.Add(1)
		idx := i
		joinStreamInputParams[idx].ParNum = idx
		go joinStream.Invoke(client, &joinStreamOutput[idx], &wg, joinStreamInputParams[idx])
	}
	wg.Wait()

	for i := uint8(0); i < q8conf.NumSrcInstance; i++ {
		idx := i
		if sourceOutput[idx].Success {
			common.ProcessThroughputLat(fmt.Sprintf("source-%d", idx),
				sourceOutput[idx].Latencies, sourceOutput[idx].Duration)
		}
	}

	for i := uint8(0); i < q8conf.NumInstance; i++ {
		idx := i
		if personsByIDOutput[idx].Success {
			common.ProcessThroughputLat(fmt.Sprintf("q3-persons-by-id-%d", idx),
				personsByIDOutput[idx].Latencies, personsByIDOutput[idx].Duration)
		}
	}

	for i := uint8(0); i < q8conf.NumInstance; i++ {
		idx := i
		if auctionsBySellerIDOutput[idx].Success {
			common.ProcessThroughputLat(fmt.Sprintf("q3-auctions-by-sellerid-%d", idx),
				auctionsBySellerIDOutput[idx].Latencies, auctionsBySellerIDOutput[idx].Duration)
		}
	}

	for i := uint8(0); i < q8conf.NumInstance; i++ {
		idx := i
		if joinStreamOutput[idx].Success {
			common.ProcessThroughputLat(fmt.Sprintf("q3-join-table-%d", idx),
				joinStreamOutput[idx].Latencies, joinStreamOutput[idx].Duration)
		}
	}
}
