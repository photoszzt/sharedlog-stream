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

func query5() {
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
	var q5conf Q5WorkloadConfig
	err = json.Unmarshal(byteVal, &q5conf)
	if err != nil {
		panic(err)
	}

	q5BidKeyedByAuctionNodeConfig := &processor.ClientNodeConfig{
		FuncName:    "q5bidkeyedbyauction",
		GatewayUrl:  FLAGS_faas_gateway,
		NumInstance: q5conf.NumSrcPartition,
	}
	q5BidKeyedByAuctionInputParams := make([]*common.QueryInput, q5BidKeyedByAuctionNodeConfig.NumInstance)
	for i := 0; i < int(q5BidKeyedByAuctionNodeConfig.NumInstance); i++ {
		q5BidKeyedByAuctionInputParams[i] = &common.QueryInput{
			Duration:        uint32(FLAGS_duration),
			InputTopicName:  q5conf.SrcOutTopic,
			OutputTopicName: q5conf.BidKeyedByAuctionOutTopic,
			SerdeFormat:     uint8(serdeFormat),
			NumInPartition:  q5conf.NumSrcPartition,
			NumOutPartition: q5conf.NumInstance,
		}
	}
	q5BidKeyedByAuction := processor.NewClientNode(q5BidKeyedByAuctionNodeConfig)

	q5AucBidsNodeConfig := &processor.ClientNodeConfig{
		FuncName:    "q5aucbids",
		GatewayUrl:  FLAGS_faas_gateway,
		NumInstance: q5conf.NumInstance,
	}
	q5aucBidsInputParams := make([]*common.QueryInput, q5AucBidsNodeConfig.NumInstance)
	for i := 0; i < int(q5AucBidsNodeConfig.NumInstance); i++ {
		q5aucBidsInputParams[i] = &common.QueryInput{
			Duration:        uint32(FLAGS_duration),
			InputTopicName:  q5conf.BidKeyedByAuctionOutTopic,
			OutputTopicName: q5conf.AucBidsOutTopic,
			SerdeFormat:     uint8(serdeFormat),
			NumInPartition:  q5conf.NumInstance,
			NumOutPartition: q5conf.NumInstance,
		}
	}
	q5aucBids := processor.NewClientNode(q5AucBidsNodeConfig)

	q5maxbidNodeConfig := &processor.ClientNodeConfig{
		FuncName:    "q5maxbid",
		GatewayUrl:  FLAGS_faas_gateway,
		NumInstance: q5conf.NumInstance,
	}
	q5maxbidInputParams := make([]*common.QueryInput, q5maxbidNodeConfig.NumInstance)
	for i := 0; i < int(q5maxbidNodeConfig.NumInstance); i++ {
		q5maxbidInputParams[i] = &common.QueryInput{
			Duration:        uint32(FLAGS_duration),
			InputTopicName:  q5conf.AucBidsOutTopic,
			OutputTopicName: q5conf.MaxBidsOutTopic,
			SerdeFormat:     uint8(serdeFormat),
			NumInPartition:  q5conf.NumInstance,
			NumOutPartition: q5conf.NumInstance,
		}
	}
	q5maxBid := processor.NewClientNode(q5maxbidNodeConfig)

	client := &http.Client{
		Transport: &http.Transport{
			IdleConnTimeout: 30 * time.Second,
		},
		Timeout: time.Duration(FLAGS_duration*2) * time.Second,
	}

	var wg sync.WaitGroup
	sourceOutput := make([]common.FnOutput, q5conf.NumSrcPartition)
	q5BidKeyedByAuctionOutput := make([]common.FnOutput, q5BidKeyedByAuctionNodeConfig.NumInstance)
	q5AucBidsOutput := make([]common.FnOutput, q5AucBidsNodeConfig.NumInstance)
	q5maxBidOutput := make([]common.FnOutput, q5maxbidNodeConfig.NumInstance)

	for i := 0; i < int(q5conf.NumSrcPartition); i++ {
		wg.Add(1)
		go invokeSourceFunc(client, uint8(i), &sourceOutput[i], &wg)
	}

	for i := 0; i < int(q5BidKeyedByAuctionNodeConfig.NumInstance); i++ {
		wg.Add(1)
		q5BidKeyedByAuctionInputParams[i].ParNum = uint8(i)
		go q5BidKeyedByAuction.Invoke(client, &q5BidKeyedByAuctionOutput[i], &wg, q5BidKeyedByAuctionInputParams[i])
	}

	for i := 0; i < int(q5AucBidsNodeConfig.NumInstance); i++ {
		wg.Add(1)
		q5aucBidsInputParams[i].ParNum = uint8(i)
		go q5aucBids.Invoke(client, &q5AucBidsOutput[i], &wg, q5aucBidsInputParams[i])
	}

	for i := 0; i < int(q5maxbidNodeConfig.NumInstance); i++ {
		wg.Add(1)
		q5maxbidInputParams[i].ParNum = uint8(i)
		go q5maxBid.Invoke(client, &q5maxBidOutput[i], &wg, q5maxbidInputParams[i])
	}
	wg.Wait()

	for i := 0; i < int(q5conf.NumSrcPartition); i++ {
		if sourceOutput[i].Success {
			common.ProcessThroughputLat(fmt.Sprintf("source-%d", i),
				sourceOutput[i].Latencies, sourceOutput[i].Duration)
		}
	}

	for i := 0; i < int(q5BidKeyedByAuctionNodeConfig.NumInstance); i++ {
		if q5BidKeyedByAuctionOutput[i].Success {
			common.ProcessThroughputLat(fmt.Sprintf("q5-bid-keyed-by-auciton-%d", i),
				q5BidKeyedByAuctionOutput[i].Latencies, q5BidKeyedByAuctionOutput[i].Duration)
		}
	}

	for i := 0; i < int(q5AucBidsNodeConfig.NumInstance); i++ {
		if q5AucBidsOutput[i].Success {
			common.ProcessThroughputLat(fmt.Sprintf("q5-auction-bids-%d", i),
				q5AucBidsOutput[i].Latencies, q5AucBidsOutput[i].Duration)
		}
	}

	for i := 0; i < int(q5maxbidNodeConfig.NumInstance); i++ {
		if q5maxBidOutput[i].Success {
			common.ProcessThroughputLat(fmt.Sprintf("q5-max-bids-%d", i),
				q5maxBidOutput[i].Latencies, q5maxBidOutput[i].Duration)
		}
	}
}
