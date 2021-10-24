package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	"sharedlog-stream/benchmark/common"
	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"

	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"

	"github.com/rs/zerolog/log"
)

var (
	FLAGS_faas_gateway  string
	FLAGS_app_name      string
	FLAGS_stream_prefix string
	FLAGS_duration      int
	FLAGS_events_num    int
	FLAGS_tps           int
	FLAGS_serdeFormat   string
)

func invokeSourceFunc(client *http.Client, response *common.FnOutput, wg *sync.WaitGroup) {
	defer wg.Done()
	var serdeFormat commtypes.SerdeFormat
	if FLAGS_serdeFormat == "json" {
		serdeFormat = commtypes.JSON
	} else if FLAGS_serdeFormat == "msgp" {
		serdeFormat = commtypes.MSGP
	} else {
		log.Error().Msgf("serde format is not recognized; default back to JSON")
		serdeFormat = commtypes.JSON
	}
	nexmarkConfig := ntypes.NewNexMarkConfigInput(FLAGS_stream_prefix+"_src", serdeFormat)
	nexmarkConfig.Duration = uint32(FLAGS_duration)
	nexmarkConfig.FirstEventRate = uint32(FLAGS_tps)
	nexmarkConfig.NextEventRate = uint32(FLAGS_tps)
	nexmarkConfig.EventsNum = uint64(FLAGS_events_num)
	url := utils.BuildFunctionUrl(FLAGS_faas_gateway, "source")
	fmt.Printf("func source url is %v\n", url)
	if err := utils.JsonPostRequest(client, url, nexmarkConfig, response); err != nil {
		log.Error().Msgf("source request failed: %v", err)
	} else if !response.Success {
		log.Error().Msgf("source request failed: %s", response.Message)
	}
}

func invokeQuery(client *http.Client, response *common.FnOutput, wg *sync.WaitGroup) {
	defer wg.Done()
	queryInput := &ntypes.QueryInput{
		Duration:        uint32(FLAGS_duration),
		InputTopicName:  FLAGS_stream_prefix + "_src",
		OutputTopicName: FLAGS_stream_prefix + "_" + FLAGS_app_name + "_output",
	}
	url := utils.BuildFunctionUrl(FLAGS_faas_gateway, FLAGS_app_name)
	fmt.Printf("func url is %v\n", url)
	if err := utils.JsonPostRequest(client, url, queryInput, response); err != nil {
		log.Error().Msgf("%v request failed with json post request returned err: %v", FLAGS_app_name, err)
	} else if !response.Success {
		log.Error().Msgf("%v request failed with unsuccess error: %v", FLAGS_app_name, response.Message)
	}
	fmt.Fprintf(os.Stderr, "response is %v\n", response)
}

func generalQuery() {
	client := &http.Client{
		Transport: &http.Transport{
			IdleConnTimeout: 30 * time.Second,
		},
		Timeout: time.Duration(FLAGS_duration*2) * time.Second,
	}
	var wg sync.WaitGroup
	var sourceOutput, queryOutput common.FnOutput
	wg.Add(1)
	go invokeSourceFunc(client, &sourceOutput, &wg)
	wg.Add(1)
	go invokeQuery(client, &queryOutput, &wg)
	wg.Wait()
}

func getSerdeFormat() commtypes.SerdeFormat {
	var serdeFormat commtypes.SerdeFormat
	if FLAGS_serdeFormat == "json" {
		serdeFormat = commtypes.JSON
	} else if FLAGS_serdeFormat == "msgp" {
		serdeFormat = commtypes.MSGP
	} else {
		log.Error().Msgf("serde format is not recognized; default back to JSON")
		serdeFormat = commtypes.JSON
	}
	return serdeFormat
}

func query5() {
	serdeFormat := getSerdeFormat()

	numInstance := uint8(5)

	q5BidKeyedByAuctionNodeConfig := &processor.ClientNodeConfig{
		FuncName:    "q5bidkeyedbyauction",
		GatewayUrl:  FLAGS_faas_gateway,
		NumInstance: 1,
	}
	q5BidKeyedByAuctionInputParams := make([]*common.QueryInput, q5BidKeyedByAuctionNodeConfig.NumInstance)
	for i := 0; i < int(q5BidKeyedByAuctionNodeConfig.NumInstance); i++ {
		q5BidKeyedByAuctionInputParams[i] = &common.QueryInput{
			Duration:        uint32(FLAGS_duration),
			InputTopicName:  "nexmark_src",
			OutputTopicName: "bids-repartition",
			SerdeFormat:     uint8(serdeFormat),
			NumInPartition:  1,
			NumOutPartition: numInstance,
		}
	}
	q5BidKeyedByAuction := processor.NewClientNode(q5BidKeyedByAuctionNodeConfig)

	q5AucBidsNodeConfig := &processor.ClientNodeConfig{
		FuncName:    "q5aucbids",
		GatewayUrl:  FLAGS_faas_gateway,
		NumInstance: uint32(numInstance),
	}
	q5aucBidsInputParams := make([]*common.QueryInput, q5AucBidsNodeConfig.NumInstance)
	for i := 0; i < int(q5AucBidsNodeConfig.NumInstance); i++ {
		q5aucBidsInputParams[i] = &common.QueryInput{
			Duration:        uint32(FLAGS_duration),
			InputTopicName:  "bids-repartition",
			OutputTopicName: "auctionBids-repartition",
			SerdeFormat:     uint8(serdeFormat),
			NumInPartition:  numInstance,
			NumOutPartition: numInstance,
		}
	}
	q5aucBids := processor.NewClientNode(q5AucBidsNodeConfig)

	q5maxbidNodeConfig := &processor.ClientNodeConfig{
		FuncName:    "q5maxbid",
		GatewayUrl:  FLAGS_faas_gateway,
		NumInstance: uint32(numInstance),
	}
	q5maxbidInputParams := make([]*common.QueryInput, q5maxbidNodeConfig.NumInstance)
	for i := 0; i < int(q5maxbidNodeConfig.NumInstance); i++ {
		q5maxbidInputParams[i] = &common.QueryInput{
			Duration:        uint32(FLAGS_duration),
			InputTopicName:  "auctionBids-repartition",
			OutputTopicName: "nexmark-q5-out",
			SerdeFormat:     uint8(serdeFormat),
			NumInPartition:  numInstance,
			NumOutPartition: numInstance,
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
	var sourceOutput common.FnOutput
	q5BidKeyedByAuctionOutput := make([]common.FnOutput, q5BidKeyedByAuctionNodeConfig.NumInstance)
	q5AucBidsOutput := make([]common.FnOutput, q5AucBidsNodeConfig.NumInstance)
	q5maxBidOutput := make([]common.FnOutput, q5maxbidNodeConfig.NumInstance)

	wg.Add(1)
	go invokeSourceFunc(client, &sourceOutput, &wg)

	for i := 0; i < int(q5BidKeyedByAuctionNodeConfig.NumInstance); i++ {
		wg.Add(1)
		q5BidKeyedByAuctionInputParams[i].ParNum = 0
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

	if sourceOutput.Success {
		common.ProcessThroughputLat("source", sourceOutput.Latencies["e2e"], sourceOutput.Duration)
	}

	for i := 0; i < int(q5BidKeyedByAuctionNodeConfig.NumInstance); i++ {
		if q5BidKeyedByAuctionOutput[i].Success {
			common.ProcessThroughputLat("q5-bid-keyed-by-auciton", q5BidKeyedByAuctionOutput[i].Latencies["e2e"], q5BidKeyedByAuctionOutput[i].Duration)
		}
	}

	for i := 0; i < int(q5AucBidsNodeConfig.NumInstance); i++ {
		if q5AucBidsOutput[i].Success {
			common.ProcessThroughputLat("q5-auction-bids", q5BidKeyedByAuctionOutput[i].Latencies["e2e"], q5BidKeyedByAuctionOutput[i].Duration)
		}
	}

	for i := 0; i < int(q5maxbidNodeConfig.NumInstance); i++ {
		if q5maxBidOutput[i].Success {
			common.ProcessThroughputLat("q5-max-bids", q5maxBidOutput[i].Latencies["e2e"], q5maxBidOutput[i].Duration)
		}
	}
}

func query7() {
	serdeFormat := getSerdeFormat()

	numInstance := uint8(5)
	q7BidKeyedByPriceNodeConfig := &processor.ClientNodeConfig{
		FuncName:    "q7bidskeyedbyprice",
		GatewayUrl:  FLAGS_faas_gateway,
		NumInstance: 1,
	}
	q7BidKeyedByPriceInputParams := make([]*common.QueryInput, q7BidKeyedByPriceNodeConfig.NumInstance)
	for i := 0; i < int(q7BidKeyedByPriceNodeConfig.NumInstance); i++ {
		q7BidKeyedByPriceInputParams[i] = &common.QueryInput{
			Duration:        uint32(FLAGS_duration),
			InputTopicName:  "nexmark_src",
			OutputTopicName: "bids-repartition",
			SerdeFormat:     uint8(serdeFormat),
			NumInPartition:  1,
			NumOutPartition: numInstance,
		}
	}
	q7BidKeyedByPrice := processor.NewClientNode(q7BidKeyedByPriceNodeConfig)

	q7TransNodeConfig := &processor.ClientNodeConfig{
		FuncName:    "query7",
		GatewayUrl:  FLAGS_faas_gateway,
		NumInstance: uint32(numInstance),
	}
	q7TransInputParams := make([]*common.QueryInput, q7TransNodeConfig.NumInstance)
	for i := 0; i < int(q7TransNodeConfig.NumInstance); i++ {
		q7TransInputParams[i] = &common.QueryInput{
			Duration:        uint32(FLAGS_duration),
			InputTopicName:  "bids-repartition",
			OutputTopicName: "nexmark-q7-out",
			SerdeFormat:     uint8(serdeFormat),
			NumInPartition:  numInstance,
			NumOutPartition: numInstance,
		}
	}
	q7Trans := processor.NewClientNode(q7TransNodeConfig)

	client := &http.Client{
		Transport: &http.Transport{
			IdleConnTimeout: 30 * time.Second,
		},
		Timeout: time.Duration(FLAGS_duration*2) * time.Second,
	}

	var wg sync.WaitGroup
	var sourceOutput common.FnOutput
	q7BidKeyedByPriceOutput := make([]common.FnOutput, q7BidKeyedByPriceNodeConfig.NumInstance)
	q7TransOutput := make([]common.FnOutput, q7TransNodeConfig.NumInstance)

	wg.Add(1)
	go invokeSourceFunc(client, &sourceOutput, &wg)

	for i := 0; i < int(q7BidKeyedByPriceNodeConfig.NumInstance); i++ {
		wg.Add(1)
		q7BidKeyedByPriceInputParams[i].ParNum = 0
		go q7BidKeyedByPrice.Invoke(client, &q7BidKeyedByPriceOutput[i], &wg, q7BidKeyedByPriceInputParams[i])
	}

	for i := 0; i < int(q7TransNodeConfig.NumInstance); i++ {
		wg.Add(1)
		q7TransInputParams[i].ParNum = uint8(i)
		go q7Trans.Invoke(client, &q7TransOutput[i], &wg, q7TransInputParams[i])
	}
	wg.Wait()

	if sourceOutput.Success {
		common.ProcessThroughputLat("source", sourceOutput.Latencies["e2e"], sourceOutput.Duration)
	}

	for i := 0; i < int(q7BidKeyedByPriceNodeConfig.NumInstance); i++ {
		wg.Add(1)
		q7BidKeyedByPriceInputParams[i].ParNum = 0
		go q7BidKeyedByPrice.Invoke(client, &q7BidKeyedByPriceOutput[i], &wg, q7BidKeyedByPriceInputParams[i])
	}

	for i := 0; i < int(q7TransNodeConfig.NumInstance); i++ {
		wg.Add(1)
		q7TransInputParams[i].ParNum = uint8(i)
		go q7Trans.Invoke(client, &q7TransOutput[i], &wg, q7TransInputParams[i])
	}
}

func windowedAvg() {
	serdeFormat := getSerdeFormat()

	numAggInstance := uint8(5)
	groupByNodeConfig := &processor.ClientNodeConfig{
		FuncName:    "windowavggroupby",
		GatewayUrl:  FLAGS_faas_gateway,
		NumInstance: 1,
	}
	groupByInputParams := make([]*common.QueryInput, groupByNodeConfig.NumInstance)
	for i := 0; i < int(groupByNodeConfig.NumInstance); i++ {
		groupByInputParams[i] = &common.QueryInput{
			Duration:        uint32(FLAGS_duration),
			InputTopicName:  "nexmark_src",
			OutputTopicName: "grouped_bid",
			SerdeFormat:     uint8(serdeFormat),
			NumInPartition:  1,
			NumOutPartition: numAggInstance,
		}
	}
	bidGroupBy := processor.NewClientNode(groupByNodeConfig)

	avgNodeConfig := &processor.ClientNodeConfig{
		FuncName:    "windowavgagg",
		GatewayUrl:  FLAGS_faas_gateway,
		NumInstance: uint32(numAggInstance),
	}
	avgNodeInputParams := make([]*common.QueryInput, avgNodeConfig.NumInstance)
	for i := 0; i < int(avgNodeConfig.NumInstance); i++ {
		avgNodeInputParams[i] = &common.QueryInput{
			Duration:        uint32(FLAGS_duration),
			InputTopicName:  "grouped_bid",
			OutputTopicName: "windowed_avg",
			SerdeFormat:     uint8(serdeFormat),
			NumInPartition:  numAggInstance,
			NumOutPartition: numAggInstance,
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
	var sourceOutput common.FnOutput

	groupByOutput := make([]common.FnOutput, groupByNodeConfig.NumInstance)
	avgOutput := make([]common.FnOutput, avgNodeConfig.NumInstance)

	wg.Add(1)
	go invokeSourceFunc(client, &sourceOutput, &wg)

	for i := 0; i < int(groupByNodeConfig.NumInstance); i++ {
		wg.Add(1)
		groupByInputParams[i].ParNum = 0
		go bidGroupBy.Invoke(client, &groupByOutput[i], &wg, groupByInputParams[i])
	}

	for i := 0; i < int(avgNodeConfig.NumInstance); i++ {
		wg.Add(1)
		avgNodeInputParams[i].ParNum = uint8(i)
		go avg.Invoke(client, &avgOutput[i], &wg, avgNodeInputParams[i])
	}
	wg.Wait()
	if sourceOutput.Success {
		common.ProcessThroughputLat("source", sourceOutput.Latencies["e2e"], sourceOutput.Duration)
	}
	for i := 0; i < int(groupByNodeConfig.NumInstance); i++ {
		if groupByOutput[i].Success {
			common.ProcessThroughputLat(fmt.Sprintf("groupby %v", i), groupByOutput[i].Latencies["e2e"], groupByOutput[i].Duration)
		}
	}
	for i := 0; i < int(avgNodeConfig.NumInstance); i++ {
		if avgOutput[i].Success {
			common.ProcessThroughputLat(fmt.Sprintf("avg %v", i), avgOutput[i].Latencies["e2e"], avgOutput[i].Duration)
		}
	}
}

func main() {
	flag.StringVar(&FLAGS_faas_gateway, "faas_gateway", "127.0.0.1:8081", "")
	flag.StringVar(&FLAGS_app_name, "app_name", "query1", "")
	flag.StringVar(&FLAGS_stream_prefix, "stream_prefix", "nexmark", "")
	flag.IntVar(&FLAGS_duration, "duration", 60, "")
	flag.IntVar(&FLAGS_events_num, "events_num", 100000000, "events.num param for nexmark")
	flag.IntVar(&FLAGS_tps, "tps", 10000000, "tps param for nexmark")
	flag.StringVar(&FLAGS_serdeFormat, "serde", "json", "serde format: json or msgp")
	flag.Parse()

	switch FLAGS_app_name {
	case "windowedAvg":
		windowedAvg()
	case "q5":
		query5()
	case "q7":
		query7()
	default:
		generalQuery()
	}
}
