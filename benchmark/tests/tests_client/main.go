package main

import (
	"flag"
	"net/http"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/benchmark/tests/pkg/tests/test_types"
	"time"

	"github.com/rs/zerolog/log"
)

var (
	FLAGS_faas_gateway string
	FLAGS_test_app     string
)

func invokeTest(client *http.Client,
	testName string,
	topicName string,
	response *common.FnOutput,
) {
	ti := &test_types.TestInput{
		TestName:  testName,
		TopicName: topicName,
	}
	url := utils.BuildFunctionUrl(FLAGS_faas_gateway, FLAGS_test_app)
	if err := utils.JsonPostRequest(client, url, "", ti, response); err != nil {
		log.Error().Msgf("%s request failed: %v", testName, err)
	} else if !response.Success {
		log.Error().Msgf("%s request failed: %s", testName, response.Message)
	}
}

func main() {
	flag.StringVar(&FLAGS_faas_gateway, "faas_gateway", "127.0.0.1:8081", "")
	flag.StringVar(&FLAGS_test_app, "test_app", "wintest", "")
	flag.Parse()

	client := &http.Client{
		Transport: &http.Transport{
			IdleConnTimeout: 30 * time.Second,
		},
		Timeout: time.Duration(30) * time.Second,
	}
	switch FLAGS_test_app {
	case "wintest":
		response := common.FnOutput{}
		invokeTest(client, "TestGetAndRange", "TestGetAndRange", &response)
		response = common.FnOutput{}
		invokeTest(client, "TestShouldGetAllNonDeletedMsgs", "TestShouldGetAllNonDeletedMsgs", &response)
		response = common.FnOutput{}
		invokeTest(client, "TestExpiration", "TestExpiration", &response)
		response = common.FnOutput{}
		invokeTest(client, "TestShouldGetAll", "TestShouldGetAll", &response)
		response = common.FnOutput{}
		invokeTest(client, "TestShouldGetAllReturnTimestampOrdered", "TestShouldGetAllReturnTimestampOrdered", &response)
		response = common.FnOutput{}
		invokeTest(client, "TestFetchRange", "TestFetchRange", &response)
		response = common.FnOutput{}
		invokeTest(client, "TestPutAndFetchBefore", "TestPutAndFetchBefore", &response)
		response = common.FnOutput{}
		invokeTest(client, "TestPutAndFetchAfter", "TestPutAndFetchAfter", &response)
		response = common.FnOutput{}
		invokeTest(client, "TestPutSameKeyTs", "TestPutSameKeyTs", &response)
	case "restore":
		response := common.FnOutput{}
		invokeTest(client, "restoreKV", "restoreKV", &response)
		response = common.FnOutput{}
		invokeTest(client, "restoreWin", "restoreWin", &response)
	case "join":
		response := common.FnOutput{}
		invokeTest(client, "streamStreamJoinMem", "streamStreamJoinMem", &response)
		response = common.FnOutput{}
		invokeTest(client, "streamStreamJoinMongo", "streamStreamJoinMongo", &response)
	case "prodConsume":
		response := common.FnOutput{}
		invokeTest(client, "multiProducer2pc", "multiProducer2pc", &response)
		response = common.FnOutput{}
		invokeTest(client, "singleProducerEpoch", "singleProducerEpoch", &response)
	}
}
