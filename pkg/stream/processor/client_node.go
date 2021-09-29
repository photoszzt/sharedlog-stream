package processor

import (
	"fmt"
	"net/http"
	"os"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

// ClientNode is used to construct the dag at the client; Each node is responsible to invoke the
// corresponding serverless function identified by the name
type ClientNode struct {
	children []*ClientNode
	client   *http.Client
	config   *ClientNodeConfig
}

type ClientNodeConfig struct {
	GatewayUrl      string
	FuncName        string
	NumInstance     uint32
	Duration        uint32 // in sec
	InputTopicName  string
	OutputTopicName string
}

func NewClientNode(config *ClientNodeConfig) *ClientNode {
	client := &http.Client{
		Transport: &http.Transport{
			IdleConnTimeout: 30 * time.Second,
		},
		Timeout: time.Duration(config.Duration*3) * time.Second,
	}
	return &ClientNode{
		config: config,
		client: client,
	}
}

func (n *ClientNode) Name() string {
	return n.config.FuncName
}

func (n *ClientNode) AddChild(node *ClientNode) {
	n.children = append(n.children, node)
}

func (n *ClientNode) Invoke() {
	var wg sync.WaitGroup
	var queryOutput common.FnOutput
	wg.Add(1)
	go n.invokeFunc(n.client, &queryOutput, &wg)
	wg.Wait()
}

func (n *ClientNode) invokeFunc(client *http.Client, response *common.FnOutput, wg *sync.WaitGroup) {
	defer wg.Done()
	queryInput := &common.QueryInput{
		Duration:        n.config.Duration,
		InputTopicName:  n.config.InputTopicName,
		OutputTopicName: n.config.OutputTopicName,
	}
	url := utils.BuildFunctionUrl(n.config.GatewayUrl, n.config.FuncName)
	fmt.Fprintf(os.Stderr, "func url is %s\n", url)
	if err := utils.JsonPostRequest(client, url, queryInput, response); err != nil {
		log.Error().Msgf("%v request failed: %v", n.config.FuncName, err)
	} else if !response.Success {
		log.Error().Msgf("%v request failed: %v", n.config.FuncName, err)
	}
}
