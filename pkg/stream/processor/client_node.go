package processor

import (
	"fmt"
	"net/http"
	"os"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sync"

	"github.com/rs/zerolog/log"
)

// ClientNode is used to construct the dag at the client; Each node is responsible to invoke the
// corresponding serverless function identified by the name
type ClientNode struct {
	children []*ClientNode
	config   *ClientNodeConfig
}

type ClientNodeConfig struct {
	GatewayUrl  string
	FuncName    string
	NumInstance uint32
}

type InvokeParam struct {
	Duration        uint32 // in sec
	InputTopicName  string
	OutputTopicName string
	SerdeFormat     uint8
	NumInPartition  uint16
	NumOutPartition uint16
	ParNum          uint16
}

func NewClientNode(config *ClientNodeConfig) *ClientNode {
	return &ClientNode{
		config: config,
	}
}

func (n *ClientNode) Name() string {
	return n.config.FuncName
}

func (n *ClientNode) AddChild(node *ClientNode) {
	n.children = append(n.children, node)
}

func (n *ClientNode) Invoke(client *http.Client, response *common.FnOutput, wg *sync.WaitGroup, param *InvokeParam) {
	defer wg.Done()
	queryInput := &common.QueryInput{
		Duration:        param.Duration,
		InputTopicName:  param.InputTopicName,
		OutputTopicName: param.OutputTopicName,
		SerdeFormat:     param.SerdeFormat,
	}
	url := utils.BuildFunctionUrl(n.config.GatewayUrl, n.config.FuncName)
	fmt.Fprintf(os.Stderr, "func url is %s\n", url)
	if err := utils.JsonPostRequest(client, url, queryInput, response); err != nil {
		log.Error().Msgf("%v request failed: %v", n.config.FuncName, err)
	} else if !response.Success {
		log.Error().Msgf("%v request failed: %v", n.config.FuncName, err)
	}
}
