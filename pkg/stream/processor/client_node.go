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
	config   *ClientNodeConfig
	children []*ClientNode
}

type ClientNodeConfig struct {
	GatewayUrl  string
	FuncName    string
	NumInstance uint32
}

type InvokeParam struct {
	InputTopicName  string
	OutputTopicName string
	Duration        uint32 // in sec
	NumInPartition  uint16
	NumOutPartition uint16
	ParNum          uint16
	SerdeFormat     uint8
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

func (n *ClientNode) Invoke(client *http.Client, response *common.FnOutput, wg *sync.WaitGroup, queryInput *common.QueryInput) {
	defer wg.Done()

	url := utils.BuildFunctionUrl(n.config.GatewayUrl, n.config.FuncName)
	fmt.Fprintf(os.Stderr, "func url is %s\n", url)
	i := 0
	for i < common.ClientRetryTimes {
		if i != 0 {
			// remove the test error on retry
			if queryInput.TestParams != nil {
				queryInput.TestParams = nil
			}
		}
		if err := utils.JsonPostRequest(client, url, queryInput, response); err != nil {
			log.Error().Msgf("%v request failed: %v", n.config.FuncName, err)
		} else if !response.Success {
			log.Error().Msgf("%v request failed with failed message: %v", n.config.FuncName, response.Message)
		} else {
			break
		}
		i += 1
	}
}
