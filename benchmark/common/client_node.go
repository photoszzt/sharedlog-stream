package common

import (
	"fmt"
	"net/http"
	"os"
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
	GatewayUrl     string
	FuncName       string
	NodeConstraint string
	NumInstance    uint8
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

func (n *ClientNode) Invoke(client *http.Client, response *FnOutput, wg *sync.WaitGroup,
	queryInput *QueryInput,
) {
	defer wg.Done()

	fmt.Fprintf(os.Stderr, "func name is %v\n", n.config.FuncName)
	url := BuildFunctionUrl(n.config.GatewayUrl, n.config.FuncName)
	fmt.Fprintf(os.Stderr, "func url is %s\n", url)

	if err := JsonPostRequest(client, url, n.config.NodeConstraint, queryInput, response); err != nil {
		log.Error().Msgf("%v request failed: %v", n.config.FuncName, err)
	} else if !response.Success {
		log.Error().Msgf("%v request failed with failed message: %v", n.config.FuncName, response.Message)
	}
	fmt.Fprintf(os.Stderr, "%s-%d call done\n", n.config.FuncName, queryInput.ParNum)
	/*
		i := 0
		startTime := time.Now()
		for {
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
				if response.Success && response.Message != "" {
					if time.Since(startTime) >= time.Duration(queryInput.Duration)*time.Second {
						break
					}
					continue
				}
				break
			}
			i += 1
		}
	*/
}
