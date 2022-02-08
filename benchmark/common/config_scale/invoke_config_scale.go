package configscale

import (
	"net/http"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sync"

	"github.com/rs/zerolog/log"
)

func InvokeConfigScale(client *http.Client, config map[string]uint8,
	appId string, faas_gateway string,
	serdeFormat uint8, response *common.FnOutput, wg *sync.WaitGroup,
) {
	defer wg.Done()
	url := utils.BuildFunctionUrl(faas_gateway, "source")
	csi := &common.ConfigScaleInput{
		Config:      config,
		AppId:       appId,
		SerdeFormat: serdeFormat,
	}
	if err := utils.JsonPostRequest(client, url, csi, response); err != nil {
		log.Error().Msgf("source request failed: %v", err)
	} else if !response.Success {
		log.Error().Msgf("source request failed: %s", response.Message)
	}
}
