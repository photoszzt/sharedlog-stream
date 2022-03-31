package common

import (
	"net/http"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"

	"github.com/rs/zerolog/log"
)

func InvokeConfigScale(client *http.Client, csi *ConfigScaleInput,
	faas_gateway string, response *FnOutput, appName string,
) {
	url := utils.BuildFunctionUrl(faas_gateway, appName)
	if err := utils.JsonPostRequest(client, url, csi, response); err != nil {
		log.Error().Msgf("%s request failed: %v", appName, err)
	} else if !response.Success {
		log.Error().Msgf("%s request failed: %s", appName, response.Message)
	}
}
