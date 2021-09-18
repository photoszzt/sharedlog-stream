package trendingtopicshandler

import (
	"fmt"
	"os"
	"sharedlog-stream/benchmark/dspbench/pkg/handlers/spike_detection"
	"sharedlog-stream/benchmark/dspbench/pkg/handlers/trending_topics"

	"cs.utexas.edu/zjia/faas"
	"cs.utexas.edu/zjia/faas/types"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type funcHandlerFactory struct {
}

func init() {
	logLevel := os.Getenv("LOG_LEVEL")
	if level, err := zerolog.ParseLevel(logLevel); err == nil {
		zerolog.SetGlobalLevel(level)
	} else {
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}

	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
}

func (f *funcHandlerFactory) New(env types.Environment, funcName string) (types.FuncHandler, error) {
	switch funcName {
	case "tt-source":
		return trending_topics.NewTrendingTopicsSource(env), nil
	case "trending-topics":
		return trending_topics.NewTrendingTopics(env), nil
	case "spike-detection":
		return spike_detection.NewSpikeDetectionHandler(env), nil
	default:
		return nil, fmt.Errorf("unknown function name %v", funcName)
	}
}

func (f *funcHandlerFactory) GrpcNew(env types.Environment, service string) (types.GrpcFuncHandler, error) {
	return nil, fmt.Errorf("not implemented")
}

func main() {
	faas.Serve(&funcHandlerFactory{})
}
