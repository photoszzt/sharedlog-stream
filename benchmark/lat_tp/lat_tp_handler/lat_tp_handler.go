package main

import (
	"fmt"
	"os"
	"sharedlog-stream/benchmark/lat_tp/pkg/handlers"

	"cs.utexas.edu/zjia/faas"
	"cs.utexas.edu/zjia/faas/types"
	"github.com/rs/zerolog/log"

	"github.com/rs/zerolog"
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
	case "produce":
		return handlers.NewSharedlogProduceBenchHandler(env), nil
	case "consume":
		return handlers.NewSharedlogConsumeBenchHandler(env), nil
	case "tranDataGen":
		return handlers.NewSharedlogTranDataGenHandler(env), nil
	case "tranProcess":
		return handlers.NewSharedlogTranProcessHandler(env), nil
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
