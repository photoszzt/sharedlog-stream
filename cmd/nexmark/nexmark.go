package main

import (
	"fmt"
	"os"

	"cs.utexas.edu/zhitingz/sharedlog-stream/pkg/nexmark/handlers"

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
		zerolog.SetGlobalLevel(zerolog.WarnLevel)
	}

	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
}

func (f *funcHandlerFactory) New(env types.Environment, funcName string) (types.FuncHandler, error) {
	switch funcName {
	case "source":
		return handlers.NewNexmarkSource(env), nil
	case "query1":
		return handlers.NewQuery1(env), nil
	case "query2":
		return handlers.NewQuery2(env), nil
	case "query3":
		return handlers.NewQuery3(env), nil
	case "query4":
		return handlers.NewQuery4(env), nil
	case "query5":
		return handlers.NewQuery5(env), nil
	case "query6":
		return handlers.NewQuery6(env), nil
	case "query7":
		return handlers.NewQuery7(env), nil
	case "query8":
		return handlers.NewQuery8(env), nil
	default:
		return nil, fmt.Errorf("Unknown function name %v", funcName)
	}
}

func (f *funcHandlerFactory) GrpcNew(env types.Environment, service string) (types.GrpcFuncHandler, error) {
	return nil, fmt.Errorf("Not implemented")
}

func main() {
	faas.Serve(&funcHandlerFactory{})
}
