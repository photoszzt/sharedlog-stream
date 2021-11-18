package main

import (
	"fmt"
	"os"
	"sharedlog-stream/benchmark/dspbench/pkg/handlers/append_read"
	"sharedlog-stream/benchmark/dspbench/pkg/handlers/spike_detection"
	"sharedlog-stream/benchmark/dspbench/pkg/handlers/wordcount"

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
	case "spikedetection":
		return spike_detection.NewSpikeDetectionHandler(env), nil
	case "sdsource":
		return spike_detection.NewSpikeDetectionSource(env), nil
	case "wcsource":
		return wordcount.NewWordCountSource(env), nil
	case "wordcountsplit":
		return wordcount.NewWordCountSplitter(env), nil
	case "wordcountcounter":
		return wordcount.NewWordCountCounterAgg(env), nil
	case "streamAppend":
		return append_read.NewAppendHandler(env), nil
	case "streamRead":
		return append_read.NewReadHandler(env), nil
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
