package main

import (
	"fmt"
	"os"

	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/handlers"

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
	// fmt.Fprint(os.Stderr, "Enter nexmark\n")
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
	case "q5bidkeyedbyauction":
		return handlers.NewBidKeyedByAuctionHandler(env), nil
	case "q5aucbids":
		return handlers.NewQ5AuctionBids(env), nil
	case "q5maxbid":
		return handlers.NewQ5MaxBid(env), nil
	case "query6":
		return handlers.NewQuery6(env), nil
	case "query7":
		return handlers.NewQuery7(env), nil
	case "q7bidkeyedbyprice":
		return handlers.NewQ7BidKeyedByPriceHandler(env), nil
	case "query8":
		return handlers.NewQuery8(env), nil
	case "windowavggroupby":
		return handlers.NewWindowAvgGroupByHandler(env), nil
	case "windowavgagg":
		return handlers.NewWindowedAvg(env), nil
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
