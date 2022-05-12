package main

import (
	"fmt"
	"os"

	configscale "sharedlog-stream/benchmark/common/config_scale"
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
	fmt.Fprintf(os.Stderr, "Enter nexmark: funcName: %v\n", funcName)
	switch funcName {
	case "source":
		return handlers.NewNexmarkSource(env, funcName), nil
	case "query1":
		return handlers.NewQuery1(env, funcName), nil
	case "query2":
		return handlers.NewQuery2(env, funcName), nil
	case "q3JoinTable":
		return handlers.NewQ3JoinTableHandler(env, funcName), nil
	case "q3GroupBy":
		return handlers.NewQ3GroupByHandler(env, funcName), nil
	case "auctionsByID":
		return handlers.NewAuctionsByIDHandler(env, funcName), nil
	case "bidsByAuctionID":
		return handlers.NewBidByAuctionIDHandler(env, funcName), nil
	case "q4JoinTable":
		return handlers.NewQ4JoinTableHandler(env, funcName), nil
	case "q5bidkeyedbyauction":
		return handlers.NewBidKeyedByAuctionHandler(env, funcName), nil
	case "q5aucbids":
		return handlers.NewQ5AuctionBids(env, funcName), nil
	case "q5maxbid":
		return handlers.NewQ5MaxBid(env, funcName), nil
	case "query6":
		return handlers.NewQuery6(env), nil
	case "query7":
		return handlers.NewQuery7(env, funcName), nil
	case "q7bidskeyedbyprice":
		return handlers.NewQ7BidKeyedByPriceHandler(env, funcName), nil
	case "q8JoinStream":
		return handlers.NewQ8JoinStreamHandler(env, funcName), nil
	case "q8GroupBy":
		return handlers.NewQ8GroupByHandler(env, funcName), nil
	case "windowavggroupby":
		return handlers.NewWindowAvgGroupByHandler(env), nil
	case "windowavgagg":
		return handlers.NewWindowedAvg(env), nil
	case "scale":
		return configscale.NewConfigScaleHandler(env), nil
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
