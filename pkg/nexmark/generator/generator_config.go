package generator

import (
	"cs.utexas.edu/zhitingz/sharedlog-stream/pkg/nexmark"
)

const (
	FIRST_AUCTION_ID  uint64 = 1000
	FIRST_PERSON_ID   uint64 = 1000
	FIRST_CATEGORY_ID uint64 = 10
)

type GeneratorConfig struct {
	PersonProportion  uint32
	AuctionProportion uint32
	BidProportion     uint32
	TotalProportion   uint32
	Configuration     *nexmark.NexMarkConfig
	InterEventDelayUs []float64
	StepLengthSec     uint64
	BaseTime          uint64
	FirstEventId      uint64
	MaxEvents         uint64
	FirstEventNumber  uint64
	EpochPeriodMs     uint64
	EventPerEpoch     uint64
}

func NewGeneratorConfig(configuration *nexmark.NexMarkConfig, baseTime uint64, firstEventId uint64,
	maxEventsOrZero uint64, firstEventNumber uint64) *GeneratorConfig {
	g := new(GeneratorConfig)
	g.AuctionProportion = configuration.AuctionProportion
	g.PersonProportion = configuration.PersonProportion
	g.BidProportion = configuration.BidProportion
	g.TotalProportion = configuration.AuctionProportion + configuration.PersonProportion + configuration.BidProportion
	g.Configuration = configuration
	g.InterEventDelayUs = []float64{1000000.0 / float64(configuration.FirstEventRate) * float64(configuration.NumEventGenerators)}
	g.StepLengthSec = uint64(configuration.RateShape.StepLengthSec(configuration.RatePeriodSec))
	return g
}
