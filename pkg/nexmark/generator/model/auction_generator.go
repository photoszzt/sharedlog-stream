package model

import (
	"math/rand"

	"cs.utexas.edu/zhitingz/sharedlog-stream/pkg/nexmark/generator"
)

const (
	NUM_CATEGORIES   uint32 = 5
	AUCTION_ID_LEAD  uint32 = 10
	HOT_SELLER_RATIO uint32 = 100
)

func NextAuction(eventsCountSoFar uint64,
	eventId uint64, random rand.Rand, timestamp uint64,
	config generator.GeneratorConfig) {

}

func lastBase0AuctionId(config generator.GeneratorConfig, eventId uint64) uint64 {
	epoch := eventId / uint64(config.TotalProportion)
	offset := eventId % uint64(config.TotalProportion)
	if offset < uint64(config.PersonProportion) {
		epoch -= 1
		offset = uint64(config.AuctionProportion) - 1
	} else if offset >= uint64(config.PersonProportion+config.AuctionProportion) {
		offset = uint64(config.AuctionProportion) - 1
	} else {
		offset -= uint64(config.PersonProportion)
	}
	return epoch*uint64(config.AuctionProportion) + offset
}

// func nextBase0AuctionId(nextEventId uint64, random rand.Rand, config generator.GeneratorConfig) uint64 {
// 	minAuction := math.Max(lastBase0AuctionId(config, nextEventId)-config.getNumInFlightAuctions(), 0)
// 	maxAuction := lastBase0AuctionId(config, nextEventId)
// 	return minAuction + NextUint64(random, maxAuction-minAuction+1+AUCTION_ID_LEAD)
// }
