package generator

import (
	"math/rand"
	"time"

	"cs.utexas.edu/zhitingz/sharedlog-stream/pkg/nexmark/types"
	"cs.utexas.edu/zhitingz/sharedlog-stream/pkg/utils"
)

const (
	NUM_CATEGORIES   uint32 = 5
	AUCTION_ID_LEAD  uint32 = 10
	HOT_SELLER_RATIO uint32 = 100
)

func NextAuction(eventsCountSoFar uint64,
	eventId uint64, random *rand.Rand, timestamp uint64,
	config *GeneratorConfig) *types.Auction {
	id := LastBase0AuctionId(config, eventId) + FIRST_AUCTION_ID
	seller := uint64(0)
	if random.Intn(int(config.Configuration.HotSellersRatio)) > 0 {
		seller = (LastBase0PersonId(config, eventId) / uint64(HOT_SELLER_RATIO)) * uint64(HOT_SELLER_RATIO)
	} else {
		seller = NextBase0PersonId(eventId, random, config)
	}
	seller += FIRST_PERSON_ID
	category := FIRST_CATEGORY_ID + uint64(random.Intn(int(NUM_CATEGORIES)))
	initialBid := NextPrice(random)
	expires := timestamp + nextAuctionLenghMs(eventsCountSoFar, random, timestamp, config)
	name := NextString(random, 20)
	desc := NextString(random, 100)
	reserve := initialBid + NextPrice(random)
	currentSize := 8 + len(name) + len(desc) + 8 + 8 + 8 + 8 + 8
	extra := NextExtra(random, uint32(currentSize), config.Configuration.AvgAuctionByteSize)
	return &types.Auction{
		ID:          id,
		ItemName:    name,
		Description: desc,
		InitialBid:  initialBid,
		Reserve:     reserve,
		DateTime:    time.Unix(int64(timestamp/1000.0), 0),
		Expires:     time.Unix(int64(expires/1000.0), 0),
		Seller:      seller,
		Category:    category,
		Extra:       extra,
	}
}

func LastBase0AuctionId(config *GeneratorConfig, eventId uint64) uint64 {
	epoch := eventId / uint64(config.TotalProportion)
	offset := eventId % uint64(config.TotalProportion)
	if offset < uint64(config.PersonProportion) {
		epoch -= 1
		offset = uint64(config.AuctionProportion - 1)
	} else if offset >= uint64(config.PersonProportion+config.AuctionProportion) {
		offset = uint64(config.AuctionProportion) - 1
	} else {
		offset -= uint64(config.PersonProportion)
	}
	return epoch*uint64(config.AuctionProportion) + offset
}

func NextBase0AuctionId(nextEventId uint64, random *rand.Rand, config *GeneratorConfig) uint64 {
	minAuction := utils.MaxUint64(LastBase0AuctionId(config, nextEventId)-uint64(config.Configuration.NumInFlightAuctions), 0)
	maxAuction := LastBase0AuctionId(config, nextEventId)
	return minAuction + NextUint64(random, maxAuction-minAuction+1+uint64(AUCTION_ID_LEAD))
}

func nextAuctionLenghMs(eventsCountSoFar uint64, random *rand.Rand, timestamp uint64, config *GeneratorConfig) uint64 {
	currentEventNumber := config.NextAdjustedEventNumber(eventsCountSoFar)
	numEventsForAuctions := uint64(config.Configuration.NumInFlightAuctions) * uint64(config.TotalProportion) / uint64(config.AuctionProportion)
	futureAuction := config.TimestampForEvent(currentEventNumber + numEventsForAuctions)
	horizonMs := futureAuction - timestamp
	return 1 + NextUint64(random, utils.MaxUint64(horizonMs*2, 1))
}
