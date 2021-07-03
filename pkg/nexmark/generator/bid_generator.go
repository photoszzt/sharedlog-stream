package generator

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"cs.utexas.edu/zhitingz/sharedlog-stream/pkg/nexmark/types"
)

const (
	HOT_AUCTION_RATIO  uint32 = 100
	HOT_BIDDER_RATIO   uint32 = 100
	HOT_CHANNELS_RATIO uint32 = 2
	CHANNELS_NUMBER    uint32 = 10_000
)

var (
	HOT_CHANNELS = [4]string{"Google", "Facebook", "Baidu", "Apple"}
	HOT_URLS     = [4]string{getBaseUrl(), getBaseUrl(), getBaseUrl(), getBaseUrl()}
)

type ChannelUrl struct {
	channel string
	url     string
}

func NextBid(ctx context.Context, eventId uint64, random *rand.Rand, timestamp uint64, config *GeneratorConfig) (*types.Bid, error) {
	auction := uint64(0)
	if random.Intn(int(config.Configuration.HotAuctionRatio)) > 0 {
		auction = LastBase0AuctionId(config, eventId) / uint64(HOT_AUCTION_RATIO) * uint64(HOT_AUCTION_RATIO)
	} else {
		auction = NextBase0AuctionId(eventId, random, config)
	}
	auction += FIRST_AUCTION_ID

	bidder := uint64(0)
	if random.Intn(int(config.Configuration.HotBiddersRatio)) > 0 {
		bidder = LastBase0PersonId(config, eventId) / uint64(HOT_AUCTION_RATIO) * uint64(HOT_AUCTION_RATIO)
	} else {
		bidder = NextBase0PersonId(eventId, random, config)
	}
	bidder += FIRST_PERSON_ID

	price := NextPrice(random)
	channel := ""
	url := ""
	if random.Intn(int(HOT_CHANNELS_RATIO)) > 0 {
		i := random.Intn(len(HOT_CHANNELS))
		channel = HOT_CHANNELS[i]
		url = HOT_URLS[i]
	} else {
		channelAndUrlMap, ok := ctx.Value("CHANNEL_URL_CACHE").(map[uint32]*ChannelUrl)
		if !ok {
			return nil, fmt.Errorf("fail to cast context value to map")
		}

		channelAndUrl := channelAndUrlMap[uint32(random.Intn(int(CHANNELS_NUMBER)))]
		channel = channelAndUrl.channel
		url = channelAndUrl.url
	}
	bidder += FIRST_PERSON_ID

	currentSize := 8 + 8 + 8 + 8
	extra := NextExtra(random, uint32(currentSize), config.Configuration.AvgBidByteSize)
	return &types.Bid{
		Auction:  auction,
		Bidder:   bidder,
		Price:    price,
		Channel:  channel,
		Url:      url,
		DateTime: time.Unix(int64(timestamp/1000.0), 0),
		Extra:    extra,
	}, nil
}

func getBaseUrl() string {
	random := rand.New(rand.NewSource(time.Now().UnixNano()))
	return "https://www.nexmark.com" +
		NextString(random, 5) + "/" +
		NextString(random, 5) + "/" +
		NextString(random, 5) + "/" +
		"item.htm?query=1"
}