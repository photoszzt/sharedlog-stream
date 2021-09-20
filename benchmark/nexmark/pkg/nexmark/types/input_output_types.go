package types

import (
	"time"

	"sharedlog-stream/benchmark/nexmark/pkg/nexmark"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/stream/processor"
)

type QueryInput struct {
	Duration        uint32 `json:"duration"`
	InputTopicName  string `json:"input_topic_name"`
	OutputTopicName string `json:"output_topic_name"`
	SerdeFormat     uint8  `json:"serde_format"`
}

type NexMarkConfigInput struct {
	TopicName              string        `json:"topic_name"`
	Duration               uint32        `json:"duration"`
	RateShape              string        `json:"rate_shape"`
	RatePeriod             time.Duration `json:"rate_period"`
	RateLimited            bool          `json:"rate_limited"`
	FirstEventRate         uint32        `json:"first_event_rate"`
	NextEventRate          uint32        `json:"next_event_rate"`
	PersonAvgSize          uint32        `json:"person_avg_size"`  // in bytes
	AuctionAvgSize         uint32        `json:"auction_avg_size"` // in bytes
	BidAvgSize             uint32        `json:"bid_avg_size"`     // in bytes
	PersonProportion       uint32        `json:"person_proportion"`
	AuctionProportion      uint32        `json:"auction_proportion"`
	BidProportion          uint32        `json:"bid_proportion"`
	BidHotRatioAuctions    uint32        `json:"bid_hot_ratio_auctions"`
	BidHotRatioBidders     uint32        `json:"bid_hot_ratio_bidders"`
	AuctionHotRatioSellers uint32        `json:"auction_hot_ratio_sellers"`
	EventsNum              uint64        `json:"events_num"`
	SerdeFormat            uint8         `json:"serde_format"`
}

func NewNexMarkConfigInput(topicName string, serdeFormat processor.SerdeFormat) *NexMarkConfigInput {
	return &NexMarkConfigInput{
		TopicName:              topicName,
		Duration:               0,
		RateShape:              "square",
		RatePeriod:             time.Duration(600) * time.Second,
		RateLimited:            false,
		FirstEventRate:         10000,
		NextEventRate:          10000,
		PersonAvgSize:          200,
		AuctionAvgSize:         500,
		BidAvgSize:             100,
		PersonProportion:       1,
		AuctionProportion:      3,
		BidProportion:          46,
		BidHotRatioAuctions:    2,
		BidHotRatioBidders:     4,
		AuctionHotRatioSellers: 4,
		EventsNum:              0,
		SerdeFormat:            uint8(serdeFormat),
	}
}

func ConvertToNexmarkConfiguration(config *NexMarkConfigInput) (*nexmark.NexMarkConfig, error) {
	rateUnit, err := utils.StrToRateShape(config.RateShape)
	if err != nil {
		return nil, err
	}
	nexmarkConfig := nexmark.NewNexMarkConfig()
	nexmarkConfig.RateShape = rateUnit
	nexmarkConfig.RatePeriodSec = uint32(config.RatePeriod.Seconds())
	nexmarkConfig.IsRateLimited = config.RateLimited
	nexmarkConfig.FirstEventRate = uint64(config.FirstEventRate)
	nexmarkConfig.NextEventRate = uint64(config.NextEventRate)
	nexmarkConfig.AvgPersonByteSize = config.PersonAvgSize
	nexmarkConfig.AvgAuctionByteSize = config.AuctionAvgSize
	nexmarkConfig.AvgBidByteSize = config.BidAvgSize
	nexmarkConfig.PersonProportion = config.PersonProportion
	nexmarkConfig.AuctionProportion = config.AuctionProportion
	nexmarkConfig.BidProportion = config.BidProportion
	nexmarkConfig.HotAuctionRatio = config.BidHotRatioAuctions
	nexmarkConfig.HotBiddersRatio = config.BidHotRatioBidders
	nexmarkConfig.HotSellersRatio = config.AuctionHotRatioSellers
	nexmarkConfig.NumEvents = uint32(config.EventsNum)
	return nexmarkConfig, nil
}

type FnOutput struct {
	Success   bool    `json:"success"`
	Message   string  `json:"message"`
	Duration  float64 `json:"duration"`
	Latencies []int   `json:"latencies"`
}
