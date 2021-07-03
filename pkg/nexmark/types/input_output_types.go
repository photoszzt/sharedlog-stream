package types

import (
	"time"

	"cs.utexas.edu/zhitingz/sharedlog-stream/pkg/nexmark"
	"cs.utexas.edu/zhitingz/sharedlog-stream/pkg/nexmark/utils"
)

type NexMarkConfigInput struct {
	TopicName              string        `json:"topic_name"`
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
}

func NewNexMarkConfigInput(topicName string) *NexMarkConfigInput {
	return &NexMarkConfigInput{
		TopicName:              topicName,
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
	}
}

func ConvertToNexmarkConfiguration(config *NexMarkConfigInput) (*nexmark.NexMarkConfig, error) {
	rateUnit, err := utils.StrToRateShape(config.RateShape)
	if err != nil {
		return nil, err
	}
	return &nexmark.NexMarkConfig{
		RateShape:          rateUnit,
		RatePeriodSec:      uint32(config.RatePeriod.Seconds()),
		IsRateLimited:      config.RateLimited,
		FirstEventRate:     uint64(config.FirstEventRate),
		NextEventRate:      uint64(config.NextEventRate),
		AvgPersonByteSize:  config.PersonAvgSize,
		AvgAuctionByteSize: config.AuctionAvgSize,
		AvgBidByteSize:     config.BidAvgSize,
		PersonProportion:   config.PersonProportion,
		AuctionProportion:  config.AuctionProportion,
		BidProportion:      config.BidProportion,
		HotAuctionRatio:    config.BidHotRatioAuctions,
		HotBiddersRatio:    config.BidHotRatioBidders,
		HotSellersRatio:    config.AuctionHotRatioSellers,
		NumEvents:          uint32(config.EventsNum),
	}, nil
}

type FnOutput struct {
	Success   bool    `json:"success"`
	Message   string  `json:"message"`
	Duration  float64 `json:"duration"`
	Latencies []int   `json:"latencies"`
}
