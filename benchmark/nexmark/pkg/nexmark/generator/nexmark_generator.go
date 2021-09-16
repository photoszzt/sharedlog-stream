package generator

import (
	"context"
	"math/rand"
	"time"

	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"
)

type NexmarkGenerator struct {
	Random            *rand.Rand
	Config            *GeneratorConfig
	EventsCountSoFar  uint64
	WallclockBaseTime int64
}

type NextEvent struct {
	WallclockTimestamp uint64
	EventTimestamp     uint64
	Event              *types.Event
	Watermark          uint64
}

func NewNextEvent(wallclockTimestamp, eventTimestamp uint64, event *types.Event, watermark uint64) *NextEvent {
	return &NextEvent{
		WallclockTimestamp: wallclockTimestamp,
		EventTimestamp:     eventTimestamp,
		Event:              event,
		Watermark:          watermark,
	}
}

func NewNexmarkGenerator(config *GeneratorConfig, eventsCountSoFar uint64, wallclockBaseTime int64) *NexmarkGenerator {
	return &NexmarkGenerator{
		Config:            config,
		EventsCountSoFar:  eventsCountSoFar,
		WallclockBaseTime: wallclockBaseTime,
		Random:            rand.New(rand.NewSource(3)),
	}
}

func (ng *NexmarkGenerator) copy() *NexmarkGenerator {
	return NewNexmarkGenerator(ng.Config, ng.EventsCountSoFar, ng.WallclockBaseTime)
}

func NewSimpleNexmarkGenerator(config *GeneratorConfig) *NexmarkGenerator {
	return NewNexmarkGenerator(config, 0, -1)
}

func (ng *NexmarkGenerator) SplitAtEventId(eventId uint64) *GeneratorConfig {
	newMaxEvents := eventId - (ng.Config.FirstEventId + ng.Config.FirstEventNumber)
	remainConfig := ng.Config.CopyWith(ng.Config.FirstEventId,
		ng.Config.MaxEvents-newMaxEvents, ng.Config.FirstEventNumber+newMaxEvents)
	ng.Config = ng.Config.CopyWith(ng.Config.FirstEventId, newMaxEvents, ng.Config.FirstEventNumber)
	return remainConfig
}

func (ng *NexmarkGenerator) GetNextEventId() uint64 {
	return ng.Config.FirstEventId + ng.Config.NextAdjustedEventNumber(ng.EventsCountSoFar)
}

func (ng *NexmarkGenerator) HasNext() bool {
	return ng.EventsCountSoFar < ng.Config.MaxEvents
}

func (ng *NexmarkGenerator) NextEvent(ctx context.Context, bidUrlCache map[uint32]*ChannelUrl) (*NextEvent, error) {
	if ng.WallclockBaseTime < 0 {
		ng.WallclockBaseTime = time.Now().Unix() * 1000
	}

	eventTimestamp := ng.Config.TimestampForEvent(ng.Config.NextEventNumber(ng.EventsCountSoFar))
	adjustedEventTimestamp := ng.Config.TimestampForEvent(ng.Config.NextAdjustedEventNumber(ng.EventsCountSoFar))
	watermark := ng.Config.TimestampForEvent(ng.Config.NextEventNumberForWatermark(ng.EventsCountSoFar))
	wallclockTimestamp := ng.WallclockBaseTime + (int64(eventTimestamp) - int64(ng.Config.BaseTime))
	newEventId := ng.GetNextEventId()
	rem := newEventId % uint64(ng.Config.TotalProportion)
	var event *types.Event
	if rem < uint64(ng.Config.PersonProportion) {
		event = types.NewPersonEvent(
			NextPerson(newEventId, ng.Random, adjustedEventTimestamp, ng.Config))
	} else if rem < uint64(ng.Config.PersonProportion)+uint64(ng.Config.AuctionProportion) {
		event = types.NewAuctionEvnet(NextAuction(ng.EventsCountSoFar, newEventId, ng.Random, adjustedEventTimestamp, ng.Config))
	} else {
		bidEvent, err := NextBid(ctx, newEventId, ng.Random, adjustedEventTimestamp, ng.Config, bidUrlCache)
		if err != nil {
			return nil, err
		}
		event = types.NewBidEvent(bidEvent)
	}
	ng.EventsCountSoFar += 1
	return NewNextEvent(uint64(wallclockTimestamp), adjustedEventTimestamp, event, watermark), nil
}
