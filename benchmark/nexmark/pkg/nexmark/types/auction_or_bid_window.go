package types

import (
	"sharedlog-stream/pkg/stream/processor/commtypes"
)

type AuctionOrBidWindow struct {
	base            commtypes.BaseWindow
	auctionId       uint64
	isAuctionWindow bool
}

func NewAuctionOrBidWindow(startMs int64,
	endMs int64,
	auctionId uint64,
	isAuctionWindow bool,
) *AuctionOrBidWindow {
	return &AuctionOrBidWindow{
		base:            commtypes.NewBaseWindow(startMs, endMs),
		auctionId:       auctionId,
		isAuctionWindow: isAuctionWindow,
	}
}

func NewAuctionWindow(ts int64, auction *Auction) *AuctionOrBidWindow {
	return NewAuctionOrBidWindow(ts, auction.Expires, auction.ID, true)
}

func NewBidWindow(expectedAuctionDurationMs int64, ts int64, bid *Bid) *AuctionOrBidWindow {
	return NewAuctionOrBidWindow(ts, ts+expectedAuctionDurationMs*2, bid.Auction, false)
}
