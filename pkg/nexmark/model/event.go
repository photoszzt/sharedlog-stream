package model

type EType uint8

const (
	PERSON  EType = 0
	AUCTION EType = 1
	BID     EType = 2
)

type Event struct {
	newPerson  *Person
	newAuction *Auction
	bid        *Bid
	etype      EType
}

func NewPersonEvent(newPerson *Person) *Event {
	return &Event{
		newPerson:  newPerson,
		newAuction: nil,
		bid:        nil,
		etype:      PERSON,
	}
}

func NewAuctionEvnet(newAuction *Auction) *Event {
	return &Event{
		newPerson:  nil,
		newAuction: newAuction,
		bid:        nil,
		etype:      AUCTION,
	}
}

func NewBidEvent(bid *Bid) *Event {
	return &Event{
		newPerson:  nil,
		newAuction: nil,
		bid:        bid,
		etype:      BID,
	}
}
