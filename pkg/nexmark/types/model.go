//go:generate msgp
//msgp:ignore Event
package types

import (
	"fmt"
	"time"

	"github.com/tinylib/msgp/msgp"
)

type Auction struct {
	ID          uint64    `msg:"id"`
	ItemName    string    `msg:"itemName"`
	Description string    `msg:"description"`
	InitialBid  uint64    `msg:"initialBid"`
	Reserve     uint64    `msg:"reserve"`
	DateTime    time.Time `msg:"dataTime"`
	Expires     time.Time `msg:"expires"`
	Seller      uint64    `msg:"seller"`
	Category    uint64    `msg:"category"`
	Extra       string    `msg:"extra"`
}

type Bid struct {
	Auction  uint64    `msg:"auction"`
	Bidder   uint64    `msg:"bidder"`
	Price    uint64    `msg:"price"`
	Channel  string    `msg:"channel"`
	Url      string    `msg:"url"`
	DateTime time.Time `msg:"dateTime"`
	Extra    string    `msg:"extra"`
}

type Person struct {
	ID           uint64    `msg:"id"`
	Name         string    `msg:"name"`
	EmailAddress string    `msg:"emailAddress"`
	CreditCard   string    `msg:"creditCard"`
	City         string    `msg:"city"`
	State        string    `msg:"state"`
	DateTime     time.Time `msg:"dateTime"`
	Extra        string    `msg:"extra"`
}

type EType uint8

const (
	PERSON  EType = 0
	AUCTION EType = 1
	BID     EType = 2
)

type Event struct {
	NewPerson  *Person
	NewAuction *Auction
	Bid        *Bid
	Etype      EType
}

type EventSerialized struct {
	Etype uint8    `msg:"etype"`
	Body  msgp.Raw `msg:"body"`
}

func NewPersonEvent(newPerson *Person) *Event {
	return &Event{
		NewPerson:  newPerson,
		NewAuction: nil,
		Bid:        nil,
		Etype:      PERSON,
	}
}

func NewAuctionEvnet(newAuction *Auction) *Event {
	return &Event{
		NewPerson:  nil,
		NewAuction: newAuction,
		Bid:        nil,
		Etype:      AUCTION,
	}
}

func NewBidEvent(bid *Bid) *Event {
	return &Event{
		NewPerson:  nil,
		NewAuction: nil,
		Bid:        bid,
		Etype:      BID,
	}
}

func (e *Event) MarshalMsg(b []byte) ([]byte, error) {
	switch e.Etype {
	case PERSON:
		person_encoded, err := e.NewPerson.MarshalMsg(nil)
		if err != nil {
			return nil, err
		}
		es := &EventSerialized{
			Etype: uint8(e.Etype),
			Body:  person_encoded,
		}
		return es.MarshalMsg(nil)
	case AUCTION:
		auction_encoded, err := e.NewAuction.MarshalMsg(nil)
		if err != nil {
			return nil, err
		}
		es := &EventSerialized{
			Etype: uint8(e.Etype),
			Body:  auction_encoded,
		}
		return es.MarshalMsg(nil)
	case BID:
		bid_encoded, err := e.Bid.MarshalMsg(nil)
		if err != nil {
			return nil, err
		}
		es := &EventSerialized{
			Etype: uint8(e.Etype),
			Body:  bid_encoded,
		}
		return es.MarshalMsg(nil)
	default:
		return nil, fmt.Errorf("wrong event type: %v", e.Etype)
	}
}

func (e *Event) UnmarshalMsg(b []byte) ([]byte, error) {
	es := &EventSerialized{}
	leftover, err := es.UnmarshalMsg(b)
	if err != nil {
		return nil, err
	}
	switch EType(es.Etype) {
	case PERSON:
		person := &Person{}
		_, err := person.UnmarshalMsg([]byte(es.Body))
		if err != nil {
			return nil, err
		}
		e.NewPerson = person
		e.Etype = EType(es.Etype)
		return leftover, nil
	case AUCTION:
		auction := &Auction{}
		_, err := auction.UnmarshalMsg([]byte(es.Body))
		if err != nil {
			return nil, err
		}
		e.NewAuction = auction
		e.Etype = EType(es.Etype)
		return leftover, nil
	case BID:
		bid := &Bid{}
		_, err := bid.UnmarshalMsg([]byte(es.Body))
		if err != nil {
			return nil, err
		}
		e.Bid = bid
		e.Etype = EType(es.Etype)
		return leftover, nil
	default:
		return nil, fmt.Errorf("wrong event type: %v", es.Etype)
	}
}
