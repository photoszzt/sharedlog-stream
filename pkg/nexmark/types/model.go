//go:generate msgp
//msgp:ignore Event
package types

import (
	"encoding/json"
	"fmt"

	"github.com/tinylib/msgp/msgp"
)

type Auction struct {
	ID          uint64 `msg:"id" json:"id"`
	ItemName    string `msg:"itemName" json:"itemName"`
	Description string `msg:"description" json:"description"`
	InitialBid  uint64 `msg:"initialBid" json:"initialBid"`
	Reserve     uint64 `msg:"reserve" json:"reserve"`
	DateTime    int64  `msg:"dataTime" json:"dataTime"` // unix timestamp in ms
	Expires     int64  `msg:"expires" json:"expires"`   // unix timestamp in ms
	Seller      uint64 `msg:"seller" json:"seller"`
	Category    uint64 `msg:"category" json:"category"`
	Extra       string `msg:"extra" json:"extra"`
}

type Bid struct {
	Auction  uint64 `msg:"auction" json:"auction"`
	Bidder   uint64 `msg:"bidder" json:"bidder"`
	Price    uint64 `msg:"price" json:"price"`
	Channel  string `msg:"channel" json:"channel"`
	Url      string `msg:"url" json:"url"`
	DateTime int64  `msg:"dateTime" json:"dateTime"` // unix timestamp in ms
	Extra    string `msg:"extra" json:"extra"`
}

type Person struct {
	ID           uint64 `msg:"id" json:"id"`
	Name         string `msg:"name" json:"name"`
	EmailAddress string `msg:"emailAddress" json:"emailAddress"`
	CreditCard   string `msg:"creditCard" json:"creditCard"`
	City         string `msg:"city" json:"city"`
	State        string `msg:"state" json:"state"`
	DateTime     int64  `msg:"dateTime" json:"dataTime"` // unix timestamp in ms
	Extra        string `msg:"extra" json:"extra"`
}

type EType uint8

const (
	PERSON  EType = 0
	AUCTION EType = 1
	BID     EType = 2
)

type Event struct {
	NewPerson  *Person  `json:"newPerson",omitempty`
	NewAuction *Auction `json:"newAuction",omitempty`
	Bid        *Bid     `json:"bid",omitempty`
	Etype      EType    `json:"type"`
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

func (e *Event) MarshalMsg() ([]byte, error) {
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

type EventMsgpEncoder struct{}

func (e EventMsgpEncoder) Encode(value interface{}) ([]byte, error) {
	event := value.(*Event)
	return event.MarshalMsg()
}

type EventJSONEncoder struct{}

func (e EventJSONEncoder) Encode(value interface{}) ([]byte, error) {
	event := value.(*Event)
	return json.Marshal(event)
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

type EventMsgDecoder struct{}

func (emd EventMsgDecoder) Decode(value []byte) (interface{}, error) {
	e := Event{}
	_, err := e.UnmarshalMsg(value)
	if err != nil {
		return nil, err
	} else {
		return e, nil
	}
}

type EventJSONDecoder struct{}

func (ejd EventJSONDecoder) Decode(value []byte) (interface{}, error) {
	e := Event{}
	if err := json.Unmarshal(value, &e); err != nil {
		return nil, err
	} else {
		return e, nil
	}
}
