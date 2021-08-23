//go:generate msgp
//msgp:ignore Event
package types

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/tinylib/msgp/msgp"
)

type Auction struct {
	ID          uint64    `msg:"id" json:"id"`
	ItemName    string    `msg:"itemName" json:"itemName"`
	Description string    `msg:"description" json:"description"`
	InitialBid  uint64    `msg:"initialBid" json:"initialBid"`
	Reserve     uint64    `msg:"reserve" json:"reserve"`
	DateTime    time.Time `msg:"dataTime" json:"dataTime"`
	Expires     time.Time `msg:"expires" json:"expires"`
	Seller      uint64    `msg:"seller" json:"seller"`
	Category    uint64    `msg:"category" json:"category"`
	Extra       string    `msg:"extra" json:"extra"`
}

type Bid struct {
	Auction  uint64    `msg:"auction" json:"auction"`
	Bidder   uint64    `msg:"bidder" json:"bidder"`
	Price    uint64    `msg:"price" json:"price"`
	Channel  string    `msg:"channel" json:"channel"`
	Url      string    `msg:"url" json:"url"`
	DateTime time.Time `msg:"dateTime" json:"dateTime"`
	Extra    string    `msg:"extra" json:"extra"`
}

type Person struct {
	ID           uint64    `msg:"id" json:"id"`
	Name         string    `msg:"name" json:"name"`
	EmailAddress string    `msg:"emailAddress" json:"emailAddress"`
	CreditCard   string    `msg:"creditCard" json:"creditCard"`
	City         string    `msg:"city" json:"city"`
	State        string    `msg:"state" json:"state"`
	DateTime     time.Time `msg:"dateTime" json:"dataTime"`
	Extra        string    `msg:"extra" json:"extra"`
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

type EventSerializedJSON struct {
	Etype uint8           `json:"etypes"`
	Body  json.RawMessage `json:"body"`
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

func (e *Event) MarshalJSON() ([]byte, error) {
	switch e.Etype {
	case PERSON:
		person_encoded, err := json.Marshal(e.NewPerson)
		if err != nil {
			return nil, err
		}
		es := &EventSerializedJSON{
			Etype: uint8(e.Etype),
			Body:  person_encoded,
		}
		return json.Marshal(es)
	case AUCTION:
		auction_encoded, err := json.Marshal(e.NewAuction)
		if err != nil {
			return nil, err
		}
		es := &EventSerializedJSON{
			Etype: uint8(e.Etype),
			Body:  auction_encoded,
		}
		return json.Marshal(es)
	case BID:
		bid_encoded, err := json.Marshal(e.Bid)
		if err != nil {
			return nil, err
		}
		es := &EventSerializedJSON{
			Etype: uint8(e.Etype),
			Body:  bid_encoded,
		}
		return json.Marshal(es)
	default:
		return nil, fmt.Errorf("wrong event type: %d", e.Etype)
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
	return event.MarshalJSON()
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

func (e *Event) UnmarshalJSON(b []byte) error {
	es := &EventSerializedJSON{}
	err := json.Unmarshal(b, &es)
	if err != nil {
		return err
	}
	switch EType(es.Etype) {
	case PERSON:
		person := &Person{}
		err = json.Unmarshal([]byte(es.Body), person)
		if err != nil {
			return err
		}
		e.NewPerson = person
		e.Etype = EType(es.Etype)
		return nil
	case AUCTION:
		auction := &Auction{}
		err = json.Unmarshal([]byte(es.Body), auction)
		if err != nil {
			return err
		}
		e.NewAuction = auction
		e.Etype = EType(es.Etype)
		return nil
	case BID:
		bid := &Bid{}
		err = json.Unmarshal([]byte(es.Body), bid)
		if err != nil {
			return err
		}
		e.Bid = bid
		e.Etype = EType(es.Etype)
		return nil
	default:
		return fmt.Errorf("wrong event type: %v", es.Etype)
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
	if err := e.UnmarshalJSON(value); err != nil {
		return nil, err
	} else {
		return e, nil
	}
}
