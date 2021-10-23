//go:generate msgp
//msgp:ignore EventMsgpEncoder EventMsgpDecoder
//msgp:ignore EventJSONEncoder EventJSONDecoder

package types

import (
	"encoding/json"

	"sharedlog-stream/pkg/stream/processor/commtypes"
)

type NameCityStateId struct {
	Name  string `msg:"name" json:"name"`
	City  string `msg:"city" json:"city"`
	State string `msg:"state" json:"state"`
	ID    uint64 `msg:"id" json:"id"`
}

type Auction struct {
	ID          uint64 `msg:"id" json:"id"`
	ItemName    string `msg:"itemName" json:"itemName"`
	Description string `msg:"description" json:"description"`
	InitialBid  uint64 `msg:"initialBid" json:"initialBid"`
	Reserve     uint64 `msg:"reserve" json:"reserve"`
	DateTime    int64  `msg:"dateTime" json:"dateTime"` // unix timestamp in ms
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
	DateTime     int64  `msg:"dateTime" json:"dateTime"` // unix timestamp in ms
	Extra        string `msg:"extra" json:"extra"`
}

type EType uint8

const (
	PERSON  EType = 0
	AUCTION EType = 1
	BID     EType = 2
)

type Event struct {
	Etype      EType    `json:"etype" msg:"etype"`
	NewPerson  *Person  `json:"newPerson,omitempty" msg:"newPerson,omitempty"`
	NewAuction *Auction `json:"newAuction,omitempty" msg:"newAuction,omitempty"`
	Bid        *Bid     `json:"bid,omitempty" msg:"bid,omitempty"`
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

type EventMsgpSerde struct{}

var _ = commtypes.Encoder(EventMsgpSerde{})

func (e EventMsgpSerde) Encode(value interface{}) ([]byte, error) {
	event := value.(*Event)
	return event.MarshalMsg(nil)
}

type EventJSONSerde struct{}

var _ = commtypes.Encoder(EventJSONSerde{})

func (e EventJSONSerde) Encode(value interface{}) ([]byte, error) {
	event := value.(*Event)
	return json.Marshal(event)
}

var _ = commtypes.Decoder(EventMsgpSerde{})

func (emd EventMsgpSerde) Decode(value []byte) (interface{}, error) {
	e := &Event{}
	_, err := e.UnmarshalMsg(value)
	if err != nil {
		return nil, err
	}
	return e, nil
}

var _ = commtypes.Decoder(EventJSONSerde{})

func (ejd EventJSONSerde) Decode(value []byte) (interface{}, error) {
	e := &Event{}
	if err := json.Unmarshal(value, e); err != nil {
		return nil, err
	}
	return e, nil
}
