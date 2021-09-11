//go:generate greenpack
//msgp:ignore EventMsgpEncoder EventMsgpDecoder
//msgp:ignore EventJSONEncoder EventJSONDecoder
//msgp:ignore MessageSerializedMsgpEncoder MessageSerializedJSONDecoder
package types

import (
	"encoding/json"

	"cs.utexas.edu/zhitingz/sharedlog-stream/pkg/stream/processor"
)

type SerdeFormat uint8

const (
	JSON SerdeFormat = 0
	MSGP SerdeFormat = 1
)

type MessageSerialized struct {
	Key   []byte `json:",omitempty" zid:"0" msg:",omitempty"`
	Value []byte `json:",omitempty" zid:"1" msg:",omitempty"`
}

type NameCityStateId struct {
	Name  string `zid:"0"`
	City  string `zid:"1"`
	State string `zid:"2"`
	ID    uint64 `zid:"3"`
}

type Auction struct {
	ID          uint64 `zid:"0" msg:"id" json:"id"`
	ItemName    string `zid:"1" msg:"itemName" json:"itemName"`
	Description string `zid:"2" msg:"description" json:"description"`
	InitialBid  uint64 `zid:"3" msg:"initialBid" json:"initialBid"`
	Reserve     uint64 `zid:"4" msg:"reserve" json:"reserve"`
	DateTime    int64  `zid:"5" msg:"dataTime" json:"dataTime"` // unix timestamp in ms
	Expires     int64  `zid:"6" msg:"expires" json:"expires"`   // unix timestamp in ms
	Seller      uint64 `zid:"7" msg:"seller" json:"seller"`
	Category    uint64 `zid:"8" msg:"category" json:"category"`
	Extra       string `zid:"9" msg:"extra" json:"extra"`
}

type Bid struct {
	Auction  uint64 `zid:"0" msg:"auction" json:"auction"`
	Bidder   uint64 `zid:"1" msg:"bidder" json:"bidder"`
	Price    uint64 `zid:"2" msg:"price" json:"price"`
	Channel  string `zid:"3" msg:"channel" json:"channel"`
	Url      string `zid:"4" msg:"url" json:"url"`
	DateTime int64  `zid:"5" msg:"dateTime" json:"dateTime"` // unix timestamp in ms
	Extra    string `zid:"6" msg:"extra" json:"extra"`
}

type Person struct {
	ID           uint64 `zid:"0" msg:"id" json:"id"`
	Name         string `zid:"1" msg:"name" json:"name"`
	EmailAddress string `zid:"2" msg:"emailAddress" json:"emailAddress"`
	CreditCard   string `zid:"3" msg:"creditCard" json:"creditCard"`
	City         string `zid:"4" msg:"city" json:"city"`
	State        string `zid:"5" msg:"state" json:"state"`
	DateTime     int64  `zid:"6" msg:"dateTime" json:"dataTime"` // unix timestamp in ms
	Extra        string `zid:"7" msg:"extra" json:"extra"`
}

type EType uint8

const (
	PERSON  EType = 0
	AUCTION EType = 1
	BID     EType = 2
)

type Event struct {
	Etype      EType    `json:"etype" zid:"0" msgp:"etype"`
	NewPerson  *Person  `json:"newPerson,omitempty" zid:"1" msgp:"newPerson,omitempty"`
	NewAuction *Auction `json:"newAuction,omitempty" zid:"2" msgp:"newAuction,omitempty"`
	Bid        *Bid     `json:"bid,omitempty" zid:"3" msgp:"bid,omitempty"`
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

type EventMsgpEncoder struct{}

var _ = processor.Encoder(EventMsgpEncoder{})

func (e EventMsgpEncoder) Encode(value interface{}) ([]byte, error) {
	event := value.(*Event)
	return event.MarshalMsg(nil)
}

type EventJSONEncoder struct{}

var _ = processor.Encoder(EventJSONEncoder{})

func (e EventJSONEncoder) Encode(value interface{}) ([]byte, error) {
	event := value.(*Event)
	return json.Marshal(event)
}

var _ = processor.MsgEncoder(MessageSerializedMsgpEncoder{})

type MessageSerializedMsgpEncoder struct{}

func (e MessageSerializedMsgpEncoder) Encode(key []byte, value []byte) ([]byte, error) {
	msg := MessageSerialized{
		Key:   key,
		Value: value,
	}
	return msg.MarshalMsg(nil)
}

type MessageSerializedJSONEncoder struct{}

var _ = processor.MsgEncoder(MessageSerializedJSONEncoder{})

func (e MessageSerializedJSONEncoder) Encode(key []byte, value []byte) ([]byte, error) {
	msg := MessageSerialized{
		Key:   key,
		Value: value,
	}
	return json.Marshal(msg)
}

type EventMsgpDecoder struct{}

var _ = processor.Decoder(EventMsgpDecoder{})

func (emd EventMsgpDecoder) Decode(value []byte) (interface{}, error) {
	e := Event{}
	_, err := e.UnmarshalMsg(value)
	if err != nil {
		return nil, err
	} else {
		return e, nil
	}
}

type EventJSONDecoder struct{}

var _ = processor.Decoder(EventJSONDecoder{})

func (ejd EventJSONDecoder) Decode(value []byte) (interface{}, error) {
	e := Event{}
	if err := json.Unmarshal(value, &e); err != nil {
		return nil, err
	} else {
		return e, nil
	}
}

type MessageSerializedMsgpDecoder struct{}

var _ = processor.MsgDecoder(MessageSerializedMsgpDecoder{})

func (msmd MessageSerializedMsgpDecoder) Decode(value []byte) ([]byte /* key */, []byte /* value */, error) {
	msg := MessageSerialized{}
	_, err := msg.UnmarshalMsg(value)
	if err != nil {
		return nil, nil, err
	} else {
		return msg.Key, msg.Value, nil
	}
}

type MessageSerializedJSONDecoder struct{}

var _ = processor.MsgDecoder(MessageSerializedJSONDecoder{})

func (msmd MessageSerializedJSONDecoder) Decode(value []byte) ([]byte /* key */, []byte /* value */, error) {
	msg := MessageSerialized{}
	if err := json.Unmarshal(value, &msg); err != nil {
		return nil, nil, err
	} else {
		return msg.Key, msg.Value, nil
	}
}
