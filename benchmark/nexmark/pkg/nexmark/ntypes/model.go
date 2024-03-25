//go:generate msgp
//msgp:ignore EventMsgpEncoder EventMsgpDecoder EventMsgpSerde
//msgp:ignore EventJSONEncoder EventJSONDecoder EventJSONSerde

package ntypes

import (
	"encoding/json"
	"fmt"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
)

type Auction struct {
	ItemName    string `msg:"itemName" json:"itemName"`
	Description string `msg:"description" json:"description"`
	Extra       string `msg:"extra" json:"extra"`
	ID          uint64 `msg:"id" json:"id"`
	Reserve     uint64 `msg:"reserve" json:"reserve"`
	DateTime    int64  `msg:"dateTime" json:"dateTime"`
	Expires     int64  `msg:"expires" json:"expires"`
	Seller      uint64 `msg:"seller" json:"seller"`
	Category    uint64 `msg:"category" json:"category"`
	InitialBid  uint64 `msg:"initialBid" json:"initialBid"`
}

var _ = fmt.Stringer(Auction{})

func (a Auction) String() string {
	return fmt.Sprintf("Auction: {ItemName: %s, Description: %s, Extra: %s, ID: %d, Reserve: %d, TsMs: %d, Expires: %d, Seller: %d, Cat: %d, InitBid: %d}",
		a.ItemName, a.Description, a.Extra, a.ID, a.Reserve, a.DateTime, a.Expires, a.Seller, a.Category, a.InitialBid)
}

type Bid struct {
	Extra    string `msg:"extra" json:"extra"`
	Channel  string `msg:"channel" json:"channel"`
	Url      string `msg:"url" json:"url"`
	Bidder   uint64 `msg:"bidder" json:"bidder"`
	Price    uint64 `msg:"price" json:"price"`
	DateTime int64  `msg:"dateTime" json:"dateTime"`
	Auction  uint64 `msg:"auction" json:"auction"`
}

var _ = fmt.Stringer(Bid{})

func (b Bid) String() string {
	return fmt.Sprintf("Bid: {Extra: %s, Channel: %s, Url: %s, Bidder: %d, Price: %d, Ts: %d, Auc: %d}",
		b.Extra, b.Channel, b.Url, b.Bidder, b.Price, b.DateTime, b.Auction)
}

type Person struct {
	Name         string `msg:"name" json:"name"`
	EmailAddress string `msg:"emailAddress" json:"emailAddress"`
	CreditCard   string `msg:"creditCard" json:"creditCard"`
	City         string `msg:"city" json:"city"`
	State        string `msg:"state" json:"state"`
	Extra        string `msg:"extra" json:"extra"`
	ID           uint64 `msg:"id" json:"id"`
	DateTime     int64  `msg:"dateTime" json:"dateTime"`
}

var _ = fmt.Stringer(Person{})

func (p Person) String() string {
	return fmt.Sprintf("Person: {Name: %s, Email: %s, CreditCard: %s, City: %s, State: %s, Extra: %s, ID: %d, Ts: %d}",
		p.Name, p.EmailAddress, p.CreditCard, p.City, p.State, p.Extra, p.ID, p.DateTime)
}

type EType uint8

const (
	PERSON  EType = 0
	AUCTION EType = 1
	BID     EType = 2
	FANOUT  EType = 3
)

type Fanout struct {
	Test int `json:"test" msg:"test"`
}

type Event struct {
	NewPerson  *Person  `json:"newPerson,omitempty" msg:"newPerson,omitempty"`
	NewAuction *Auction `json:"newAuction,omitempty" msg:"newAuction,omitempty"`
	Bid        *Bid     `json:"bid,omitempty" msg:"bid,omitempty"`
	FanoutTest *Fanout  `json:"fo,omitempty" msg:"fo,omitempty"`
	Etype      EType    `json:"etype" msg:"etype"`
}

type Events struct {
	EventsArr []Event `json:"events"`
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
	e := Event{}
	_, err := e.UnmarshalMsg(value)
	if err != nil {
		return nil, err
	}
	return &e, nil
}

var _ = commtypes.Decoder(EventJSONSerde{})

func (ejd EventJSONSerde) Decode(value []byte) (interface{}, error) {
	e := &Event{}
	if err := json.Unmarshal(value, e); err != nil {
		return nil, err
	}
	return e, nil
}

func (e *Event) ExtractEventTime() (int64, error) {
	switch e.Etype {
	case PERSON:
		if e.NewPerson == nil {
			return 0, fmt.Errorf("new person should not be nil")
		}
		return e.NewPerson.DateTime, nil
	case BID:
		if e.Bid == nil {
			return 0, fmt.Errorf("bid should not be nil")
		}
		return e.Bid.DateTime, nil
	case AUCTION:
		if e.NewAuction == nil {
			return 0, fmt.Errorf("new auction should not be nil")
		}
		return e.NewAuction.DateTime, nil
	default:
		return 0, fmt.Errorf("failed to recognize event type")
	}
}

func GetEventSerde(serdeFormat commtypes.SerdeFormat) (commtypes.Serde, error) {
	if serdeFormat == commtypes.JSON {
		return EventJSONSerde{}, nil
	} else if serdeFormat == commtypes.MSGP {
		return EventMsgpSerde{}, nil
	} else {
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}
