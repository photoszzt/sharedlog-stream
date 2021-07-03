//go:generate msgp
package types

import (
	"time"
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
