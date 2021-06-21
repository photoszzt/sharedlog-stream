package model

import (
	"time"
)

type Auction struct {
	ID          uint64    `json:"id"`
	ItemName    string    `json:"itemName"`
	Description string    `json:"description"`
	InitialBid  uint64    `json:"initialBid"`
	Reserve     uint64    `json:"reserve"`
	DateTime    time.Time `json:"dataTime"`
	Expires     time.Time `json:"expires"`
	Seller      uint64    `json:"seller"`
	Category    uint64    `json:"category"`
	Extra       string    `json:"extra"`
}

type Bid struct {
	Auction  uint64    `json:"auction"`
	Bidder   uint64    `json:"bidder"`
	Price    uint64    `json:"price"`
	Channel  string    `json:"channel"`
	Url      string    `json:"url"`
	DateTime time.Time `json:"dateTime"`
	Extra    string    `json:"extra"`
}

type Person struct {
	ID           uint64    `json:"id"`
	Name         string    `json:"name"`
	EmailAddress string    `json:"emailAddress"`
	CreditCard   string    `json:"creditCard"`
	City         string    `json:"city"`
	State        string    `json:"state"`
	DateTime     time.Time `json:"dateTime"`
	Extra        string    `json:"extra"`
}
