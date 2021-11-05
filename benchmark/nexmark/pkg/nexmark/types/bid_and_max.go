//go:generate msgp
//msgp:ignore BidAndMaxJSONSerde BidAndMaxMsgpSerde
package types

import "encoding/json"

type BidAndMax struct {
	Extra       string `json:"extra" msg:"extra"`
	Price       uint64 `json:"price" msg:"price"`
	Auction     uint64 `json:"auction" msg:"auction"`
	Bidder      uint64 `json:"bidder" msg:"bidder"`
	DateTime    int64  `json:"dateTime" msg:"dateTime"`
	MaxDateTime int64  `json:"maxDateTime" msg:"maxDateTime"`
}

type BidAndMaxJSONSerde struct{}

func (s BidAndMaxJSONSerde) Encode(value interface{}) ([]byte, error) {
	bm := value.(*BidAndMax)
	return json.Marshal(bm)
}

func (s BidAndMaxJSONSerde) Decode(value []byte) (interface{}, error) {
	bm := BidAndMax{}
	if err := json.Unmarshal(value, &bm); err != nil {
		return nil, err
	}
	return bm, nil
}

type BidAndMaxMsgpSerde struct{}

func (s BidAndMaxMsgpSerde) Encode(value interface{}) ([]byte, error) {
	bm := value.(*BidAndMax)
	return bm.MarshalMsg(nil)
}

func (s BidAndMaxMsgpSerde) Decode(value []byte) (interface{}, error) {
	bm := BidAndMax{}
	if _, err := bm.UnmarshalMsg(value); err != nil {
		return nil, err
	}
	return bm, nil
}
