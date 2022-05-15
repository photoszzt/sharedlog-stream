//go:generate msgp
//msgp:ignore AuctionIdCategoryJSONSerde AuctionIdCategoryMsgpSerde
package types

import "encoding/json"

type AuctionIdCategory struct {
	AucId    uint64 `json:"aucId,omitempty" msg:"aucId,omitempty"`
	Category uint64 `json:"cat,omitempty" msg:"cat,omitempty"`
}

type AuctionIdCategoryJSONSerde struct{}

func (s AuctionIdCategoryJSONSerde) Encode(value interface{}) ([]byte, error) {
	v := value.(*AuctionIdCategory)
	return json.Marshal(v)
}

func (s AuctionIdCategoryJSONSerde) Decode(value []byte) (interface{}, error) {
	v := &AuctionIdCategory{}
	err := json.Unmarshal(value, v)
	if err != nil {
		return nil, err
	}
	return v, nil
}

type AuctionIdCategoryMsgpSerde struct{}

func (s AuctionIdCategoryMsgpSerde) Encode(value interface{}) ([]byte, error) {
	aic := value.(*AuctionIdCategory)
	return aic.MarshalMsg(nil)
}

func (s AuctionIdCategoryMsgpSerde) Decode(value []byte) (interface{}, error) {
	aic := &AuctionIdCategory{}
	_, err := aic.UnmarshalMsg(value)
	if err != nil {
		return nil, err
	}
	return aic, nil
}
