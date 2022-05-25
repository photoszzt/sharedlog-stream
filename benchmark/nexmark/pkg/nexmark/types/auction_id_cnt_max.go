//go:generate msgp
package types

import (
	"encoding/json"
	"sharedlog-stream/pkg/stream/processor/commtypes"
)

type AuctionIdCntMax struct {
	AucId  uint64 `json:"aucId" msg:"aucId"`
	Count  uint64 `json:"cnt" msg:"cnt"`
	MaxCnt uint64 `json:"maxCnt" msg:"maxCnt"`

	BaseInjTime `msg:"bInjT"`
}

type AuctionIdCntMaxJSONSerde struct{}

var _ = commtypes.Encoder(AuctionIdCntMaxJSONSerde{})
var _ = commtypes.Decoder(AuctionIdCntMaxJSONSerde{})

func (s AuctionIdCntMaxJSONSerde) Encode(value interface{}) ([]byte, error) {
	ai := value.(*AuctionIdCntMax)
	return json.Marshal(ai)
}

func (s AuctionIdCntMaxJSONSerde) Decode(value []byte) (interface{}, error) {
	ai := &AuctionIdCntMax{}
	err := json.Unmarshal(value, ai)
	if err != nil {
		return nil, err
	}
	return ai, nil
}

type AuctionIdCntMaxMsgpSerde struct{}

var _ = commtypes.Encoder(AuctionIdCntMaxMsgpSerde{})

func (s AuctionIdCntMaxMsgpSerde) Encode(value interface{}) ([]byte, error) {
	ai := value.(*AuctionIdCntMax)
	return ai.MarshalMsg(nil)
}

func (s AuctionIdCntMaxMsgpSerde) Decode(value []byte) (interface{}, error) {
	ai := &AuctionIdCntMax{}
	_, err := ai.UnmarshalMsg(value)
	if err != nil {
		return nil, err
	}
	return ai, nil
}
