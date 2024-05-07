//go:generate msgp
//msgp:ignore AuctionIdCntMaxJSONSerde AuctionIdCntMaxMsgpSerde
package ntypes

import (
	"encoding/json"
	"fmt"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
)

type AuctionIdCntMax struct {
	AucId  uint64 `json:"aucId" msg:"aucId"`
	Count  uint64 `json:"cnt" msg:"cnt"`
	MaxCnt uint64 `json:"maxCnt" msg:"maxCnt"`
}

func SizeOfAuctionIdCntMax(k AuctionIdCntMax) int64 {
	return 24
}

var _ = fmt.Stringer(AuctionIdCntMax{})

func (aicm AuctionIdCntMax) String() string {
	return fmt.Sprintf("AuctionIdCntMax: {AucID: %d, Count: %d, MaxCnt: %d}",
		aicm.AucId, aicm.Count, aicm.MaxCnt)
}

type AuctionIdCntMaxJSONSerde struct {
	commtypes.DefaultJSONSerde
}

var _ = commtypes.Serde(AuctionIdCntMaxJSONSerde{})

func (s AuctionIdCntMaxJSONSerde) Encode(value interface{}) ([]byte, error) {
	ai := value.(*AuctionIdCntMax)
	return json.Marshal(ai)
}

func (s AuctionIdCntMaxJSONSerde) Decode(value []byte) (interface{}, error) {
	ai := AuctionIdCntMax{}
	err := json.Unmarshal(value, &ai)
	if err != nil {
		return nil, err
	}
	return ai, nil
}

type AuctionIdCntMaxMsgpSerde struct {
	commtypes.DefaultMsgpSerde
}

var _ = commtypes.Serde(AuctionIdCntMaxMsgpSerde{})

func (s AuctionIdCntMaxMsgpSerde) Encode(value interface{}) ([]byte, error) {
	ai := value.(*AuctionIdCntMax)
	b := commtypes.PopBuffer()
	buf := *b
	return ai.MarshalMsg(buf[:0])
}

func (s AuctionIdCntMaxMsgpSerde) Decode(value []byte) (interface{}, error) {
	ai := AuctionIdCntMax{}
	_, err := ai.UnmarshalMsg(value)
	if err != nil {
		return nil, err
	}
	return ai, nil
}

func GetAuctionIdCntMaxSerde(serdeFormat commtypes.SerdeFormat) (commtypes.Serde, error) {
	if serdeFormat == commtypes.JSON {
		return AuctionIdCntMaxJSONSerde{}, nil
	} else if serdeFormat == commtypes.MSGP {
		return AuctionIdCntMaxMsgpSerde{}, nil
	} else {
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}
