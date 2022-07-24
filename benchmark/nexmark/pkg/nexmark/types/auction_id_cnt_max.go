//go:generate msgp
//msgp:ignore AuctionIdCntMaxJSONSerde AuctionIdCntMaxMsgpSerde
package types

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

var _ = fmt.Stringer(AuctionIdCntMax{})

func (aicm AuctionIdCntMax) String() string {
	return fmt.Sprintf("AuctionIdCntMax: {AucID: %d, Count: %d, MaxCnt: %d}",
		aicm.AucId, aicm.Count, aicm.MaxCnt)
}

type AuctionIdCntMaxJSONSerde struct{}
type AuctionIdCntMaxJSONSerdeG struct{}

var _ = commtypes.Serde(AuctionIdCntMaxJSONSerde{})
var _ = commtypes.SerdeG[AuctionIdCntMax](AuctionIdCntMaxJSONSerdeG{})

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

func (s AuctionIdCntMaxJSONSerdeG) Encode(value AuctionIdCntMax) ([]byte, error) {
	return json.Marshal(&value)
}

func (s AuctionIdCntMaxJSONSerdeG) Decode(value []byte) (AuctionIdCntMax, error) {
	ai := AuctionIdCntMax{}
	err := json.Unmarshal(value, &ai)
	if err != nil {
		return AuctionIdCntMax{}, err
	}
	return ai, nil
}

type AuctionIdCntMaxMsgpSerde struct{}
type AuctionIdCntMaxMsgpSerdeG struct{}

var _ = commtypes.Serde(AuctionIdCntMaxMsgpSerde{})
var _ = commtypes.SerdeG[AuctionIdCntMax](AuctionIdCntMaxMsgpSerdeG{})

func (s AuctionIdCntMaxMsgpSerde) Encode(value interface{}) ([]byte, error) {
	ai := value.(*AuctionIdCntMax)
	return ai.MarshalMsg(nil)
}

func (s AuctionIdCntMaxMsgpSerde) Decode(value []byte) (interface{}, error) {
	ai := AuctionIdCntMax{}
	_, err := ai.UnmarshalMsg(value)
	if err != nil {
		return nil, err
	}
	return ai, nil
}

func (s AuctionIdCntMaxMsgpSerdeG) Encode(value AuctionIdCntMax) ([]byte, error) {
	return value.MarshalMsg(nil)
}

func (s AuctionIdCntMaxMsgpSerdeG) Decode(value []byte) (AuctionIdCntMax, error) {
	ai := AuctionIdCntMax{}
	_, err := ai.UnmarshalMsg(value)
	if err != nil {
		return AuctionIdCntMax{}, err
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

func GetAuctionIdCntMaxSerdeG(serdeFormat commtypes.SerdeFormat) (commtypes.SerdeG[AuctionIdCntMax], error) {
	if serdeFormat == commtypes.JSON {
		return AuctionIdCntMaxJSONSerdeG{}, nil
	} else if serdeFormat == commtypes.MSGP {
		return AuctionIdCntMaxMsgpSerdeG{}, nil
	} else {
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}
