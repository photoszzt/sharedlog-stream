package types

import (
	"encoding/json"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
)

type AuctionIdCntMaxJSONSerdeG struct{}

var _ = commtypes.SerdeG[AuctionIdCntMax](AuctionIdCntMaxJSONSerdeG{})

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

type AuctionIdCntMaxMsgpSerdeG struct{}

var _ = commtypes.SerdeG[AuctionIdCntMax](AuctionIdCntMaxMsgpSerdeG{})

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

func GetAuctionIdCntMaxSerdeG(serdeFormat commtypes.SerdeFormat) (commtypes.SerdeG[AuctionIdCntMax], error) {
	if serdeFormat == commtypes.JSON {
		return AuctionIdCntMaxJSONSerdeG{}, nil
	} else if serdeFormat == commtypes.MSGP {
		return AuctionIdCntMaxMsgpSerdeG{}, nil
	} else {
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}
