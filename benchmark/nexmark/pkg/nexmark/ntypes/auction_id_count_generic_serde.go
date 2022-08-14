package ntypes

import (
	"encoding/json"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
)

type AuctionIdCountJSONSerdeG struct{}

var _ = commtypes.SerdeG[AuctionIdCount](AuctionIdCountJSONSerdeG{})

func (e AuctionIdCountJSONSerdeG) Encode(value AuctionIdCount) ([]byte, error) {
	return json.Marshal(&value)
}

func (d AuctionIdCountJSONSerdeG) Decode(value []byte) (AuctionIdCount, error) {
	se := AuctionIdCount{}
	err := json.Unmarshal(value, &se)
	if err != nil {
		return AuctionIdCount{}, err
	}
	return se, nil
}

type AuctionIdCountMsgpSerdeG struct{}

var _ = commtypes.SerdeG[AuctionIdCount](AuctionIdCountMsgpSerdeG{})

func (e AuctionIdCountMsgpSerdeG) Encode(value AuctionIdCount) ([]byte, error) {
	return value.MarshalMsg(nil)
}

func (d AuctionIdCountMsgpSerdeG) Decode(value []byte) (AuctionIdCount, error) {
	se := AuctionIdCount{}
	_, err := se.UnmarshalMsg(value)
	if err != nil {
		return AuctionIdCount{}, err
	}
	return se, nil
}

func NewAuctionIdCountSerdeG(serdeFormat commtypes.SerdeFormat) (commtypes.SerdeG[AuctionIdCount], error) {
	if serdeFormat == commtypes.JSON {
		return AuctionIdCountJSONSerdeG{}, nil
	} else if serdeFormat == commtypes.MSGP {
		return AuctionIdCountMsgpSerdeG{}, nil
	} else {
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}
