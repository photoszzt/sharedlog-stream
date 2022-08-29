package ntypes

import (
	"encoding/json"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
)

type BidPriceJSONSerdeG struct{}
type BidPriceMsgpSerdeG struct{}

var _ = commtypes.SerdeG[BidPrice](BidPriceJSONSerdeG{})

func (s BidPriceJSONSerdeG) Encode(value BidPrice) ([]byte, error) {
	return json.Marshal(&value)
}

func (s BidPriceJSONSerdeG) Decode(value []byte) (BidPrice, error) {
	bp := BidPrice{}
	if err := json.Unmarshal(value, &bp); err != nil {
		return BidPrice{}, err
	}
	return bp, nil
}

var _ = commtypes.SerdeG[BidPrice](BidPriceMsgpSerdeG{})

func (s BidPriceMsgpSerdeG) Encode(value BidPrice) ([]byte, error) {
	return value.MarshalMsg(nil)
}

func (s BidPriceMsgpSerdeG) Decode(value []byte) (BidPrice, error) {
	bp := BidPrice{}
	if _, err := bp.UnmarshalMsg(value); err != nil {
		return BidPrice{}, err
	}
	return bp, nil
}

func GetBidPriceSerdeG(serdeFormat commtypes.SerdeFormat) (commtypes.SerdeG[BidPrice], error) {
	if serdeFormat == commtypes.JSON {
		return BidPriceJSONSerdeG{}, nil
	} else if serdeFormat == commtypes.MSGP {
		return BidPriceMsgpSerdeG{}, nil
	} else {
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}
