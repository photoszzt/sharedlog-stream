package ntypes

import (
	"encoding/json"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
)

type BidPriceJSONSerdeG struct {
	commtypes.DefaultJSONSerde
}

var _ = commtypes.SerdeG[BidPrice](BidPriceJSONSerdeG{})

func (s BidPriceJSONSerdeG) Encode(value BidPrice) ([]byte, *[]byte, error) {
	r, err := json.Marshal(value)
	return r, nil, err
}

func (s BidPriceJSONSerdeG) Decode(value []byte) (BidPrice, error) {
	v := BidPrice{}
	if err := json.Unmarshal(value, &v); err != nil {
		return BidPrice{}, err
	}
	return v, nil
}

type BidPriceMsgpSerdeG struct {
	commtypes.DefaultMsgpSerde
}

var _ = commtypes.SerdeG[BidPrice](BidPriceMsgpSerdeG{})

func (s BidPriceMsgpSerdeG) Encode(value BidPrice) ([]byte, *[]byte, error) {
	b := commtypes.PopBuffer(value.Msgsize())
	buf := *b
	r, err := value.MarshalMsg(buf[:0])
	return r, b, err
}

func (s BidPriceMsgpSerdeG) Decode(value []byte) (BidPrice, error) {
	v := BidPrice{}
	if _, err := v.UnmarshalMsg(value); err != nil {
		return BidPrice{}, err
	}
	return v, nil
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
