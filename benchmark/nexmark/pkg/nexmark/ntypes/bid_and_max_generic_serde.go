package ntypes

import (
	"encoding/json"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
)

type BidAndMaxJSONSerdeG struct{}

var _ = commtypes.SerdeG[BidAndMax](BidAndMaxJSONSerdeG{})

func (s BidAndMaxJSONSerdeG) Encode(value BidAndMax) ([]byte, error) {
	return json.Marshal(&value)
}

func (s BidAndMaxJSONSerdeG) Decode(value []byte) (BidAndMax, error) {
	bm := BidAndMax{}
	if err := json.Unmarshal(value, &bm); err != nil {
		return BidAndMax{}, err
	}
	return bm, nil
}

type BidAndMaxMsgpSerdeG struct{}

var _ = commtypes.SerdeG[BidAndMax](BidAndMaxMsgpSerdeG{})

func (s BidAndMaxMsgpSerdeG) Encode(value BidAndMax) ([]byte, error) {
	return value.MarshalMsg(nil)
}

func (s BidAndMaxMsgpSerdeG) Decode(value []byte) (BidAndMax, error) {
	bm := BidAndMax{}
	if _, err := bm.UnmarshalMsg(value); err != nil {
		return BidAndMax{}, err
	}
	return bm, nil
}

func GetBidAndMaxSerdeG(serdeFormat commtypes.SerdeFormat) (commtypes.SerdeG[BidAndMax], error) {
	if serdeFormat == commtypes.JSON {
		return BidAndMaxJSONSerdeG{}, nil
	} else if serdeFormat == commtypes.MSGP {
		return BidAndMaxMsgpSerdeG{}, nil
	} else {
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}
