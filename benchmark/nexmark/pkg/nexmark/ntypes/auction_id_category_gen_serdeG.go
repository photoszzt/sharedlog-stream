package ntypes

import (
	"encoding/json"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
)

type AuctionIdCategoryJSONSerdeG struct {
	commtypes.DefaultJSONSerde
}

var _ = commtypes.SerdeG[AuctionIdCategory](AuctionIdCategoryJSONSerdeG{})

func (s AuctionIdCategoryJSONSerdeG) Encode(value AuctionIdCategory) ([]byte, *[]byte, error) {
	r, err := json.Marshal(value)
	return r, nil, err
}

func (s AuctionIdCategoryJSONSerdeG) Decode(value []byte) (AuctionIdCategory, error) {
	v := AuctionIdCategory{}
	if err := json.Unmarshal(value, &v); err != nil {
		return AuctionIdCategory{}, err
	}
	return v, nil
}

type AuctionIdCategoryMsgpSerdeG struct {
	commtypes.DefaultMsgpSerde
}

var _ = commtypes.SerdeG[AuctionIdCategory](AuctionIdCategoryMsgpSerdeG{})

func (s AuctionIdCategoryMsgpSerdeG) Encode(value AuctionIdCategory) ([]byte, *[]byte, error) {
	b := commtypes.PopBuffer(value.Msgsize())
	buf := *b
	r, err := value.MarshalMsg(buf[:0])
	return r, b, err
}

func (s AuctionIdCategoryMsgpSerdeG) Decode(value []byte) (AuctionIdCategory, error) {
	v := AuctionIdCategory{}
	if _, err := v.UnmarshalMsg(value); err != nil {
		return AuctionIdCategory{}, err
	}
	return v, nil
}

func GetAuctionIdCategorySerdeG(serdeFormat commtypes.SerdeFormat) (commtypes.SerdeG[AuctionIdCategory], error) {
	if serdeFormat == commtypes.JSON {
		return AuctionIdCategoryJSONSerdeG{}, nil
	} else if serdeFormat == commtypes.MSGP {
		return AuctionIdCategoryMsgpSerdeG{}, nil
	} else {
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}
