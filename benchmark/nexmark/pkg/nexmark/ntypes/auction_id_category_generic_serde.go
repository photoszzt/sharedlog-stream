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

func (s AuctionIdCategoryJSONSerdeG) Encode(value AuctionIdCategory) ([]byte, error) {
	return json.Marshal(&value)
}

func (s AuctionIdCategoryJSONSerdeG) Decode(value []byte) (AuctionIdCategory, error) {
	v := AuctionIdCategory{}
	err := json.Unmarshal(value, &v)
	if err != nil {
		return AuctionIdCategory{}, err
	}
	return v, nil
}

type AuctionIdCategoryMsgpSerdeG struct {
	commtypes.DefaultJSONSerde
}

var _ = commtypes.SerdeG[AuctionIdCategory](AuctionIdCategoryMsgpSerdeG{})

func (s AuctionIdCategoryMsgpSerdeG) Encode(value AuctionIdCategory) ([]byte, error) {
	b := commtypes.PopBuffer()
	buf := *b
	return value.MarshalMsg(buf[:0])
}

func (s AuctionIdCategoryMsgpSerdeG) Decode(value []byte) (AuctionIdCategory, error) {
	aic := AuctionIdCategory{}
	_, err := aic.UnmarshalMsg(value)
	if err != nil {
		return AuctionIdCategory{}, err
	}
	return aic, nil
}

func GetAuctionIdCategorySerdeG(serdeFormat commtypes.SerdeFormat) (commtypes.SerdeG[AuctionIdCategory], error) {
	switch serdeFormat {
	case commtypes.JSON:
		return AuctionIdCategoryJSONSerdeG{}, nil
	case commtypes.MSGP:
		return AuctionIdCategoryMsgpSerdeG{}, nil
	default:
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}
