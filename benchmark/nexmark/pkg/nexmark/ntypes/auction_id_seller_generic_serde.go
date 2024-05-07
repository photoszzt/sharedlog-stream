package ntypes

import (
	"encoding/json"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
)

type AuctionIdSellerJSONSerdeG struct {
	commtypes.DefaultJSONSerde
}

var _ = commtypes.SerdeG[AuctionIdSeller](AuctionIdSellerJSONSerdeG{})

func (s AuctionIdSellerJSONSerdeG) Encode(value AuctionIdSeller) ([]byte, error) {
	return json.Marshal(&value)
}

func (s AuctionIdSellerJSONSerdeG) Decode(value []byte) (AuctionIdSeller, error) {
	v := AuctionIdSeller{}
	err := json.Unmarshal(value, &v)
	if err != nil {
		return AuctionIdSeller{}, err
	}
	return v, nil
}

type AuctionIdSellerMsgpSerdeG struct {
	commtypes.DefaultMsgpSerde
}

var _ = commtypes.SerdeG[AuctionIdSeller](AuctionIdSellerMsgpSerdeG{})

func (s AuctionIdSellerMsgpSerdeG) Encode(value AuctionIdSeller) ([]byte, error) {
	b := commtypes.PopBuffer()
	buf := *b
	return value.MarshalMsg(buf[:0])
}

func (s AuctionIdSellerMsgpSerdeG) Decode(value []byte) (AuctionIdSeller, error) {
	v := AuctionIdSeller{}
	_, err := v.UnmarshalMsg(value)
	if err != nil {
		return AuctionIdSeller{}, err
	}
	return v, nil
}

func GetAuctionIDSellerSerdeG(serdeFormat commtypes.SerdeFormat) (commtypes.SerdeG[AuctionIdSeller], error) {
	switch serdeFormat {
	case commtypes.JSON:
		return AuctionIdSellerJSONSerdeG{}, nil
	case commtypes.MSGP:
		return AuctionIdSellerMsgpSerdeG{}, nil
	default:
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}
