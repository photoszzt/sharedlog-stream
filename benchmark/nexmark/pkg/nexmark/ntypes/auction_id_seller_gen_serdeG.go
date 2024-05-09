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

func (s AuctionIdSellerJSONSerdeG) Encode(value AuctionIdSeller) ([]byte, *[]byte, error) {
	r, err := json.Marshal(value)
	return r, nil, err
}

func (s AuctionIdSellerJSONSerdeG) Decode(value []byte) (AuctionIdSeller, error) {
	v := AuctionIdSeller{}
	if err := json.Unmarshal(value, &v); err != nil {
		return AuctionIdSeller{}, err
	}
	return v, nil
}

type AuctionIdSellerMsgpSerdeG struct {
	commtypes.DefaultMsgpSerde
}

var _ = commtypes.SerdeG[AuctionIdSeller](AuctionIdSellerMsgpSerdeG{})

func (s AuctionIdSellerMsgpSerdeG) Encode(value AuctionIdSeller) ([]byte, *[]byte, error) {
	b := commtypes.PopBuffer()
	buf := *b
	r, err := value.MarshalMsg(buf[:0])
	return r, b, err
}

func (s AuctionIdSellerMsgpSerdeG) Decode(value []byte) (AuctionIdSeller, error) {
	v := AuctionIdSeller{}
	if _, err := v.UnmarshalMsg(value); err != nil {
		return AuctionIdSeller{}, err
	}
	return v, nil
}

func GetAuctionIdSellerSerdeG(serdeFormat commtypes.SerdeFormat) (commtypes.SerdeG[AuctionIdSeller], error) {
	if serdeFormat == commtypes.JSON {
		return AuctionIdSellerJSONSerdeG{}, nil
	} else if serdeFormat == commtypes.MSGP {
		return AuctionIdSellerMsgpSerdeG{}, nil
	} else {
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}
