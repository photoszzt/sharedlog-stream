package ntypes

import (
	"encoding/json"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
)

type AuctionIdSellerJSONSerde struct {
	commtypes.DefaultJSONSerde
}

var _ = commtypes.Serde(AuctionIdSellerJSONSerde{})

func (s AuctionIdSellerJSONSerde) Encode(value interface{}) ([]byte, *[]byte, error) {
	v, ok := value.(*AuctionIdSeller)
	if !ok {
		vTmp := value.(AuctionIdSeller)
		v = &vTmp
	}
	r, err := json.Marshal(v)
	return r, nil, err
}

func (s AuctionIdSellerJSONSerde) Decode(value []byte) (interface{}, error) {
	v := AuctionIdSeller{}
	if err := json.Unmarshal(value, &v); err != nil {
		return nil, err
	}
	return v, nil
}

type AuctionIdSellerMsgpSerde struct {
	commtypes.DefaultMsgpSerde
}

var _ = commtypes.Serde(AuctionIdSellerMsgpSerde{})

func (s AuctionIdSellerMsgpSerde) Encode(value interface{}) ([]byte, *[]byte, error) {
	v, ok := value.(*AuctionIdSeller)
	if !ok {
		vTmp := value.(AuctionIdSeller)
		v = &vTmp
	}
	b := commtypes.PopBuffer()
	buf := *b
	r, err := v.MarshalMsg(buf[:0])
	return r, b, err
}

func (s AuctionIdSellerMsgpSerde) Decode(value []byte) (interface{}, error) {
	v := AuctionIdSeller{}
	if _, err := v.UnmarshalMsg(value); err != nil {
		return nil, err
	}
	return v, nil
}

func GetAuctionIdSellerSerde(serdeFormat commtypes.SerdeFormat) (commtypes.Serde, error) {
	switch serdeFormat {
	case commtypes.JSON:
		return AuctionIdSellerJSONSerde{}, nil
	case commtypes.MSGP:
		return AuctionIdSellerMsgpSerde{}, nil
	default:
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}
