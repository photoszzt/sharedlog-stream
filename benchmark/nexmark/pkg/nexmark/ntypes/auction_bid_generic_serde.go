package ntypes

import (
	"encoding/json"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
)

type AuctionBidJSONSerdeG struct{}

var _ = commtypes.SerdeG[*AuctionBid](AuctionBidJSONSerdeG{})

func (s AuctionBidJSONSerdeG) Encode(value *AuctionBid) ([]byte, error) {
	return json.Marshal(value)
}

func (s AuctionBidJSONSerdeG) Decode(value []byte) (*AuctionBid, error) {
	ab := AuctionBid{}
	if err := json.Unmarshal(value, &ab); err != nil {
		return nil, err
	}
	return &ab, nil
}

type AuctionBidMsgpSerdeG struct{}

var _ = commtypes.SerdeG[*AuctionBid](AuctionBidMsgpSerdeG{})

func (s AuctionBidMsgpSerdeG) Encode(value *AuctionBid) ([]byte, error) {
	return value.MarshalMsg(nil)
}

func (s AuctionBidMsgpSerdeG) Decode(value []byte) (*AuctionBid, error) {
	ab := AuctionBid{}
	if _, err := ab.UnmarshalMsg(value); err != nil {
		return nil, err
	}
	return &ab, nil
}

func GetAuctionBidSerdeG(serdeFormat commtypes.SerdeFormat) (commtypes.SerdeG[*AuctionBid], error) {
	switch serdeFormat {
	case commtypes.JSON:
		return AuctionBidJSONSerdeG{}, nil
	case commtypes.MSGP:
		return AuctionBidMsgpSerdeG{}, nil
	default:
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}
