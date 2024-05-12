package ntypes

import (
	"encoding/json"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
)

type AuctionBidJSONSerdeG struct {
	commtypes.DefaultJSONSerde
}

var _ = commtypes.SerdeG[*AuctionBid](AuctionBidJSONSerdeG{})

func (s AuctionBidJSONSerdeG) Encode(value *AuctionBid) ([]byte, *[]byte, error) {
	r, err := json.Marshal(value)
	return r, nil, err
}

func (s AuctionBidJSONSerdeG) Decode(value []byte) (*AuctionBid, error) {
	v := AuctionBid{}
	if err := json.Unmarshal(value, &v); err != nil {
		return nil, err
	}
	return &v, nil
}

type AuctionBidMsgpSerdeG struct {
	commtypes.DefaultMsgpSerde
}

var _ = commtypes.SerdeG[*AuctionBid](AuctionBidMsgpSerdeG{})

func (s AuctionBidMsgpSerdeG) Encode(value *AuctionBid) ([]byte, *[]byte, error) {
	b := commtypes.PopBuffer(value.Msgsize())
	buf := *b
	r, err := value.MarshalMsg(buf[:0])
	return r, b, err
}

func (s AuctionBidMsgpSerdeG) Decode(value []byte) (*AuctionBid, error) {
	v := AuctionBid{}
	if _, err := v.UnmarshalMsg(value); err != nil {
		return nil, err
	}
	return &v, nil
}

func GetAuctionBidSerdeG(serdeFormat commtypes.SerdeFormat) (commtypes.SerdeG[*AuctionBid], error) {
	if serdeFormat == commtypes.JSON {
		return AuctionBidJSONSerdeG{}, nil
	} else if serdeFormat == commtypes.MSGP {
		return AuctionBidMsgpSerdeG{}, nil
	} else {
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}
