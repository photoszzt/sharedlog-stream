package ntypes

import (
	"encoding/json"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
)

type AuctionIdCountJSONSerdeG struct {
	commtypes.DefaultJSONSerde
}

var _ = commtypes.SerdeG[AuctionIdCount](AuctionIdCountJSONSerdeG{})

func (s AuctionIdCountJSONSerdeG) Encode(value AuctionIdCount) ([]byte, *[]byte, error) {
	r, err := json.Marshal(value)
	return r, nil, err
}

func (s AuctionIdCountJSONSerdeG) Decode(value []byte) (AuctionIdCount, error) {
	v := AuctionIdCount{}
	if err := json.Unmarshal(value, &v); err != nil {
		return AuctionIdCount{}, err
	}
	return v, nil
}

type AuctionIdCountMsgpSerdeG struct {
	commtypes.DefaultMsgpSerde
}

var _ = commtypes.SerdeG[AuctionIdCount](AuctionIdCountMsgpSerdeG{})

func (s AuctionIdCountMsgpSerdeG) Encode(value AuctionIdCount) ([]byte, *[]byte, error) {
	b := commtypes.PopBuffer(value.Msgsize())
	buf := *b
	r, err := value.MarshalMsg(buf[:0])
	return r, b, err
}

func (s AuctionIdCountMsgpSerdeG) Decode(value []byte) (AuctionIdCount, error) {
	v := AuctionIdCount{}
	if _, err := v.UnmarshalMsg(value); err != nil {
		return AuctionIdCount{}, err
	}
	return v, nil
}

func GetAuctionIdCountSerdeG(serdeFormat commtypes.SerdeFormat) (commtypes.SerdeG[AuctionIdCount], error) {
	if serdeFormat == commtypes.JSON {
		return AuctionIdCountJSONSerdeG{}, nil
	} else if serdeFormat == commtypes.MSGP {
		return AuctionIdCountMsgpSerdeG{}, nil
	} else {
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}
