//go:generate msgp
//msgp:ignore AuctionIdCountJSONEncoder AuctionIdCountJSONDecoder AuctionIdCountJSONSerde
//msgp:ignore AuctionIdCountMsgpEncoder AuctionIdCountMsgpDecoder AuctionIdCountMsgpSerde
package types

import (
	"encoding/json"
	"fmt"

	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
)

type AuctionIdCount struct {
	AucId uint64 `json:"aucId,omitempty" msg:"aucId,omitempty"`
	Count uint64 `json:"cnt,omitempty" msg:"cnt,omitempty"`
}

var _ = fmt.Stringer(AuctionIdCount{})

func (aic AuctionIdCount) String() string {
	return fmt.Sprintf("AuctionIdCount: {AucId: %d, Count: %d}", aic.AucId, aic.Count)
}

type AuctionIdCountJSONEncoder struct{}

var _ = commtypes.Encoder[AuctionIdCount](AuctionIdCountJSONEncoder{})

func (e AuctionIdCountJSONEncoder) Encode(value AuctionIdCount) ([]byte, error) {
	return json.Marshal(&value)
}

type AuctionIdCountJSONDecoder struct{}

var _ = commtypes.Decoder[AuctionIdCount](AuctionIdCountJSONDecoder{})

func (d AuctionIdCountJSONDecoder) Decode(value []byte) (AuctionIdCount, error) {
	se := AuctionIdCount{}
	err := json.Unmarshal(value, se)
	if err != nil {
		return AuctionIdCount{}, err
	}
	return se, nil
}

type AuctionIdCountJSONSerde struct {
	AuctionIdCountJSONEncoder
	AuctionIdCountJSONDecoder
}

type AuctionIdCountMsgpEncoder struct{}

var _ = commtypes.Encoder[AuctionIdCount](AuctionIdCountMsgpEncoder{})

func (e AuctionIdCountMsgpEncoder) Encode(value AuctionIdCount) ([]byte, error) {
	return value.MarshalMsg(nil)
}

type AuctionIdCountMsgpDecoder struct{}

var _ = commtypes.Decoder[AuctionIdCount](AuctionIdCountMsgpDecoder{})

func (d AuctionIdCountMsgpDecoder) Decode(value []byte) (AuctionIdCount, error) {
	se := AuctionIdCount{}
	_, err := se.UnmarshalMsg(value)
	if err != nil {
		return AuctionIdCount{}, err
	}
	return se, nil
}

type AuctionIdCountMsgpSerde struct {
	AuctionIdCountMsgpEncoder
	AuctionIdCountMsgpDecoder
}

func NewAuctionIdCountSerde(serdeFormat commtypes.SerdeFormat) (commtypes.Serde[AuctionIdCount], error) {
	if serdeFormat == commtypes.JSON {
		return AuctionIdCountJSONSerde{}, nil
	} else if serdeFormat == commtypes.MSGP {
		return AuctionIdCountMsgpSerde{}, nil
	} else {
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}
