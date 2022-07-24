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

type AuctionIdCountJSONSerde struct{}
type AuctionIdCountJSONSerdeG struct{}

var _ = commtypes.Serde(AuctionIdCountJSONSerde{})
var _ = commtypes.SerdeG[AuctionIdCount](AuctionIdCountJSONSerdeG{})

func (e AuctionIdCountJSONSerde) Encode(value interface{}) ([]byte, error) {
	se := value.(*AuctionIdCount)
	return json.Marshal(se)
}

func (d AuctionIdCountJSONSerde) Decode(value []byte) (interface{}, error) {
	se := AuctionIdCount{}
	err := json.Unmarshal(value, &se)
	if err != nil {
		return nil, err
	}
	return se, nil
}

func (e AuctionIdCountJSONSerdeG) Encode(value AuctionIdCount) ([]byte, error) {
	return json.Marshal(&value)
}

func (d AuctionIdCountJSONSerdeG) Decode(value []byte) (AuctionIdCount, error) {
	se := AuctionIdCount{}
	err := json.Unmarshal(value, &se)
	if err != nil {
		return AuctionIdCount{}, err
	}
	return se, nil
}

type AuctionIdCountMsgpSerde struct{}
type AuctionIdCountMsgpSerdeG struct{}

var _ = commtypes.Serde(AuctionIdCountMsgpSerde{})
var _ = commtypes.SerdeG[AuctionIdCount](AuctionIdCountMsgpSerdeG{})

func (e AuctionIdCountMsgpSerde) Encode(value interface{}) ([]byte, error) {
	se := value.(*AuctionIdCount)
	return se.MarshalMsg(nil)
}

func (d AuctionIdCountMsgpSerde) Decode(value []byte) (interface{}, error) {
	se := AuctionIdCount{}
	_, err := se.UnmarshalMsg(value)
	if err != nil {
		return nil, err
	}
	return se, nil
}

func (e AuctionIdCountMsgpSerdeG) Encode(value AuctionIdCount) ([]byte, error) {
	return value.MarshalMsg(nil)
}

func (d AuctionIdCountMsgpSerdeG) Decode(value []byte) (AuctionIdCount, error) {
	se := AuctionIdCount{}
	_, err := se.UnmarshalMsg(value)
	if err != nil {
		return AuctionIdCount{}, err
	}
	return se, nil
}

func NewAuctionIdCountSerde(serdeFormat commtypes.SerdeFormat) (commtypes.Serde, error) {
	if serdeFormat == commtypes.JSON {
		return AuctionIdCountJSONSerde{}, nil
	} else if serdeFormat == commtypes.MSGP {
		return AuctionIdCountMsgpSerde{}, nil
	} else {
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}

func NewAuctionIdCountSerdeG(serdeFormat commtypes.SerdeFormat) (commtypes.SerdeG[AuctionIdCount], error) {
	if serdeFormat == commtypes.JSON {
		return AuctionIdCountJSONSerdeG{}, nil
	} else if serdeFormat == commtypes.MSGP {
		return AuctionIdCountMsgpSerdeG{}, nil
	} else {
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}
