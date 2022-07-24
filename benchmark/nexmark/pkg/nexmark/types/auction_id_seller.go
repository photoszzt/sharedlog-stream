//go:generate msgp
//msgp:ignore AuctionIdSellerJSONSerde AuctionIdSellerMsgpSerde
package types

import (
	"encoding/json"
	"fmt"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
)

type AuctionIdSeller struct {
	AucId  uint64 `json:"aucId,omitempty" msg:"aucId,omitempty"`
	Seller uint64 `json:"seller,omitempty" msg:"seller,omitempty"`
}

func CompareAuctionIDSeller(a, b *AuctionIdSeller) int {
	if a.AucId < b.AucId {
		return -1
	} else if a.AucId == b.AucId {
		if a.Seller < b.Seller {
			return -1
		} else if a.Seller == b.Seller {
			return 0
		} else {
			return 1
		}
	} else {
		return 1
	}
}

var _ = fmt.Stringer(AuctionIdSeller{})

func (aic AuctionIdSeller) String() string {
	return fmt.Sprintf("AuctionIdSeller: {AucID: %d, Seller: %d}", aic.AucId, aic.Seller)
}

func CastToAuctionIdSeller(value interface{}) *AuctionIdSeller {
	val, ok := value.(*AuctionIdSeller)
	if !ok {
		valTmp := value.(AuctionIdSeller)
		val = &valTmp
	}
	return val
}

type AuctionIdSellerJSONSerde struct{}
type AuctionIdSellerJSONSerdeG struct{}

var _ = commtypes.Serde(AuctionIdSellerJSONSerde{})
var _ = commtypes.SerdeG[AuctionIdSeller](AuctionIdSellerJSONSerdeG{})

func (s AuctionIdSellerJSONSerde) Encode(value interface{}) ([]byte, error) {
	v := CastToAuctionIdSeller(value)
	return json.Marshal(v)
}

func (s AuctionIdSellerJSONSerde) Decode(value []byte) (interface{}, error) {
	v := AuctionIdSeller{}
	err := json.Unmarshal(value, &v)
	if err != nil {
		return nil, err
	}
	return v, nil
}

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

type AuctionIdSellerMsgpSerde struct{}
type AuctionIdSellerMsgpSerdeG struct{}

var _ = commtypes.Serde(AuctionIdSellerMsgpSerde{})
var _ = commtypes.SerdeG[AuctionIdSeller](AuctionIdSellerMsgpSerdeG{})

func (s AuctionIdSellerMsgpSerde) Encode(value interface{}) ([]byte, error) {
	v := CastToAuctionIdSeller(value)
	return v.MarshalMsg(nil)
}

func (s AuctionIdSellerMsgpSerde) Decode(value []byte) (interface{}, error) {
	v := AuctionIdSeller{}
	_, err := v.UnmarshalMsg(value)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (s AuctionIdSellerMsgpSerdeG) Encode(value AuctionIdSeller) ([]byte, error) {
	return value.MarshalMsg(nil)
}

func (s AuctionIdSellerMsgpSerdeG) Decode(value []byte) (AuctionIdSeller, error) {
	v := AuctionIdSeller{}
	_, err := v.UnmarshalMsg(value)
	if err != nil {
		return AuctionIdSeller{}, err
	}
	return v, nil
}

func GetAuctionIDSellerSerde(serdeFormat commtypes.SerdeFormat) (commtypes.Serde, error) {
	switch serdeFormat {
	case commtypes.JSON:
		return AuctionIdSellerJSONSerde{}, nil
	case commtypes.MSGP:
		return AuctionIdSellerMsgpSerde{}, nil
	default:
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
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
