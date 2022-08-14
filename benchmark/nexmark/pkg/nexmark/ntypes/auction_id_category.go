//go:generate msgp
//msgp:ignore AuctionIdCategoryJSONSerde AuctionIdCategoryMsgpSerde
package ntypes

import (
	"encoding/json"
	"fmt"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
)

type AuctionIdCategory struct {
	AucId    uint64 `json:"aucId,omitempty" msg:"aucId,omitempty"`
	Category uint64 `json:"cat,omitempty" msg:"cat,omitempty"`
}

var _ = fmt.Stringer(AuctionIdCategory{})

func (aic AuctionIdCategory) String() string {
	return fmt.Sprintf("AuctionIdCat: {AucID: %d, Cat: %d}", aic.AucId, aic.Category)
}

func CompareAuctionIdCategory(a, b *AuctionIdCategory) int {
	if a.AucId < b.AucId {
		return -1
	} else if a.AucId == b.AucId {
		if a.Category < b.Category {
			return -1
		} else if a.Category == b.Category {
			return 0
		} else {
			return 1
		}
	} else {
		return 1
	}
}

type AuctionIdCategoryJSONSerde struct{}

var _ = commtypes.Serde(AuctionIdCategoryJSONSerde{})

func (s AuctionIdCategoryJSONSerde) Encode(value interface{}) ([]byte, error) {
	aic, ok := value.(*AuctionIdCategory)
	if !ok {
		aicTmp := value.(AuctionIdCategory)
		aic = &aicTmp
	}
	return json.Marshal(aic)
}

func (s AuctionIdCategoryJSONSerde) Decode(value []byte) (interface{}, error) {
	v := AuctionIdCategory{}
	err := json.Unmarshal(value, &v)
	if err != nil {
		return nil, err
	}
	return v, nil
}

type AuctionIdCategoryMsgpSerde struct{}

var _ = commtypes.Serde(AuctionIdCategoryMsgpSerde{})

func (s AuctionIdCategoryMsgpSerde) Encode(value interface{}) ([]byte, error) {
	aic, ok := value.(*AuctionIdCategory)
	if !ok {
		aicTmp := value.(AuctionIdCategory)
		aic = &aicTmp
	}
	return aic.MarshalMsg(nil)
}

func (s AuctionIdCategoryMsgpSerde) Decode(value []byte) (interface{}, error) {
	aic := AuctionIdCategory{}
	_, err := aic.UnmarshalMsg(value)
	if err != nil {
		return nil, err
	}
	return aic, nil
}

func GetAuctionIdCategorySerde(serdeFormat commtypes.SerdeFormat) (commtypes.Serde, error) {
	switch serdeFormat {
	case commtypes.JSON:
		return AuctionIdCategoryJSONSerde{}, nil
	case commtypes.MSGP:
		return AuctionIdCategoryMsgpSerde{}, nil
	default:
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}
