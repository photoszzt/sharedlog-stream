//go:generate msgp
//msgp:ignore PriceTimeJSONSerde PriceTimeMsgpSerde
package types

import (
	"encoding/json"
	"fmt"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
)

type PriceTime struct {
	Price    uint64 `json:"price" msg:"price"`
	DateTime int64  `json:"dateTime" msg:"dateTime"` // unix timestamp in ms
}

func ComparePriceTime(a, b PriceTime) int {
	if a.Price < b.Price {
		return -1
	} else if a.Price == b.Price {
		if a.DateTime < b.DateTime {
			return -1
		} else if a.DateTime == b.DateTime {
			return 0
		} else {
			return 1
		}
	} else {
		return 1
	}
}

var _ = fmt.Stringer(PriceTime{})

func (pt PriceTime) String() string {
	return fmt.Sprintf("PriceTime: {Price: %d, Ts: %d}",
		pt.Price, pt.DateTime)
}

func CastToPriceTimePtr(value interface{}) *PriceTime {
	pt, ok := value.(*PriceTime)
	if !ok {
		ptTmp := value.(PriceTime)
		pt = &ptTmp
	}
	return pt
}

type PriceTimeJSONSerde struct{}

var _ = commtypes.Serde(PriceTimeJSONSerde{})

func (s PriceTimeJSONSerde) Encode(value interface{}) ([]byte, error) {
	pt := CastToPriceTimePtr(value)
	return json.Marshal(pt)
}

func (s PriceTimeJSONSerde) Decode(value []byte) (interface{}, error) {
	pt := PriceTime{}
	if err := json.Unmarshal(value, &pt); err != nil {
		return nil, err
	}
	return pt, nil
}

type PriceTimeMsgpSerde struct{}

var _ = commtypes.Serde(PriceTimeMsgpSerde{})

func (s PriceTimeMsgpSerde) Encode(value interface{}) ([]byte, error) {
	pt := CastToPriceTimePtr(value)
	return pt.MarshalMsg(nil)
}

func (s PriceTimeMsgpSerde) Decode(value []byte) (interface{}, error) {
	pt := PriceTime{}
	if _, err := pt.UnmarshalMsg(value); err != nil {
		return nil, err
	}
	return pt, nil
}

func GetPriceTimeSerde(serdeFormat commtypes.SerdeFormat) (commtypes.Serde, error) {
	switch serdeFormat {
	case commtypes.JSON:
		return PriceTimeJSONSerde{}, nil
	case commtypes.MSGP:
		return PriceTimeMsgpSerde{}, nil
	default:
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}
