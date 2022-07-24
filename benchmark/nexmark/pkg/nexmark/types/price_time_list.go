//go:generate msgp
//msgp:ignore PriceTimeListJSONSerde PriceTimeListMsgpSerde
package types

import (
	"encoding/json"
	"fmt"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
)

type PriceTimeSlice []PriceTime

func (t PriceTimeSlice) Len() int {
	return len(t)
}

func (t PriceTimeSlice) Less(i, j int) bool {
	return ComparePriceTime(&t[i], &t[j]) < 0
}

func (t PriceTimeSlice) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

func Delete(t PriceTimeSlice, i, j int) PriceTimeSlice {
	return append(t[:i], t[j:]...)
}

func RemoveMatching(t PriceTimeSlice, pt *PriceTime) PriceTimeSlice {
	for i, v := range t {
		if v.Price == pt.Price && v.DateTime == pt.DateTime {
			return Delete(t, i, i+1)
		}
	}
	return t
}

type PriceTimeList struct {
	PTList PriceTimeSlice `json:"ptList" msg:"ptList"`
}

var _ = fmt.Stringer(PriceTimeList{})

func (ptl PriceTimeList) String() string {
	return fmt.Sprintf("PriceTimeList: {PTList: %v}", ptl.PTList)
}

func castToPriceTimeListPtr(value interface{}) *PriceTimeList {
	ptl, ok := value.(*PriceTimeList)
	if !ok {
		ptlTmp := value.(PriceTimeList)
		ptl = &ptlTmp
	}
	return ptl
}

type PriceTimeListJSONSerde struct{}
type PriceTimeListJSONSerdeG struct{}

var _ = commtypes.Serde(PriceTimeListJSONSerde{})
var _ = commtypes.SerdeG[PriceTimeList](PriceTimeListJSONSerdeG{})

func (s PriceTimeListJSONSerde) Encode(value interface{}) ([]byte, error) {
	ptl := castToPriceTimeListPtr(value)
	return json.Marshal(ptl)
}

func (s PriceTimeListJSONSerde) Decode(value []byte) (interface{}, error) {
	ptl := PriceTimeList{}
	err := json.Unmarshal(value, &ptl)
	if err != nil {
		return nil, err
	}
	return ptl, nil
}

func (s PriceTimeListJSONSerdeG) Encode(value PriceTimeList) ([]byte, error) {
	return json.Marshal(&value)
}

func (s PriceTimeListJSONSerdeG) Decode(value []byte) (PriceTimeList, error) {
	ptl := PriceTimeList{}
	err := json.Unmarshal(value, &ptl)
	if err != nil {
		return PriceTimeList{}, err
	}
	return ptl, nil
}

type PriceTimeListMsgpSerde struct{}
type PriceTimeListMsgpSerdeG struct{}

var _ = commtypes.Serde(PriceTimeListMsgpSerde{})
var _ = commtypes.SerdeG[PriceTimeList](PriceTimeListMsgpSerdeG{})

func (s PriceTimeListMsgpSerde) Encode(value interface{}) ([]byte, error) {
	ptl := castToPriceTimeListPtr(value)
	return ptl.MarshalMsg(nil)
}

func (s PriceTimeListMsgpSerde) Decode(value []byte) (interface{}, error) {
	ptl := PriceTimeList{}
	_, err := ptl.UnmarshalMsg(value)
	if err != nil {
		return nil, err
	}
	return ptl, nil
}

func (s PriceTimeListMsgpSerdeG) Encode(value PriceTimeList) ([]byte, error) {
	return value.MarshalMsg(nil)
}

func (s PriceTimeListMsgpSerdeG) Decode(value []byte) (PriceTimeList, error) {
	ptl := PriceTimeList{}
	_, err := ptl.UnmarshalMsg(value)
	if err != nil {
		return PriceTimeList{}, err
	}
	return ptl, nil
}
func GetPriceTimeListSerde(serdeFormat commtypes.SerdeFormat) (commtypes.Serde, error) {
	switch serdeFormat {
	case commtypes.JSON:
		return PriceTimeListJSONSerde{}, nil
	case commtypes.MSGP:
		return PriceTimeListMsgpSerde{}, nil
	default:
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}

func GetPriceTimeListSerdeG(serdeFormat commtypes.SerdeFormat) (commtypes.SerdeG[PriceTimeList], error) {
	switch serdeFormat {
	case commtypes.JSON:
		return PriceTimeListJSONSerdeG{}, nil
	case commtypes.MSGP:
		return PriceTimeListMsgpSerdeG{}, nil
	default:
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}
