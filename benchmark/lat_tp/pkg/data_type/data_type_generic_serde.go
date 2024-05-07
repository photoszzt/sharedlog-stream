package datatype

import (
	"encoding/json"
	"sharedlog-stream/pkg/commtypes"
)

type PayloadTsJsonSerdeG struct{}

var _ commtypes.SerdeG[PayloadTs] = PayloadTsJsonSerdeG{}

func (s PayloadTsJsonSerdeG) Encode(value PayloadTs) ([]byte, error) {
	return json.Marshal(value)
}

func (s PayloadTsJsonSerdeG) Decode(data []byte) (PayloadTs, error) {
	var value PayloadTs
	err := json.Unmarshal(data, &value)
	return value, err
}

type PayloadTsMsgpSerdeG struct{}

var _ commtypes.SerdeG[PayloadTs] = PayloadTsMsgpSerdeG{}

func (s PayloadTsMsgpSerdeG) Encode(value PayloadTs) ([]byte, error) {
	b := commtypes.PopBuffer()
	buf := *b
	return value.MarshalMsg(buf[:0])
}

func (s PayloadTsMsgpSerdeG) Decode(value []byte) (PayloadTs, error) {
	pt := PayloadTs{}
	if _, err := pt.UnmarshalMsg(value); err != nil {
		return pt, err
	}
	return pt, nil
}
