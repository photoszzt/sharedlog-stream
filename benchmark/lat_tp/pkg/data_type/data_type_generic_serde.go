package datatype

import "sharedlog-stream/pkg/commtypes"

type PayloadTsMsgpSerdeG struct{}

var _ commtypes.SerdeG[PayloadTs] = PayloadTsMsgpSerdeG{}

func (s PayloadTsMsgpSerdeG) Encode(value PayloadTs) ([]byte, error) {
	return value.MarshalMsg(nil)
}

func (s PayloadTsMsgpSerdeG) Decode(value []byte) (PayloadTs, error) {
	pt := PayloadTs{}
	if _, err := pt.UnmarshalMsg(value); err != nil {
		return pt, err
	}
	return pt, nil
}
