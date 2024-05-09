package txn_data

import (
	"encoding/json"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
)

type OffsetRecordJSONSerdeG struct {
	commtypes.DefaultJSONSerde
}

var _ = commtypes.SerdeG[OffsetRecord](OffsetRecordJSONSerdeG{})

func (s OffsetRecordJSONSerdeG) Encode(value OffsetRecord) ([]byte, *[]byte, error) {
	r, err := json.Marshal(value)
	return r, nil, err
}

func (s OffsetRecordJSONSerdeG) Decode(value []byte) (OffsetRecord, error) {
	v := OffsetRecord{}
	if err := json.Unmarshal(value, &v); err != nil {
		return OffsetRecord{}, err
	}
	return v, nil
}

type OffsetRecordMsgpSerdeG struct {
	commtypes.DefaultMsgpSerde
}

var _ = commtypes.SerdeG[OffsetRecord](OffsetRecordMsgpSerdeG{})

func (s OffsetRecordMsgpSerdeG) Encode(value OffsetRecord) ([]byte, *[]byte, error) {
	b := commtypes.PopBuffer()
	buf := *b
	r, err := value.MarshalMsg(buf[:0])
	return r, b, err
}

func (s OffsetRecordMsgpSerdeG) Decode(value []byte) (OffsetRecord, error) {
	v := OffsetRecord{}
	if _, err := v.UnmarshalMsg(value); err != nil {
		return OffsetRecord{}, err
	}
	return v, nil
}

func GetOffsetRecordSerdeG(serdeFormat commtypes.SerdeFormat) (commtypes.SerdeG[OffsetRecord], error) {
	if serdeFormat == commtypes.JSON {
		return OffsetRecordJSONSerdeG{}, nil
	} else if serdeFormat == commtypes.MSGP {
		return OffsetRecordMsgpSerdeG{}, nil
	} else {
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}
