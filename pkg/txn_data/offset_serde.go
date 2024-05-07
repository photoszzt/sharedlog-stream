package txn_data

import (
	"encoding/json"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
)

type (
	OffsetRecordJSONSerde struct {
		commtypes.DefaultJSONSerde
	}
	OffsetRecordJSONSerdeG struct {
		commtypes.DefaultJSONSerde
	}
)

var (
	_ = commtypes.Serde(OffsetRecordJSONSerde{})
	_ = commtypes.SerdeG[OffsetRecord](OffsetRecordJSONSerdeG{})
)

func (s OffsetRecordJSONSerde) Encode(value interface{}) ([]byte, error) {
	or := value.(*OffsetRecord)
	return json.Marshal(or)
}

func (s OffsetRecordJSONSerdeG) Encode(value OffsetRecord) ([]byte, error) {
	return json.Marshal(&value)
}

func (s OffsetRecordJSONSerde) Decode(value []byte) (interface{}, error) {
	or := OffsetRecord{}
	if err := json.Unmarshal(value, &or); err != nil {
		return nil, err
	}
	return or, nil
}

func (s OffsetRecordJSONSerdeG) Decode(value []byte) (OffsetRecord, error) {
	or := OffsetRecord{}
	if err := json.Unmarshal(value, &or); err != nil {
		return OffsetRecord{}, err
	}
	return or, nil
}

type (
	OffsetRecordMsgpSerde struct {
		commtypes.DefaultMsgpSerde
	}
	OffsetRecordMsgpSerdeG struct {
		commtypes.DefaultMsgpSerde
	}
)

var (
	_ = commtypes.Serde(OffsetRecordMsgpSerde{})
	_ = commtypes.SerdeG[OffsetRecord](OffsetRecordMsgpSerdeG{})
)

func (s OffsetRecordMsgpSerde) Encode(value interface{}) ([]byte, error) {
	or := value.(*OffsetRecord)
	b := commtypes.PopBuffer()
	buf := *b
	return or.MarshalMsg(buf[:0])
}

func (s OffsetRecordMsgpSerdeG) Encode(value OffsetRecord) ([]byte, error) {
	b := commtypes.PopBuffer()
	buf := *b
	return value.MarshalMsg(buf[:0])
}

func (s OffsetRecordMsgpSerde) Decode(value []byte) (interface{}, error) {
	or := OffsetRecord{}
	if _, err := or.UnmarshalMsg(value); err != nil {
		return OffsetRecord{}, err
	}
	return or, nil
}

func (s OffsetRecordMsgpSerdeG) Decode(value []byte) (OffsetRecord, error) {
	or := OffsetRecord{}
	if _, err := or.UnmarshalMsg(value); err != nil {
		return OffsetRecord{}, err
	}
	return or, nil
}

func GetOffsetRecordSerde(serdeFormat commtypes.SerdeFormat) (commtypes.Serde, error) {
	if serdeFormat == commtypes.JSON {
		return OffsetRecordJSONSerde{}, nil
	} else if serdeFormat == commtypes.MSGP {
		return OffsetRecordMsgpSerde{}, nil
	} else {
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
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
