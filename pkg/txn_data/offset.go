//go:generate msgp
//msgp:ignore OffsetRecordJSONSerde OffsetRecordMsgpSerde
package txn_data

import (
	"encoding/json"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
)

type OffsetRecord struct {
	Offset    uint64 `json:"offset" msg:"os"`
	TaskId    uint64 `json:"aid" msg:"aid"`
	TaskEpoch uint16 `json:"ae" msg:"ae"`
}

type OffsetRecordJSONSerde struct{}

func (s OffsetRecordJSONSerde) Encode(value interface{}) ([]byte, error) {
	or := value.(*OffsetRecord)
	return json.Marshal(or)
}

func (s OffsetRecordJSONSerde) Decode(value []byte) (interface{}, error) {
	or := OffsetRecord{}
	if err := json.Unmarshal(value, &or); err != nil {
		return nil, err
	}
	return or, nil
}

type OffsetRecordMsgpSerde struct{}

func (s OffsetRecordMsgpSerde) Encode(value interface{}) ([]byte, error) {
	or := value.(*OffsetRecord)
	return or.MarshalMsg(nil)
}

func (s OffsetRecordMsgpSerde) Decode(value []byte) (interface{}, error) {
	or := OffsetRecord{}
	if _, err := or.UnmarshalMsg(value); err != nil {
		return nil, err
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
