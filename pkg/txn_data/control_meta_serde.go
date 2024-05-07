package txn_data

import (
	"encoding/json"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
)

type ControlMetadataJSONSerde struct{}
type ControlMetadataJSONSerdeG struct{}

var _ = commtypes.Serde(ControlMetadataJSONSerde{})
var _ = commtypes.SerdeG[ControlMetadata](ControlMetadataJSONSerdeG{})

func (s ControlMetadataJSONSerde) Encode(value interface{}) ([]byte, error) {
	rf := value.(ControlMetadata)
	return json.Marshal(&rf)
}

func (s ControlMetadataJSONSerde) Decode(value []byte) (interface{}, error) {
	rf := ControlMetadata{}
	if err := json.Unmarshal(value, &rf); err != nil {
		return nil, err
	}
	return rf, nil
}

func (s ControlMetadataJSONSerdeG) Encode(value ControlMetadata) ([]byte, error) {
	return json.Marshal(&value)
}

func (s ControlMetadataJSONSerdeG) Decode(value []byte) (ControlMetadata, error) {
	rf := ControlMetadata{}
	if err := json.Unmarshal(value, &rf); err != nil {
		return ControlMetadata{}, err
	}
	return rf, nil
}

type ControlMetadataMsgpSerde struct{}
type ControlMetadataMsgpSerdeG struct{}

var _ = commtypes.Serde(ControlMetadataMsgpSerde{})
var _ = commtypes.SerdeG[ControlMetadata](ControlMetadataMsgpSerdeG{})

func (s ControlMetadataMsgpSerde) Encode(value interface{}) ([]byte, error) {
	rf := value.(*ControlMetadata)
	return rf.MarshalMsg(nil)
}

func (s ControlMetadataMsgpSerde) Decode(value []byte) (interface{}, error) {
	rf := ControlMetadata{}
	if _, err := rf.UnmarshalMsg(value); err != nil {
		return nil, err
	}
	return rf, nil
}

func (s ControlMetadataMsgpSerdeG) Encode(value ControlMetadata) ([]byte, error) {
	b := commtypes.PopBuffer()
	buf := *b
	return value.MarshalMsg(buf[:0])
}

func (s ControlMetadataMsgpSerdeG) Decode(value []byte) (ControlMetadata, error) {
	rf := ControlMetadata{}
	if _, err := rf.UnmarshalMsg(value); err != nil {
		return ControlMetadata{}, err
	}
	return rf, nil
}

func GetControlMetadataSerde(serdeFormat commtypes.SerdeFormat) (commtypes.Serde, error) {
	switch serdeFormat {
	case commtypes.JSON:
		return ControlMetadataJSONSerde{}, nil
	case commtypes.MSGP:
		return ControlMetadataMsgpSerde{}, nil
	default:
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}

func GetControlMetadataSerdeG(serdeFormat commtypes.SerdeFormat) (commtypes.SerdeG[ControlMetadata], error) {
	switch serdeFormat {
	case commtypes.JSON:
		return ControlMetadataJSONSerdeG{}, nil
	case commtypes.MSGP:
		return ControlMetadataMsgpSerdeG{}, nil
	default:
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}
