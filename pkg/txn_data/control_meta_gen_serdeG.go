package txn_data

import (
	"encoding/json"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
)

type ControlMetadataJSONSerdeG struct {
	commtypes.DefaultJSONSerde
}

var _ = commtypes.SerdeG[ControlMetadata](ControlMetadataJSONSerdeG{})

func (s ControlMetadataJSONSerdeG) Encode(value ControlMetadata) ([]byte, *[]byte, error) {
	r, err := json.Marshal(value)
	return r, nil, err
}

func (s ControlMetadataJSONSerdeG) Decode(value []byte) (ControlMetadata, error) {
	v := ControlMetadata{}
	if err := json.Unmarshal(value, &v); err != nil {
		return ControlMetadata{}, err
	}
	return v, nil
}

type ControlMetadataMsgpSerdeG struct {
	commtypes.DefaultMsgpSerde
}

var _ = commtypes.SerdeG[ControlMetadata](ControlMetadataMsgpSerdeG{})

func (s ControlMetadataMsgpSerdeG) Encode(value ControlMetadata) ([]byte, *[]byte, error) {
	b := commtypes.PopBuffer()
	buf := *b
	r, err := value.MarshalMsg(buf[:0])
	return r, b, err
}

func (s ControlMetadataMsgpSerdeG) Decode(value []byte) (ControlMetadata, error) {
	v := ControlMetadata{}
	if _, err := v.UnmarshalMsg(value); err != nil {
		return ControlMetadata{}, err
	}
	return v, nil
}

func GetControlMetadataSerdeG(serdeFormat commtypes.SerdeFormat) (commtypes.SerdeG[ControlMetadata], error) {
	if serdeFormat == commtypes.JSON {
		return ControlMetadataJSONSerdeG{}, nil
	} else if serdeFormat == commtypes.MSGP {
		return ControlMetadataMsgpSerdeG{}, nil
	} else {
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}