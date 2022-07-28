package types

import (
	"encoding/json"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
)

type EventMsgpSerdeG struct{}

var _ = commtypes.EncoderG[*Event](EventMsgpSerdeG{})

func (e EventMsgpSerdeG) Encode(value *Event) ([]byte, error) {
	return value.MarshalMsg(nil)
}

type EventJSONSerdeG struct{}

var _ = commtypes.EncoderG[*Event](EventJSONSerdeG{})

func (e EventJSONSerdeG) Encode(value *Event) ([]byte, error) {
	return json.Marshal(value)
}

var _ = commtypes.DecoderG[*Event](EventMsgpSerdeG{})

func (emd EventMsgpSerdeG) Decode(value []byte) (*Event, error) {
	e := Event{}
	_, err := e.UnmarshalMsg(value)
	if err != nil {
		return nil, err
	}
	return &e, nil
}

var _ = commtypes.DecoderG[*Event](EventJSONSerdeG{})

func (ejd EventJSONSerdeG) Decode(value []byte) (*Event, error) {
	e := Event{}
	if err := json.Unmarshal(value, &e); err != nil {
		return nil, err
	}
	return &e, nil
}

func GetEventSerdeG(serdeFormat commtypes.SerdeFormat) (commtypes.SerdeG[*Event], error) {
	if serdeFormat == commtypes.JSON {
		return EventJSONSerdeG{}, nil
	} else if serdeFormat == commtypes.MSGP {
		return EventMsgpSerdeG{}, nil
	} else {
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}
