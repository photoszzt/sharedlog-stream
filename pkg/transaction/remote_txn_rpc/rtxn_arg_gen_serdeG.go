package remote_txn_rpc

import (
	"encoding/json"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
)

type RTxnArgJSONSerdeG struct {
	commtypes.DefaultJSONSerde
}

var _ = commtypes.SerdeG[*RTxnArg](RTxnArgJSONSerdeG{})

func (s RTxnArgJSONSerdeG) Encode(value *RTxnArg) ([]byte, *[]byte, error) {
	r, err := json.Marshal(value)
	return r, nil, err
}

func (s RTxnArgJSONSerdeG) Decode(value []byte) (*RTxnArg, error) {
	v := RTxnArg{}
	if err := json.Unmarshal(value, &v); err != nil {
		return nil, err
	}
	return &v, nil
}

type RTxnArgMsgpSerdeG struct {
	commtypes.DefaultMsgpSerde
}

var _ = commtypes.SerdeG[*RTxnArg](RTxnArgMsgpSerdeG{})

func (s RTxnArgMsgpSerdeG) Encode(value *RTxnArg) ([]byte, *[]byte, error) {
	b := commtypes.PopBuffer(value.Msgsize())
	buf := *b
	r, err := value.MarshalMsg(buf[:0])
	return r, b, err
}

func (s RTxnArgMsgpSerdeG) Decode(value []byte) (*RTxnArg, error) {
	v := RTxnArg{}
	if _, err := v.UnmarshalMsg(value); err != nil {
		return nil, err
	}
	return &v, nil
}

func GetRTxnArgSerdeG(serdeFormat commtypes.SerdeFormat) (commtypes.SerdeG[*RTxnArg], error) {
	if serdeFormat == commtypes.JSON {
		return RTxnArgJSONSerdeG{}, nil
	} else if serdeFormat == commtypes.MSGP {
		return RTxnArgMsgpSerdeG{}, nil
	} else {
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}
