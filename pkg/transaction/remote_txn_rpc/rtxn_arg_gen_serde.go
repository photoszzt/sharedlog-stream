package remote_txn_rpc

import (
	"encoding/json"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
)

type RTxnArgJSONSerde struct {
	commtypes.DefaultJSONSerde
}

var _ = commtypes.Serde(RTxnArgJSONSerde{})

func (s RTxnArgJSONSerde) Encode(value interface{}) ([]byte, *[]byte, error) {
	v, ok := value.(*RTxnArg)
	if !ok {
		vTmp := value.(RTxnArg)
		v = &vTmp
	}
	r, err := json.Marshal(v)
	return r, nil, err
}

func (s RTxnArgJSONSerde) Decode(value []byte) (interface{}, error) {
	v := RTxnArg{}
	if err := json.Unmarshal(value, &v); err != nil {
		return nil, err
	}
	return &v, nil
}

type RTxnArgMsgpSerde struct {
	commtypes.DefaultMsgpSerde
}

var _ = commtypes.Serde(RTxnArgMsgpSerde{})

func (s RTxnArgMsgpSerde) Encode(value interface{}) ([]byte, *[]byte, error) {
	v, ok := value.(*RTxnArg)
	if !ok {
		vTmp := value.(RTxnArg)
		v = &vTmp
	}
	b := commtypes.PopBuffer()
	buf := *b
	r, err := v.MarshalMsg(buf[:0])
	return r, b, err
}

func (s RTxnArgMsgpSerde) Decode(value []byte) (interface{}, error) {
	v := RTxnArg{}
	if _, err := v.UnmarshalMsg(value); err != nil {
		return nil, err
	}
	return &v, nil
}

func GetRTxnArgSerde(serdeFormat commtypes.SerdeFormat) (commtypes.Serde, error) {
	switch serdeFormat {
	case commtypes.JSON:
		return RTxnArgJSONSerde{}, nil
	case commtypes.MSGP:
		return RTxnArgMsgpSerde{}, nil
	default:
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}
