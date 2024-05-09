package remote_txn_rpc

import (
	"encoding/json"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
)

type RTxnReplyJSONSerdeG struct {
	commtypes.DefaultJSONSerde
}

var _ = commtypes.SerdeG[*RTxnReply](RTxnReplyJSONSerdeG{})

func (s RTxnReplyJSONSerdeG) Encode(value *RTxnReply) ([]byte, *[]byte, error) {
	r, err := json.Marshal(value)
	return r, nil, err
}

func (s RTxnReplyJSONSerdeG) Decode(value []byte) (*RTxnReply, error) {
	v := RTxnReply{}
	if err := json.Unmarshal(value, &v); err != nil {
		return nil, err
	}
	return &v, nil
}

type RTxnReplyMsgpSerdeG struct {
	commtypes.DefaultMsgpSerde
}

var _ = commtypes.SerdeG[*RTxnReply](RTxnReplyMsgpSerdeG{})

func (s RTxnReplyMsgpSerdeG) Encode(value *RTxnReply) ([]byte, *[]byte, error) {
	b := commtypes.PopBuffer()
	buf := *b
	r, err := value.MarshalMsg(buf[:0])
	return r, b, err
}

func (s RTxnReplyMsgpSerdeG) Decode(value []byte) (*RTxnReply, error) {
	v := RTxnReply{}
	if _, err := v.UnmarshalMsg(value); err != nil {
		return nil, err
	}
	return &v, nil
}

func GetRTxnReplySerdeG(serdeFormat commtypes.SerdeFormat) (commtypes.SerdeG[*RTxnReply], error) {
	if serdeFormat == commtypes.JSON {
		return RTxnReplyJSONSerdeG{}, nil
	} else if serdeFormat == commtypes.MSGP {
		return RTxnReplyMsgpSerdeG{}, nil
	} else {
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}
