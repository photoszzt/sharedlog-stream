package remote_txn_rpc

import commtypes "sharedlog-stream/pkg/commtypes"

type (
	RTxnArgJSONSerdeG struct{}
	RTxnArgMsgpSerdeG struct {
		commtypes.DefaultMsgpSerde
	}
)

var _ = commtypes.SerdeG[*RTxnArg](RTxnArgMsgpSerdeG{})

func (e RTxnArgMsgpSerdeG) Encode(value *RTxnArg) ([]byte, error) {
	b := commtypes.PopBuffer()
	buf := *b
	return value.MarshalMsg(buf[:0])
}

func (emd RTxnArgMsgpSerdeG) Decode(value []byte) (*RTxnArg, error) {
	e := RTxnArg{}
	_, err := e.UnmarshalMsg(value)
	if err != nil {
		return nil, err
	}
	return &e, nil
}

func GetRTxnArgSerdeG() commtypes.SerdeG[*RTxnArg] {
	return RTxnArgMsgpSerdeG{}
}
