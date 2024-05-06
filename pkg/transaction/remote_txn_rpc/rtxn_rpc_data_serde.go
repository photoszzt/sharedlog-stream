package remote_txn_rpc

import commtypes "sharedlog-stream/pkg/commtypes"

type RTxnArgJSONSerdeG struct{}

type RTxnArgMsgpSerdeG struct{}

var _ = commtypes.SerdeG[*RTxnArg](RTxnArgMsgpSerdeG{})

func (e RTxnArgMsgpSerdeG) Encode(value *RTxnArg) ([]byte, error) {
	return value.MarshalMsg(nil)
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
