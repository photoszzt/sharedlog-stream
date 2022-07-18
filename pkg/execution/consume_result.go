package execution

import "sharedlog-stream/pkg/commtypes"

type ConsumeResult struct {
	MsgSeqs *commtypes.MsgAndSeqs
	err     error
}

func ConsumeErr(err error) ConsumeResult {
	return ConsumeResult{
		err: err,
	}
}

func ConsumeVal(msgSeqs *commtypes.MsgAndSeqs) ConsumeResult {
	return ConsumeResult{
		MsgSeqs: msgSeqs,
		err:     nil,
	}
}

func (r *ConsumeResult) Valid() bool {
	return r.err == nil
}

func (r *ConsumeResult) Value() *commtypes.MsgAndSeqs {
	return r.MsgSeqs
}

func (r *ConsumeResult) Err() error {
	return r.err
}
