package transaction

import (
	"sharedlog-stream/pkg/commtypes"

	"cs.utexas.edu/zjia/faas/types"
)

type RemoteTxnManager struct {
	env         types.Environment
	serdeFormat commtypes.SerdeFormat
}

func NewRemoteTxnManager(env types.Environment, serdeFormat commtypes.SerdeFormat) (*RemoteTxnManager, error) {
	tm := &RemoteTxnManager{
		env:         env,
		serdeFormat: serdeFormat,
	}
	return tm, nil
}
