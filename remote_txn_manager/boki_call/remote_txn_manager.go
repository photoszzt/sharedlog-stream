package main

import (
	"context"
	"fmt"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/transaction"
	"sharedlog-stream/pkg/transaction/remote_txn_rpc"

	"cs.utexas.edu/zjia/faas"
	"cs.utexas.edu/zjia/faas/types"
)

type mngrFuncHandlerFactory struct{}

func init() {
	common.SetLogLevelFromEnv()
}

type mngrFuncHanlder struct {
	env types.Environment
}

func (h *mngrFuncHanlder) Call(ctx context.Context, input []byte) ([]byte, error) {
	serde := remote_txn_rpc.RTxnArgMsgpSerdeG{}
	in, err := serde.Decode(input)
	if err != nil {
		return nil, err
	}
	r := transaction.NewRemoteTxnManager(h.env, commtypes.SerdeFormat(in.SerdeFormat))
	switch in.RpcType {
	case remote_txn_rpc.Init:
		ret, err := r.Init(ctx, in.Init)
	}
	return nil, nil
}

func (f *mngrFuncHandlerFactory) New(env types.Environment, funcName string) (types.FuncHandler, error) {
	return &mngrFuncHanlder{
		env: env,
	}, nil
}

func (f *mngrFuncHandlerFactory) GrpcNew(env types.Environment, service string) (types.GrpcFuncHandler, error) {
	return nil, fmt.Errorf("not implemented")
}

func main() {
	factory := &mngrFuncHandlerFactory{}
	faas.Serve(factory)
}
