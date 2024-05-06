package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/transaction"
	"sharedlog-stream/pkg/transaction/remote_txn_rpc"
	"strconv"
	"sync/atomic"

	"cs.utexas.edu/zjia/faas"
	"cs.utexas.edu/zjia/faas/types"
	"google.golang.org/grpc"
)

type emptyFuncHandlerFactory struct {
	serdeFormat commtypes.SerdeFormat
	port        int
	lunched     atomic.Bool
}

func init() {
	common.SetLogLevelFromEnv()
}

type emptyFuncHanlder struct {
	env types.Environment
}

func (h *emptyFuncHanlder) Call(ctx context.Context, input []byte) ([]byte, error) {
	return nil, nil
}

func (f *emptyFuncHandlerFactory) New(env types.Environment, funcName string) (types.FuncHandler, error) {
	if !f.lunched.Load() {
		f.lunched.Store(true)
		lis, err := net.Listen("tcp", fmt.Sprintf(":%d", f.port))
		if err != nil {
			log.Printf("failed to listen %v: %v", f.port, err)
			return &emptyFuncHanlder{
				env: env,
			}, nil
		}
		grpcServer := grpc.NewServer()
		remote_txn_rpc.RegisterRemoteTxnMngrServer(grpcServer, transaction.NewRemoteTxnManager(env, f.serdeFormat))
		go func() {
			err = grpcServer.Serve(lis)
			if err != nil {
				log.Fatalf("failed to serve grpc server: %v", err)
			}
		}()
	}
	return &emptyFuncHanlder{
		env: env,
	}, nil
}

func (f *emptyFuncHandlerFactory) GrpcNew(env types.Environment, service string) (types.GrpcFuncHandler, error) {
	return nil, fmt.Errorf("not implemented")
}

func main() {
	var port int
	var err error
	serde := os.Getenv("RTX_SERDE_FORMAT")
	if serde == "" {
		serde = "msgp"
		log.Printf("serdeFormat default to msgp")
	}
	portStr := os.Getenv("RTX_PORT")
	if portStr == "" {
		port = 5050
	} else {
		port, err = strconv.Atoi(portStr)
		if err != nil {
			log.Fatalf("[FATAL] Failed to read rtx port")
		}
	}
	serdeFormat := common.GetSerdeFormat(serde)
	log.Printf("[INFO] port %v, serdeFormat %v\n", port, serdeFormat)
	factory := &emptyFuncHandlerFactory{
		serdeFormat: serdeFormat,
		port:        port,
	}
	factory.lunched.Store(false)
	faas.Serve(factory)
}
