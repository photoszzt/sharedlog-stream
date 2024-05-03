package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/pkg/transaction"
	"sharedlog-stream/pkg/transaction/remote_txn_rpc"
	"strconv"

	"cs.utexas.edu/zjia/faas"
	"cs.utexas.edu/zjia/faas/types"
	"google.golang.org/grpc"
)

type emptyFuncHandlerFactory struct{}

func init() {
	common.SetLogLevelFromEnv()
}

type emptyFuncHanlder struct{}

func (h *emptyFuncHanlder) Call(ctx context.Context, input []byte) ([]byte, error) {
	return nil, nil
}

func (f *emptyFuncHandlerFactory) New(env types.Environment, funcName string) (types.FuncHandler, error) {
	var port int
	var err error
	serde := os.Getenv("RTX_SERDE_FORMAT")
	if serde == "" {
		serde = "msgp"
		log.Printf("serdeFormat default to msgp")
	}
	portStr := os.Getenv("RTX_PORT")
	if portStr == "" {
		port = 50052
	} else {
		port, err = strconv.Atoi(portStr)
		if err != nil {
			log.Fatalf("[FATAL] Failed to read rtx port")
		}
	}
	serdeFormat := common.GetSerdeFormat(serde)
	log.Printf("[INFO] port %v, serdeFormat %v\n", port, serdeFormat)
	lis, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		log.Fatalf("failed to listen %v: %v", port, err)
	}
	grpcServer := grpc.NewServer()
	remote_txn_rpc.RegisterRemoteTxnMngrServer(grpcServer, transaction.NewRemoteTxnManager(env, serdeFormat))
	go func() {
		err = grpcServer.Serve(lis)
		if err != nil {
			log.Fatalf("failed to serve grpc server: %v", err)
		}
	}()
	return &emptyFuncHanlder{}, nil
}

func (f *emptyFuncHandlerFactory) GrpcNew(env types.Environment, service string) (types.GrpcFuncHandler, error) {
	return nil, fmt.Errorf("not implemented")
}

func main() {
	faas.Serve(&emptyFuncHandlerFactory{})
}
