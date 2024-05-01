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

	ipc "cs.utexas.edu/zjia/faas/ipc"
	"cs.utexas.edu/zjia/faas/types"
	"cs.utexas.edu/zjia/faas/worker"
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
	return &emptyFuncHanlder{}, nil
}

func (f *emptyFuncHandlerFactory) GrpcNew(env types.Environment, service string) (types.GrpcFuncHandler, error) {
	return nil, fmt.Errorf("not implemented")
}

func main() {
	serde := os.Getenv("RTX_SERDE_FORMAT")
	if serde == "" {
		serde = "msgp"
		log.Printf("serdeFormat default to msgp")
	}
	var port int
	var err error
	portStr := os.Getenv("RTC_PORT")
	if portStr == "" {
		port = 50051
	} else {
		port, err = strconv.Atoi(portStr)
		if err != nil {
			log.Fatalf("[FATAL] Failed to read rtx port")
		}
	}
	serdeFormat := common.GetSerdeFormat(serde)
	ipc.SetRootPathForIpc(os.Getenv("FAAS_ROOT_PATH_FOR_IPC"))
	funcId, err := strconv.Atoi(os.Getenv("FAAS_FUNC_ID"))
	if err != nil {
		log.Fatal("[FATAL] Failed to parse FAAS_FUNC_ID")
	}
	clientId, err := strconv.Atoi(os.Getenv("FAAS_CLIENT_ID"))
	if err != nil {
		log.Fatal("[FATAL] Failed to parse FAAS_CLIENT_ID")
	}
	w, err := worker.NewFuncWorker(uint16(funcId), uint16(clientId), &emptyFuncHandlerFactory{})
	if err != nil {
		log.Fatal("[FATAL] Failed to create FuncWorker: ", err)
	}
	go func(w *worker.FuncWorker) {
		w.Run()
	}(w)
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		log.Fatalf("failed to listen %v: %v", port, err)
	}
	grpcServer := grpc.NewServer()
	remote_txn_rpc.RegisterRemoteTxnMngrServer(grpcServer, transaction.NewRemoteTxnManager(w, serdeFormat))
	err = grpcServer.Serve(lis)
	if err != nil {
		log.Fatalf("failed to serve grpc server: %v", err)
	}
}
