package main

import (
	"context"
	"flag"
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

var (
	FLAGS_port        int
	FLAGS_serdeFormat string
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
	flag.IntVar(&FLAGS_port, "port", 50051, "The server port")
	flag.StringVar(&FLAGS_serdeFormat, "serde", "json", "serde format: json or msgp")
	flag.Parse()
	serdeFormat := common.GetSerdeFormat(FLAGS_serdeFormat)
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
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", FLAGS_port))
	if err != nil {
		log.Fatalf("failed to listen %v: %v", FLAGS_port, err)
	}
	grpcServer := grpc.NewServer()
	remote_txn_rpc.RegisterRemoteTxnMngrServer(grpcServer, transaction.NewRemoteTxnManager(w, serdeFormat))
	err = grpcServer.Serve(lis)
	if err != nil {
		log.Fatalf("failed to serve grpc server: %v", err)
	}
}
