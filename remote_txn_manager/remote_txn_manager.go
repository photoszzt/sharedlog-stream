package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sharedlog-stream/benchmark/common"
	"strconv"

	ipc "cs.utexas.edu/zjia/faas/ipc"
	"cs.utexas.edu/zjia/faas/types"
	"cs.utexas.edu/zjia/faas/worker"
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
}
