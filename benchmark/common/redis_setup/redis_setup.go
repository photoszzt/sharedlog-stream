package redis_setup

import (
	"context"
	"encoding/json"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/pkg/checkpt"

	"cs.utexas.edu/zjia/faas/types"
)

type RedisSetup struct {
	env types.Environment
}

func NewRedisSetupHandler(env types.Environment) types.FuncHandler {
	return &RedisSetup{
		env: env,
	}
}

func (h *RedisSetup) Call(ctx context.Context, input []byte) ([]byte, error) {
	ret := &common.FnOutput{Success: true}
	rcm := checkpt.NewRedisChkptManager(ctx)
	err := rcm.InitReqRes(ctx)
	if err != nil {
		ret = common.GenErrFnOutput(err)
	}
	encodedOutput, err := json.Marshal(ret)
	if err != nil {
		panic(err)
	}
	return common.CompressData(encodedOutput), nil
}
