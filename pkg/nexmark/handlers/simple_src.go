package handlers

import (
	"context"

	"cs.utexas.edu/zjia/faas/types"
)

type simpleSrcHandler struct {
	env types.Environment
}

func NewSimpleSrcHandler(env types.Environment) types.FuncHandler {
	return &simpleSrcHandler{
		env: env,
	}
}

func (h *simpleSrcHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	return nil, nil
}
