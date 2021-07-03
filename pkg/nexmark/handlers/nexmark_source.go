package handlers

import (
	"context"

	"cs.utexas.edu/zjia/faas/types"
)

type nexmarkSourceHandler struct {
	env types.Environment
}

func NewNexmarkSource(env types.Environment) types.FuncHandler {
	return &nexmarkSourceHandler{
		env: env,
	}
}

func (h *nexmarkSourceHandler) Call(ctx context.Context, input []byte) ([]byte, error) {

	return nil, nil
}
