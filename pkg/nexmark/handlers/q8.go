package handlers

import (
	"context"

	"cs.utexas.edu/zjia/faas/types"
)

type query8Handler struct {
	env types.Environment
}

func NewQuery8(env types.Environment) types.FuncHandler {
	return &query8Handler{
		env: env,
	}
}

func (h *query8Handler) Call(ctx context.Context, input []byte) ([]byte, error) {
	return nil, nil
}
