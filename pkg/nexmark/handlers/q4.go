package handlers

import (
	"context"

	"cs.utexas.edu/zjia/faas/types"
)

type query4Handler struct {
	env types.Environment
}

func NewQuery4(env types.Environment) types.FuncHandler {
	return &query4Handler{
		env: env,
	}
}

func (h *query4Handler) Call(ctx context.Context, input []byte) ([]byte, error) {
	return nil, nil
}
