package handlers

import (
	"context"

	"cs.utexas.edu/zjia/faas/types"
)

type query6Handler struct {
	env types.Environment
}

func NewQuery6(env types.Environment) types.FuncHandler {
	return &query6Handler{
		env: env,
	}
}

func (h *query6Handler) Call(ctx context.Context, input []byte) ([]byte, error) {
	return nil, nil
}
