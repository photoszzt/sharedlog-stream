package handlers

import (
	"context"

	"cs.utexas.edu/zjia/faas/types"
)

type query7Handler struct {
	env types.Environment
}

func NewQuery7(env types.Environment) types.FuncHandler {
	return &query7Handler{
		env: env,
	}
}

func (h *query7Handler) Call(ctx context.Context, input []byte) ([]byte, error) {
	return nil, nil
}
