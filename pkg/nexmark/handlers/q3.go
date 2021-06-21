package handlers

import (
	"context"

	"cs.utexas.edu/zjia/faas/types"
)

type query3Handler struct {
	env types.Environment
}

func NewQuery3(env types.Environment) types.FuncHandler {
	return &query3Handler{
		env: env,
	}
}

func (h *query3Handler) Call(ctx context.Context, input []byte) ([]byte, error) {
	return nil, nil
}
