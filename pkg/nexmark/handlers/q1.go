package handlers

import (
	"context"

	"cs.utexas.edu/zjia/faas/types"
)

type query1Handler struct {
	env types.Environment
}

func NewQuery1(env types.Environment) types.FuncHandler {
	return &query1Handler{
		env: env,
	}
}

func (h *query1Handler) Call(ctx context.Context, input []byte) ([]byte, error) {
	return nil, nil
}
