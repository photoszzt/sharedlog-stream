package handlers

import (
	"context"

	"cs.utexas.edu/zjia/faas/types"
)

type query5Handler struct {
	env types.Environment
}

func NewQuery5(env types.Environment) types.FuncHandler {
	return &query5Handler{
		env: env,
	}
}

func (h *query5Handler) Call(ctx context.Context, input []byte) ([]byte, error) {
	return nil, nil
}
