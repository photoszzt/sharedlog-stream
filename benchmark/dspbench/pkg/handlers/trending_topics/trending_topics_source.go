package trending_topics

import (
	"context"

	"cs.utexas.edu/zjia/faas/types"
)

type trendingTopicsSourceHandler struct {
	env types.Environment
}

func NewTrendingTopicsSource(env types.Environment) types.FuncHandler {
	return &trendingTopicsSourceHandler{
		env: env,
	}
}

func (h *trendingTopicsSourceHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	return nil, nil
}
