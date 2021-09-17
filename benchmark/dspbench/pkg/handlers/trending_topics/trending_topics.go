package trending_topics

import (
	"context"

	"cs.utexas.edu/zjia/faas/types"
)

type trendingTopicsHandler struct {
	env types.Environment
}

func NewTrendingTopics(env types.Environment) types.FuncHandler {
	return &trendingTopicsHandler{
		env: env,
	}
}

func (h *trendingTopicsHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	return nil, nil
}
