package stream_task

import (
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/pkg/stats"
)

type StreamTaskBuilder struct {
	task *StreamTask
}

type SetAppProcessFunc interface {
	AppProcessFunc(process ProcessFunc) BuildStreamTask
}

type BuildStreamTask interface {
	Build() *StreamTask
	InitFunc(i func(task *StreamTask)) BuildStreamTask
	PauseFunc(p func() *common.FnOutput) BuildStreamTask
	ResumeFunc(r func(task *StreamTask)) BuildStreamTask
	HandleErrFunc(he func() error) BuildStreamTask
}

func NewStreamTaskBuilder() SetAppProcessFunc {
	return &StreamTaskBuilder{
		task: &StreamTask{
			pauseFunc:        nil,
			resumeFunc:       nil,
			initFunc:         nil,
			HandleErrFunc:    nil,
			appProcessFunc:   nil,
			flushForALO:      stats.NewInt64Collector("flushForALO", stats.DEFAULT_COLLECT_DURATION),
			commitTrTime:     stats.NewInt64Collector("commitTrTime", stats.DEFAULT_COLLECT_DURATION),
			beginTrTime:      stats.NewInt64Collector("beginTrTime", stats.DEFAULT_COLLECT_DURATION),
			markEpochTime:    stats.NewInt64Collector("markEpochTime", stats.DEFAULT_COLLECT_DURATION),
			markEpochPrepare: stats.NewInt64Collector("markEpochPrepare", stats.DEFAULT_COLLECT_DURATION),
			ctrlFlushTime:    stats.NewInt64Collector("ctrlFlushTime", stats.DEFAULT_COLLECT_DURATION),
		},
	}
}

func (b *StreamTaskBuilder) AppProcessFunc(process ProcessFunc) BuildStreamTask {
	b.task.appProcessFunc = process
	return b
}

func (b *StreamTaskBuilder) Build() *StreamTask {
	return b.task
}
func (b *StreamTaskBuilder) PauseFunc(p func() *common.FnOutput) BuildStreamTask {
	b.task.pauseFunc = p
	return b
}
func (b *StreamTaskBuilder) ResumeFunc(r func(task *StreamTask)) BuildStreamTask {
	b.task.resumeFunc = r
	return b
}
func (b *StreamTaskBuilder) InitFunc(i func(task *StreamTask)) BuildStreamTask {
	b.task.initFunc = i
	return b
}
func (b *StreamTaskBuilder) HandleErrFunc(he func() error) BuildStreamTask {
	b.task.HandleErrFunc = he
	return b
}
