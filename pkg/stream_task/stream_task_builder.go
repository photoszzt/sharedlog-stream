package stream_task

import (
	"context"
	"sharedlog-stream/benchmark/common"
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
	PauseFunc(p func(sargs *StreamTaskArgs) *common.FnOutput) BuildStreamTask
	ResumeFunc(r func(task *StreamTask, sargs *StreamTaskArgs)) BuildStreamTask
	FlushFunc(r func(ctx context.Context, args *StreamTaskArgs) error) BuildStreamTask
	HandleErrFunc(he func() error) BuildStreamTask
}

func NewStreamTaskBuilder() SetAppProcessFunc {
	return &StreamTaskBuilder{
		task: &StreamTask{
			CurrentConsumeOffset:    make(map[string]uint64),
			pauseFunc:               nil,
			resumeFunc:              nil,
			initFunc:                nil,
			HandleErrFunc:           nil,
			appProcessFunc:          nil,
			flushFuncForAtLeastOnce: nil,
		},
	}
}

func (b *StreamTaskBuilder) AppProcessFunc(process ProcessFunc) BuildStreamTask {
	b.task.appProcessFunc = process
	return b
}

func (b *StreamTaskBuilder) Build() *StreamTask {
	if b.task.flushFuncForAtLeastOnce == nil {
		b.task.flushFuncForAtLeastOnce = b.task.flushStreams
	}
	return b.task
}
func (b *StreamTaskBuilder) PauseFunc(p func(sargs *StreamTaskArgs) *common.FnOutput) BuildStreamTask {
	b.task.pauseFunc = p
	return b
}
func (b *StreamTaskBuilder) ResumeFunc(r func(task *StreamTask, sargs *StreamTaskArgs)) BuildStreamTask {
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

func (b *StreamTaskBuilder) FlushFunc(r func(ctx context.Context, args *StreamTaskArgs) error) BuildStreamTask {
	b.task.flushFuncForAtLeastOnce = r
	return b
}
