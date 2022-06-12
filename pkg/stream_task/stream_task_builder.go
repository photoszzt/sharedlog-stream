package stream_task

import "sharedlog-stream/benchmark/common"

type StreamTaskBuilder struct {
	task *StreamTask
}

type SetAppProcessFunc interface {
	AppProcessFunc(process ProcessFunc) SetInitFunc
}

type SetInitFunc interface {
	InitFunc(i func(progArgs interface{})) BuildStreamTask
}

type BuildStreamTask interface {
	Build() *StreamTask
	PauseFunc(p func() *common.FnOutput) BuildStreamTask
	ResumeFunc(r func(task *StreamTask)) BuildStreamTask
	HandleErrFunc(he func() error) BuildStreamTask
}

func NewStreamTaskBuilder() SetAppProcessFunc {
	return &StreamTaskBuilder{
		task: &StreamTask{
			CurrentOffset:            make(map[string]uint64),
			trackEveryForAtLeastOnce: common.CommitDuration,
			pauseFunc:                nil,
			resumeFunc:               nil,
			initFunc:                 nil,
			HandleErrFunc:            nil,
			appProcessFunc:           nil,
		},
	}
}

func (b *StreamTaskBuilder) AppProcessFunc(process ProcessFunc) SetInitFunc {
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
func (b *StreamTaskBuilder) InitFunc(i func(progArgs interface{})) BuildStreamTask {
	b.task.initFunc = i
	return b
}
func (b *StreamTaskBuilder) HandleErrFunc(he func() error) BuildStreamTask {
	b.task.HandleErrFunc = he
	return b
}
