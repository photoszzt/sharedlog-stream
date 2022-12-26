package stream_task

import (
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/pkg/stats"
)

type StreamTaskBuilder struct {
	task *StreamTask
}

type BuildStreamTask interface {
	Build() *StreamTask
	AppProcessFunc(process ProcessFunc) BuildStreamTask
	InitFunc(i func(task *StreamTask)) BuildStreamTask
	PauseFunc(p func() *common.FnOutput) BuildStreamTask
	ResumeFunc(r func(task *StreamTask)) BuildStreamTask
	HandleErrFunc(he func() error) BuildStreamTask
	MarkFinalStage() BuildStreamTask
}

func NewStreamTaskBuilder() BuildStreamTask {
	return &StreamTaskBuilder{
		task: &StreamTask{
			pauseFunc:        nil,
			resumeFunc:       nil,
			initFunc:         nil,
			HandleErrFunc:    nil,
			commitTxnAPITime: stats.NewPrintLogStatsCollector[int64]("commitTxnAPITime"),
			sendOffsetTime:   stats.NewPrintLogStatsCollector[int64]("sendOffsetTime"),
			txnCommitTime:    stats.NewPrintLogStatsCollector[int64]("txnCommitTime"),

			// markEpochTime:    stats.NewStatsCollector[int64]("markEpochTime", stats.DEFAULT_COLLECT_DURATION),
			// markEpochPrepare: stats.NewStatsCollector[int64]("markEpochPrepare", stats.DEFAULT_COLLECT_DURATION),
			flushStageTime:  stats.NewPrintLogStatsCollector[int64]("flushStage"),
			flushAtLeastOne: stats.NewPrintLogStatsCollector[int64]("flushAtLeastOne"),
			epochMarkTimes:  0,
			isFinalStage:    false,
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

func (b *StreamTaskBuilder) MarkFinalStage() BuildStreamTask {
	b.task.isFinalStage = true
	return b
}
