package stream_task

import (
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/pkg/exactly_once_intr"
	"sharedlog-stream/pkg/stats"
)

type StreamTaskBuilder struct {
	task *StreamTask
}

type (
	ResumeFuncType func(task *StreamTask, gua exactly_once_intr.GuaranteeMth)
	PauseFuncType  func(gua exactly_once_intr.GuaranteeMth) *common.FnOutput
)

type BuildStreamTask interface {
	Build() *StreamTask
	AppProcessFunc(process ProcessFunc) BuildStreamTask
	InitFunc(i func(task *StreamTask)) BuildStreamTask
	PauseFunc(p PauseFuncType) BuildStreamTask
	ResumeFunc(r ResumeFuncType) BuildStreamTask
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
			flushStageTime:   stats.NewPrintLogStatsCollector[int64]("flushStage(us)"),
			flushAtLeastOne:  stats.NewPrintLogStatsCollector[int64]("flushAtLeastOne(us)"),
			commitTxnAPITime: stats.NewPrintLogStatsCollector[int64]("commitTxnAPITime(us)"),
			sendOffsetTime:   stats.NewPrintLogStatsCollector[int64]("sendOffsetTime(us)"),
			txnCommitTime:    stats.NewPrintLogStatsCollector[int64]("txnCommitTime(us)"),
			markPartUs:       stats.NewPrintLogStatsCollector[int64]("markPart(us)"),
			epochMarkTime:    stats.NewPrintLogStatsCollector[int64]("epochMarkTime(us)"),

			markEpochAppend:  stats.NewPrintLogStatsCollector[int64]("appendEpochMark(us)"),
			markEpochPrepare: stats.NewPrintLogStatsCollector[int64]("markEpochPrepare(us)"),
			waitPrevTxn:      stats.NewPrintLogStatsCollector[int64]("waitPrevTxn"),
			markerSize:       stats.NewPrintLogStatsCollector[int]("epochMarkerSize(B)"),
			epochMarkTimes:   0,
			isFinalStage:     false,
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

func (b *StreamTaskBuilder) PauseFunc(p PauseFuncType) BuildStreamTask {
	b.task.pauseFunc = p
	return b
}

func (b *StreamTaskBuilder) ResumeFunc(r ResumeFuncType) BuildStreamTask {
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
