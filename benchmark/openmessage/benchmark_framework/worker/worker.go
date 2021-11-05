package worker

import "sharedlog-stream/benchmark/openmessage/benchmark_framework/worker/commands"

type Worker interface {
	InitDriver(configFile string) error
	CreateTopics(topicsInfo commands.TopicsInfo) ([]Topic, error)
	NotifyTopicCreation(topics []Topic) error
	CreateProducers(topics []string) error
	CreateConsumers(consumerAssignment commands.ConsumerAssignment) error
	StartLoad(producerWorkAssignment commands.ProducerWorkAssignment) error
	ProbeProducers() error
	AdjustPublishRate(publishRate float64) error
	PauseConsumers() error
	ResumeConsumers() error
	PauseProducers() error
	ResumeProducers() error
	GetCounterStats() error
	GetPeriodStats() error
	GetCumulativeLatencies() (commands.CumulativeLatencies, error)
	ResetStats() error
	StopAll() error
}
