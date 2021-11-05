package worker

import (
	"sharedlog-stream/benchmark/openmessage/benchmark_framework/worker/commands"
	"sharedlog-stream/benchmark/openmessage/driver_interfaces"

	"golang.org/x/time/rate"
)

type LocalWorker struct {
	rateLimiter rate.Limiter
	benDriver   driver_interfaces.BenchmarkDriver
	producers   []driver_interfaces.BenchmarkProducer
	consumers   []driver_interfaces.BenchmarkConsumer

	msgSent   uint64
	bytesSent uint64

	msgReceived   uint64
	bytesReceived uint64

	totalMsgSent     uint64
	totalMsgReceived uint64

	testCompleted  bool
	consumerPaused bool
	producerPaused bool
}

func NewLocalWorker() *LocalWorker {
	return &LocalWorker{
		producers:        make([]driver_interfaces.BenchmarkProducer, 0, 8),
		consumers:        make([]driver_interfaces.BenchmarkConsumer, 0, 8),
		rateLimiter:      *rate.NewLimiter(rate.Limit(1), 1),
		testCompleted:    false,
		consumerPaused:   false,
		producerPaused:   false,
		msgSent:          0,
		bytesSent:        0,
		msgReceived:      0,
		bytesReceived:    0,
		totalMsgSent:     0,
		totalMsgReceived: 0,
		benDriver:        nil,
	}
}

var _ = Worker(&LocalWorker{})

func (lw *LocalWorker) InitDriver(configFile string) error {
	panic("not implemented")
}

func (lw *LocalWorker) CreateTopics(topicsInfo commands.TopicsInfo) ([]Topic, error) {
	panic("not implemented")
}

func (lw *LocalWorker) NotifyTopicCreation(topics []Topic) error {
	panic("not implemented")

}

func (lw *LocalWorker) CreateProducers(topics []string) error {
	panic("not implemented")

}

func (lw *LocalWorker) CreateConsumers(consumerAssignment commands.ConsumerAssignment) error {
	panic("not implemented")

}

func (lw *LocalWorker) StartLoad(producerWorkAssignment commands.ProducerWorkAssignment) error {
	panic("not implemented")

}

func (lw *LocalWorker) ProbeProducers() error {
	panic("not implemented")

}

func (lw *LocalWorker) AdjustPublishRate(publishRate float64) error {
	panic("not implemented")

}

func (lw *LocalWorker) PauseConsumers() error {
	panic("not implemented")

}

func (lw *LocalWorker) ResumeConsumers() error {
	panic("not implemented")

}

func (lw *LocalWorker) PauseProducers() error {
	panic("not implemented")

}

func (lw *LocalWorker) ResumeProducers() error {
	panic("not implemented")

}

func (lw *LocalWorker) GetCounterStats() error {
	panic("not implemented")

}

func (lw *LocalWorker) GetPeriodStats() error {
	panic("not implemented")

}

func (lw *LocalWorker) GetCumulativeLatencies() (commands.CumulativeLatencies, error) {
	panic("not implemented")
}

func (lw *LocalWorker) ResetStats() error {
	panic("not implemented")
}

func (lw *LocalWorker) StopAll() error {
	panic("not implemented")
}
