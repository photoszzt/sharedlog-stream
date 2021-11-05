package commands

import "sharedlog-stream/benchmark/openmessage/benchmark_framework/utils/distributor"

type ConsumerAssignment struct {
	TopicsSubscriptions []TopicSubscription
}

type CounterStats struct {
	MessageSent     uint64
	MessageReceived uint64
}

type CumulativeLatencies struct {
	PublishLatencyBytes  []byte
	EndToEndLatencyBytes []byte
}

type PeriodStats struct {
	PublishLatencyBytes   []byte
	EndToEndLatencyBytes  []byte
	MessagesReceived      uint64
	BytesReceived         uint64
	TotalMessageSent      uint64
	TotalMessagesReceived uint64
	MessageSent           uint64
	BytesSent             uint64
}

func NewPeriodStats() *PeriodStats {
	return &PeriodStats{
		MessageSent:           0,
		BytesSent:             0,
		MessagesReceived:      0,
		BytesReceived:         0,
		TotalMessageSent:      0,
		TotalMessagesReceived: 0,
	}
}

type ProducerWorkAssignment struct {
	PayloadData        []byte
	PublishRate        float64
	KeyDistributorType distributor.KeyDistributorType
}

type TopicSubscription struct {
	Topic        string
	Subscription string
	Partition    uint32
}

type TopicsInfo struct {
	NumTopics             uint32
	NumPartitionsPerTopic uint32
}
