package commands

type PeriodStats struct {
	MessageSent           uint64
	BytesSent             uint64
	MessagesReceived      uint64
	BytesReceived         uint64
	TotalMessageSent      uint64
	TotalMessagesReceived uint64
	PublishLatencyBytes   []byte
	EndToEndLatencyBytes  []byte
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
