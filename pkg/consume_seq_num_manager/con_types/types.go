package con_types

const (
	CONSUMER_OFFSET_LOG_TOPIC_NAME = "__offset_log"
)

type ConsumedSeqNumConfig struct {
	TopicToTrack   string
	TaskId         uint64
	ConsumedSeqNum uint64
	TaskEpoch      uint16
	Partition      uint8
}

func OffsetTopic(topicToTrack string) string {
	return CONSUMER_OFFSET_LOG_TOPIC_NAME + topicToTrack
}
