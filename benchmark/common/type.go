package common

import "time"

const (
	SrcConsumeTimeout = 5 * time.Second
)

type QueryInput struct {
	OutputTopicName   string `json:"outputTopicName"`
	InputTopicName    string `json:"inputTopicName"`
	CommitEvery       uint64 `json:"commEvery"`
	Duration          uint32 `json:"duration,omitempty"`
	NumInPartition    uint8  `json:"numInPartition"`
	NumOutPartition   uint8  `json:"numOutPartition"`
	ParNum            uint8  `json:"ParNum"`
	EnableTransaction bool   `json:"enTran"`
	SerdeFormat       uint8  `json:"serdeFormat"`
}

type DumpInput struct {
	DumpDir       string `json:"dumpDir"`
	TopicName     string `json:"tp"`
	NumPartitions uint8  `json:"numPartitions"`
	SerdeFormat   uint8  `json:"serdeFormat"`
}

type FnOutput struct {
	Latencies map[string][]int `json:"latencies"`
	Message   string           `json:"message"`
	Duration  float64          `json:"duration"`
	Success   bool             `json:"success"`
}

type SourceParam struct {
	TopicName   string `json:"topicName"`
	FileName    string `json:"fname,omitempty"`
	Duration    uint32 `json:"duration,omitempty"` // in sec
	SerdeFormat uint8  `json:"serdeFormat"`
	NumEvents   uint32 `json:"numEvents,omitempty"`
}
