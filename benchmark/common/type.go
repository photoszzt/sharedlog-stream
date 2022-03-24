package common

import (
	"time"
)

const (
	SrcConsumeTimeout = 20 * time.Second
	ClientRetryTimes  = 100
)

type QueryInput struct {
	TestParams        map[string]bool `json:"testParams,omitempty"`
	OutputTopicName   string          `json:"outputTopicName,omitempty"`
	AppId             string          `json:"aid"`
	MongoAddr         string          `json:"mongoAddr,omitempty"`
	InputTopicNames   []string        `json:"inputTopicName,omitempty"`
	CommitEveryMs     uint64          `json:"commEveryMs,omitempty"`
	CommitEveryNIter  uint32          `json:"commEveryNIter,omitempty"`
	ExitAfterNCommit  uint32          `json:"exitAfterNCommit,omitempty"`
	Duration          uint32          `json:"duration,omitempty"`
	NumInPartition    uint8           `json:"numInPartition,omitempty"`
	NumOutPartition   uint8           `json:"numOutPartition,omitempty"`
	ParNum            uint8           `json:"ParNum,omitempty"`
	EnableTransaction bool            `json:"enTran,omitempty"`
	SerdeFormat       uint8           `json:"serdeFormat,omitempty"`
	TableType         uint8           `json:"tabT,omitempty"`
}

type ConfigScaleInput struct {
	Config      map[string]uint8 `json:"sg"`
	AppId       string           `json:"aid"`
	SerdeFormat uint8            `json:"serdeFormat"`
}

type DumpInput struct {
	DumpDir       string `json:"dumpDir"`
	TopicName     string `json:"tp"`
	NumPartitions uint8  `json:"numPartitions"`
	SerdeFormat   uint8  `json:"serdeFormat"`
}

type FnOutput struct {
	Latencies map[string][]int  `json:"latencies,omitempty"`
	Consumed  map[string]uint64 `json:"consumed,omitempty"`
	Message   string            `json:"message,omitempty"`
	Duration  float64           `json:"duration,omitempty"`
	Success   bool              `json:"success"`
}

type SourceParam struct {
	TopicName       string `json:"topicName"`
	FileName        string `json:"fname,omitempty"`
	Duration        uint32 `json:"duration,omitempty"` // in sec
	SerdeFormat     uint8  `json:"serdeFormat"`
	NumEvents       uint32 `json:"numEvents,omitempty"`
	NumOutPartition uint8  `json:"numOutPar,omitempty"`
}
