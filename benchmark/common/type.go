package common

import (
	"time"
)

const (
	SrcConsumeTimeout = 2 * time.Second
	ClientRetryTimes  = 100
)

type QueryInput struct {
	TestParams        map[string]bool `json:"testParams,omitempty"`
	OutputTopicNames  []string        `json:"outputTopicName,omitempty"`
	MongoAddr         string          `json:"mongoAddr,omitempty"`
	AppId             string          `json:"aid"`
	InputTopicNames   []string        `json:"inputTopicName,omitempty"`
	CommitEveryMs     uint64          `json:"commEveryMs,omitempty"`
	ScaleEpoch        uint64          `json:"epoch"`
	Duration          uint32          `json:"duration,omitempty"`
	CommitEveryNIter  uint32          `json:"commEveryNIter,omitempty"`
	ExitAfterNCommit  uint32          `json:"exitAfterNCommit,omitempty"`
	NumInPartition    uint8           `json:"numInPartition,omitempty"`
	NumOutPartitions  []uint8         `json:"numOutPartition,omitempty"`
	ParNum            uint8           `json:"ParNum,omitempty"`
	EnableTransaction bool            `json:"enTran,omitempty"`
	SerdeFormat       uint8           `json:"serdeFormat,omitempty"`
	TableType         uint8           `json:"tabT,omitempty"`
}

type ConfigScaleInput struct {
	Config      map[string]uint8 `json:"sg,omitempty"`
	AppId       string           `json:"aid,omitempty"`
	FuncNames   []string         `json:"fns,omitempty"`
	ScaleEpoch  uint64           `json:"epoch,omitempty"`
	SerdeFormat uint8            `json:"serdeFormat,omitempty"`
	Bootstrap   bool             `json:"bs,omitempty"`
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
	Err       error             `json:"-"`
	Message   string            `json:"message,omitempty"`
	Duration  float64           `json:"duration,omitempty"`
	Success   bool              `json:"success,omitempty"`
}

type SourceParam struct {
	TopicName       string `json:"topicName"`
	FileName        string `json:"fname,omitempty"`
	Duration        uint32 `json:"duration,omitempty"` // in sec
	SerdeFormat     uint8  `json:"serdeFormat"`
	NumEvents       uint32 `json:"numEvents,omitempty"`
	NumOutPartition uint8  `json:"numOutPar,omitempty"`
}

type BenchSourceParam struct {
	TopicName       string `json:"topicName"`
	FileName        string `json:"fname,omitempty"`
	Duration        uint32 `json:"duration,omitempty"`
	NumEvents       uint32 `json:"numEvents,omitempty"`
	Tps             uint32 `json:"tps"`
	WarmUpTime      uint32 `json:wt`
	WarmUpEvents    uint32 `json:we`
	SerdeFormat     uint8  `json:"serdeFormat"`
	NumOutPartition uint8  `json:"numOutPar,omitempty"`
}
