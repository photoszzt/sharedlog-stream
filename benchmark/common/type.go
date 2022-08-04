package common

import (
	"time"
)

const (
	SrcConsumeTimeout = 1 * time.Second
	ClientRetryTimes  = 100
	CommitDuration    = time.Duration(100) * time.Millisecond
	FlushDuration     = time.Duration(100) * time.Millisecond
)

type QueryInput struct {
	TestParams       map[string]bool `json:"testParams,omitempty"`
	AppId            string          `json:"aid"`
	MongoAddr        string          `json:"mongoAddr,omitempty"`
	NumOutPartitions []uint8         `json:"numOutPartition,omitempty"`
	OutputTopicNames []string        `json:"outputTopicName,omitempty"`
	InputTopicNames  []string        `json:"inputTopicName,omitempty"`
	CommitEveryMs    uint64          `json:"commEveryMs,omitempty"`
	FlushMs          uint32          `json:"flushMs,omitempty"`
	WarmupS          uint32          `json:"warmup,omitempty"`
	Duration         uint32          `json:"duration,omitempty"`
	ScaleEpoch       uint16          `json:"epoch"`
	WaitForEndMark   bool            `json:"waitEnd,omitempty"`
	NumInPartition   uint8           `json:"numInPartition,omitempty"`
	ParNum           uint8           `json:"ParNum,omitempty"`
	GuaranteeMth     uint8           `json:"gua,omitempty"`
	SerdeFormat      uint8           `json:"serdeFormat,omitempty"`
	TableType        uint8           `json:"tabT,omitempty"`
}

type ConfigScaleInput struct {
	Config      map[string]uint8 `json:"sg,omitempty"`
	AppId       string           `json:"aid,omitempty"`
	FuncNames   []string         `json:"fns,omitempty"`
	ScaleEpoch  uint16           `json:"epoch,omitempty"`
	SerdeFormat uint8            `json:"serdeFormat,omitempty"`
	Bootstrap   bool             `json:"bs,omitempty"`
}

type DumpInput struct {
	DumpDir       string `json:"dumpDir"`
	TopicName     string `json:"tp"`
	KeySerde      string `json:"keySerde"`
	ValueSerde    string `json:"valSerde"`
	NumPartitions uint8  `json:"numPartitions"`
	SerdeFormat   uint8  `json:"serdeFormat"`
}

type TestSourceInput struct {
	FileName    string `json:"fname"`
	SerdeFormat uint8  `json:"serdeFormat"`
}

type FnOutput struct {
	Latencies map[string][]int  `json:"latencies,omitempty"`
	Counts    map[string]uint64 `json:"counts,omitempty"`
	Err       error             `json:"-"`
	Message   string            `json:"message,omitempty"`
	Duration  float64           `json:"duration,omitempty"`
	Success   bool              `json:"success,omitempty"`
}

func GenErrFnOutput(err error) *FnOutput {
	return &FnOutput{Success: false, Message: err.Error()}
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
	WarmUpTime      uint32 `json:"wt"`
	WarmUpEvents    uint32 `json:"we"`
	FlushMs         uint32 `json:"flushMs"`
	SerdeFormat     uint8  `json:"serdeFormat"`
	NumOutPartition uint8  `json:"numOutPar,omitempty"`
}
