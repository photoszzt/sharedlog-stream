package distributor

import (
	"encoding/base64"
	"math/rand"
	"time"
)

const (
	UNIQUE_COUNT  = 10_000
	KEY_BYTE_SIZE = 7
)

var (
	random     = rand.New(rand.NewSource(time.Now().UnixNano()))
	randomKeys = genRandomKeys()
)

func genRandomKeys() []string {
	rKeys := make([]string, UNIQUE_COUNT)
	buf := make([]byte, KEY_BYTE_SIZE)
	for i := 0; i < len(rKeys); i++ {
		_, _ = random.Read(buf)
		rKeys[i] = base64.StdEncoding.EncodeToString(buf)
	}
	return rKeys
}

type BaseKeyDistributor struct{}

func (kd *BaseKeyDistributor) Get(index uint32) string {
	return randomKeys[index]
}

func (kd *BaseKeyDistributor) GetLength() uint32 {
	return UNIQUE_COUNT
}
