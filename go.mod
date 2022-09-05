module sharedlog-stream

go 1.18

require (
	cs.utexas.edu/zjia/faas v0.0.0
	github.com/Jeffail/gabs/v2 v2.6.1
	github.com/cespare/xxhash/v2 v2.1.2
	github.com/confluentinc/confluent-kafka-go v1.8.2
	github.com/gammazero/deque v0.1.1
	github.com/google/btree v1.1.2
	github.com/rs/zerolog v1.26.1
	github.com/spaolacci/murmur3 v1.1.0
	github.com/stretchr/testify v1.8.0
	github.com/tinylib/msgp v1.1.6
	github.com/zhangyunhao116/skipmap v0.0.0
	github.com/zhangyunhao116/skipset v0.12.1
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/tools v0.1.10
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1
)

require (
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/go-redis/redis/v9 v9.0.0-beta.2 // indirect
	github.com/zhangyunhao116/fastrand v0.2.1 // indirect
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/niemeyer/pretty v0.0.0-20200227124842-a10e7caefd8e // indirect
	github.com/petermattis/goid v0.0.0-20180202154549-b0b1615b78e5 // indirect
	github.com/philhofer/fwd v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/sasha-s/go-deadlock v0.3.1
	golang.org/x/exp v0.0.0-20220722155223-a9213eeb770e
	golang.org/x/mod v0.6.0-dev.0.20220106191415-9b9b3d81d5e3 // indirect
	golang.org/x/sys v0.0.0-20220422013727-9388b58f7150 // indirect
	gopkg.in/check.v1 v1.0.0-20200227125254-8fa46927fb4f // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace cs.utexas.edu/zjia/faas => ../boki/worker/golang

replace cs.utexas.edu/zjia/faas/slib => ../boki/faas/slib

replace github.com/zhangyunhao116/skipmap v0.0.0 => github.com/photoszzt/skipmap v0.0.0-20220824160709-b60acf7319b0
