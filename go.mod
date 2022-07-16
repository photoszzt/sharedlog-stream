module sharedlog-stream

go 1.17

require (
	cs.utexas.edu/zjia/faas v0.0.0
	github.com/Jeffail/gabs/v2 v2.6.1
	github.com/confluentinc/confluent-kafka-go v1.8.2
	github.com/gammazero/deque v0.1.1
	github.com/go-redis/redis/v8 v8.11.4
	github.com/google/btree v1.0.1
	github.com/huandu/skiplist v1.2.0
	github.com/rs/zerolog v1.26.1
	github.com/spaolacci/murmur3 v1.1.0
	github.com/stretchr/testify v1.7.1
	github.com/tinylib/msgp v1.1.6
	go.mongodb.org/mongo-driver v1.8.4
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/tools v0.1.10
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1
)

require (
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/go-stack/stack v1.8.0 // indirect
	github.com/golang/snappy v0.0.1 // indirect
	github.com/klauspost/compress v1.13.6 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/niemeyer/pretty v0.0.0-20200227124842-a10e7caefd8e // indirect
	github.com/petermattis/goid v0.0.0-20180202154549-b0b1615b78e5 // indirect
	github.com/philhofer/fwd v1.1.1 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/sasha-s/go-deadlock v0.3.1 // indirect
	github.com/xdg-go/pbkdf2 v1.0.0 // indirect
	github.com/xdg-go/scram v1.0.2 // indirect
	github.com/xdg-go/stringprep v1.0.2 // indirect
	github.com/youmark/pkcs8 v0.0.0-20181117223130-1be2e3e5546d // indirect
	golang.org/x/crypto v0.0.0-20211215165025-cf75a172585e // indirect
	golang.org/x/mod v0.6.0-dev.0.20220106191415-9b9b3d81d5e3 // indirect
	golang.org/x/sys v0.0.0-20211019181941-9d821ace8654 // indirect
	golang.org/x/text v0.3.7 // indirect
	gopkg.in/check.v1 v1.0.0-20200227125254-8fa46927fb4f // indirect
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b // indirect
)

replace cs.utexas.edu/zjia/faas => ../boki/worker/golang

replace cs.utexas.edu/zjia/faas/slib => ../boki/faas/slib
