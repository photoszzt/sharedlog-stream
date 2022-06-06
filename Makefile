GO_FILES?=$$(find . -name '*.go' |grep -v deps)

default: lat_tp_handler_debug lat_tp_handler sharedlog_bench_client \
	kafka_consume_bench kafka_produce_bench nexmark nexmark_stats nexmark_debug \
	nexmark_gen_data_by_spec nexmark_client nexmark_genevents_kafka tests_debug tests_client dspbench \
	dspbench_debug dspbench_client wordcount_client wordcount_genevents_kafka \
	append_read_client

.PHONY: golangci-lint
golangci-lint:
	@golangci-lint -E goimports run ./...

.PHONY: nexmark_gen_data_by_spec
nexmark_gen_data_by_spec:
	mkdir -p ./bin
	GO111MODULE=on go build -o bin/nexmark_gen_data_by_spec ./benchmark/nexmark/gen_test_data

.PHONY: nexmark
nexmark:
	mkdir -p ./bin
	GO111MODULE=on go build -o bin/nexmark_handler ./benchmark/nexmark/nexmark_handler

.PHONY: nexmark_stats
nexmark_stats:
	mkdir -p ./bin
	GO111MODULE=on go build -tags stats -o bin/nexmark_handler_stats ./benchmark/nexmark/nexmark_handler

.PHONY: nexmark_debug
nexmark_debug:
	mkdir -p ./bin
	GO111MODULE=on go build -tags "debug,stats" -o bin/nexmark_handler_debug ./benchmark/nexmark/nexmark_handler

.PHONY: nexmark_client
nexmark_client:
	mkdir -p ./bin
	GO111MODULE=on go build -o bin/nexmark_client ./benchmark/nexmark/nexmark_client

.PHONY: nexmark_genevents_kafka
nexmark_genevents_kafka:
	mkdir -p ./bin
	GO111MODULE=on go build -o bin/nexmark_genevents_kafka ./benchmark/nexmark/nexmark_genevents_kafka

.PHONY: nexmark_kafka_test_src
nexmark_kafka_test_src:
	mkdir -p ./bin
	GO111MODULE=on go build -o bin/nexmark_kafka_test_src ./benchmark/nexmark/nexmark_kafka_test_src

.PHONY: tests_debug
tests_debug:
	mkdir -p ./bin
	GO111MODULE=on go build -tags debug -o bin/tests_handler_debug ./benchmark/tests/tests_handler

.PHONY: tests_client
tests_client:
	mkdir -p ./bin
	GO111MODULE=on go build -o bin/tests_client ./benchmark/tests/tests_client

.PHONY: wordcount_genevents_kafka
wordcount_genevents_kafka:
	mkdir -p ./bin
	GO111MODULE=on go build -o bin/wordcount_genevents_kafka ./benchmark/dspbench/wordcount_genevents_kafka

.PHONY: dspbench
dspbench:
	mkdir -p ./bin
	GO111MODULE=on go build -o bin/dspbench_handler ./benchmark/dspbench/dspbench_handler

.PHONY: dspbench_debug
dspbench_debug:
	mkdir -p ./bin
	GO111MODULE=on go build -tags debug -o bin/dspbench_handler_debug ./benchmark/dspbench/dspbench_handler

.PHONY: dspbench_client
dspbench_client:
	mkdir -p ./bin
	GO111MODULE=on go build -o bin/dspbench_client ./benchmark/dspbench/dspbench_client

.PHONY: wordcount_client
wordcount_client:
	mkdir -p ./bin
	GO111MODULE=on go build -o bin/wordcount_client ./benchmark/dspbench/wordcount_client

.PHONY: append_read_client
append_read_client:
	mkdir -p ./bin
	GO111MODULE=on go build -o bin/append_read_client ./benchmark/dspbench/append_read_client

.PHONY: kafka_produce_bench
kafka_produce_bench:
	mkdir -p ./bin
	GO111MODULE=on go build -o bin/kafka_produce_bench ./benchmark/lat_tp/kafka_produce

.PHONY: kafka_consume_bench
kafka_consume_bench:
	mkdir -p ./bin
	GO111MODULE=on go build -o bin/kafka_consume_bench ./benchmark/lat_tp/kafka_consume

.PHONY: lat_tp_handler
lat_tp_handler:
	mkdir -p ./bin
	GO111MODULE=on go build -o bin/lat_tp_handler ./benchmark/lat_tp/lat_tp_handler

.PHONY: lat_tp_handler_debug
lat_tp_handler_debug:
	mkdir -p ./bin
	GO111MODULE=on go build -tags debug -o bin/lat_tp_handler_debug ./benchmark/lat_tp/lat_tp_handler

.PHONY: sharedlog_bench_client
sharedlog_bench_client:
	mkdir -p ./bin
	GO111MODULE=on go build -o bin/sharedlog_bench_client ./benchmark/lat_tp/sharedlog_bench_client

.PHONY: download-book
download-book:
	mkdir -p data
	wget -O data/books.dat https://raw.githubusercontent.com/mayconbordin/storm-applications/master/data/books.dat
