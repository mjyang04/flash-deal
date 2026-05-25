.PHONY: tidy up down migrate seed api consumer test bench lint format clean

tidy:
	go mod tidy

up:
	cd deploy && docker compose up -d

down:
	cd deploy && docker compose down

migrate:
	docker exec -i fd-mysql mysql -uflashdeal -pflashdeal flashdeal < migrations/001_init.up.sql

migrate-down:
	docker exec -i fd-mysql mysql -uflashdeal -pflashdeal flashdeal < migrations/001_init.down.sql

kafka-topic:
	docker exec fd-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
	  --create --if-not-exists --topic seckill_orders --partitions 16 --replication-factor 1
	docker exec fd-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
	  --create --if-not-exists --topic seckill_orders_dlq --partitions 4 --replication-factor 1

migrate-shards:
	docker exec -i fd-mysql mysql -uroot -prootpw < migrations/002_orders_shards.up.sql
	@for n in 0 1 2 3; do \
	  echo "applying orders DDL to flashdeal_$$n"; \
	  docker exec -i fd-mysql mysql -uflashdeal -pflashdeal flashdeal_$$n < migrations/002_orders_shards_table.sql; \
	done

migrate-all: migrate migrate-shards

PPROF_DURATION ?= 30
pprof-cpu:
	curl -sS "http://localhost:6060/debug/pprof/profile?seconds=$(PPROF_DURATION)" -o /tmp/cpu.pprof
	go tool pprof -text /tmp/cpu.pprof | head -30

pprof-heap:
	curl -sS http://localhost:6060/debug/pprof/heap -o /tmp/heap.pprof
	go tool pprof -text /tmp/heap.pprof | head -30

pprof-mutex:
	curl -sS http://localhost:6060/debug/pprof/mutex -o /tmp/mutex.pprof
	go tool pprof -text /tmp/mutex.pprof | head -30

seed:
	go run ./cmd/seed

api:
	go run ./cmd/api

consumer:
	go run ./cmd/consumer

test:
	go test ./... -race -cover

bench:
	k6 run bench/k6/seckill.js

lint:
	golangci-lint run

format:
	gofmt -s -w .

clean:
	rm -rf bin/ dist/ coverage.txt
