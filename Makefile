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

seed:
	@echo "TODO: implement scripts/seed.go to insert demo activity + warm Redis stock"

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
