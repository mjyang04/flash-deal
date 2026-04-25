# flash-deal — Task List

Day-level. Each item ≤ 1 PR.

## Week 1 — MVP single-node

### Day 1 — Go env + scaffolding sanity
- [ ] `brew install go` (Go 1.22+)
- [ ] `go mod tidy` resolves
- [ ] `go test ./...` passes (shardkey test)
- [ ] Commit: `chore: verify go env`

### Day 2 — docker-compose
- [ ] `make up` starts mysql + redis + kafka + jaeger + prom + grafana
- [ ] `mysql -h127.0.0.1 -P3307 -uflashdeal -pflashdeal -e 'show databases'`
- [ ] `redis-cli -p 6380 ping`
- [ ] `make migrate` applies schema
- [ ] Commit: `chore(deploy): docker-compose verified`

### Day 3 — domain + repo skeletons
- [ ] `internal/repo/activity_repo.go` (interface + sqlx impl)
- [ ] `internal/repo/order_repo.go` (interface + per-shard sqlx impl)
- [ ] `internal/repo/stock_repo.go` (Redis wrapper, Lua not yet)
- [ ] Unit tests with `testcontainers-go`
- [ ] Commit: `feat(repo): activity/order/stock repositories`

### Day 4 — service v0 (synchronous, no Lua, no Kafka)
- [ ] `internal/service/seckill.go` Seckill(ctx, req) → result
- [ ] Synchronous: SELECT activity → check window → MySQL UPDATE … WHERE remaining > 0 → INSERT order
- [ ] Commit: `feat(service): synchronous seckill v0`

### Day 5 — handler + middleware basics
- [ ] `internal/handler/seckill.go`
- [ ] `internal/middleware/recovery.go`, `tracing.go` (OTel), `requestid.go`
- [ ] Wire into `cmd/api/main.go`
- [ ] Manual curl test
- [ ] Commit: `feat(handler): wire seckill route + base middleware`

### Day 6 — first k6 baseline
- [ ] Adapt `bench/k6/seckill.js` to v0 contract
- [ ] Seed an activity with 1000 stock
- [ ] Run k6 small (100 VU × 30s)
- [ ] Document baseline numbers in `reports/week1_mvp.md`
- [ ] Commit: `chore(report): v0 baseline`

### Day 7 — buffer
- [ ] Fix issues from baseline (often: connection pool too small)
- [ ] **Tag**: `git tag m1`

## Week 2 — Redis Lua + Kafka async

### Day 8 — Lua stock script
- [ ] Implement `internal/infra/redis/scripts.go`: load + cache `stock_deduct.lua` SHA, EVALSHA path
- [ ] `internal/repo/stock_repo.go::Deduct(ctx, aid, uid, n, limit)` returns enum + remaining
- [ ] **Concurrency test**: 1000 goroutines × 1 stock each, total stock 100 → exactly 100 succeed, exactly 0 oversell
- [ ] Commit: `feat(stock): atomic Lua deduct with concurrency test`

### Day 9 — warm script
- [ ] `scripts/warm.go`: read activity from MySQL → SET Redis stock + per_user_limit + activity hash
- [ ] Invoked via `make warm ACTIVITY=1001`
- [ ] Commit: `feat(scripts): activity warmup`

### Day 10 — Kafka producer
- [ ] `internal/infra/kafka/producer.go` (segmentio)
- [ ] `internal/service/seckill.go`: replace synchronous order insert with kafka produce
- [ ] Idempotency middleware to handle duplicate token
- [ ] Commit: `feat(service): async order via Kafka`

### Day 11 — consumer
- [ ] `cmd/consumer/main.go` → `internal/service/order_materializer.go`
- [ ] Insert into sharded MySQL via `pkg/shardkey`
- [ ] Idempotent insert (UNIQUE KEY → ignore on duplicate)
- [ ] DLQ on persistent failure
- [ ] Commit: `feat(consumer): order materialization`

### Day 12 — queue token state
- [ ] Update `queue:{token}` after consumer success/failure
- [ ] `GET /v1/order/by-token/:token` reads it
- [ ] Commit: `feat(api): queue token polling`

### Day 13 — bench v1
- [ ] Re-run k6, compare to v0
- [ ] `reports/week2_redis_kafka.md`
- [ ] Commit: `chore(report): week 2 async results`

### Day 14 — buffer + **Tag**: `git tag m2`

## Week 3 — Sharding + limits + observability

### Day 15 — sharding
- [ ] Apply `001_init.up.sql` to 4 logical DBs (or 4 schemas in single MySQL for dev)
- [ ] Repo: `OrderRepo` becomes `[]*OrderRepoShard`
- [ ] All read/write paths use `shardkey.DBIndex`
- [ ] Test: insert + lookup round-trip across shards
- [ ] Commit: `feat(repo): orders sharded × 4`

### Day 16 — rate limit
- [ ] `internal/middleware/ratelimit.go`: per-user via Redis token bucket; global via local
- [ ] Tests: 429 on overflow
- [ ] Commit: `feat(middleware): rate limit`

### Day 17 — circuit breaker + idempotency middleware
- [ ] gobreaker around MySQL writes (consumer side)
- [ ] `internal/middleware/idempotency.go`: Redis SETNX + cache result on completion
- [ ] Tests: replay returns cached body
- [ ] Commit: `feat(middleware): idempotency + circuit breaker`

### Day 18 — OTel + Prom complete
- [ ] OTel: trace propagation across api → kafka → consumer (header carrier)
- [ ] Prometheus: all metrics in `data_model.md` registered
- [ ] Commit: `feat(observability): full trace + metrics`

### Day 19 — Grafana dashboard
- [ ] JSON dashboard in `deploy/grafana/dashboards/seckill.json`
- [ ] Panels: RPS, success rate, P50/P99, stock burndown, DLQ, Redis lua latency
- [ ] Commit: `chore(monitoring): grafana dashboard`

### Day 20 — load + chaos
- [ ] k6 ramping-arrival-rate 0→30k QPS
- [ ] Kill consumer mid-run; verify DLQ + recovery on restart
- [ ] Kill one MySQL shard; verify circuit breaker
- [ ] Commit: `chore(run): chaos test results`

### Day 21 — week 3 report + **Tag**: `git tag m3`

## Week 4 — Optimization + writeup

### Day 22 — profiling
- [ ] `pprof` CPU + alloc + mutex during sustained load
- [ ] Identify top 3 hotspots
- [ ] Commit: `chore(profile): pprof captures`

### Day 23 — quick wins
- [ ] sync.Pool for hot objects (request DTO, etc.)
- [ ] Tune `MaxOpenConns` / `MaxIdleConns` per shard
- [ ] Tune Redis pool size, kafka batch
- [ ] Commit: `perf: connection pool + sync.Pool`

### Day 24 — re-run bench
- [ ] Compare pre/post optimization
- [ ] Goal: ≥30k QPS single node, P99 ≤ 50ms
- [ ] Commit: `chore(report): post-optimization numbers`

### Day 25 — correctness final
- [ ] Run "stock correctness suite": for each activity, assert orders.count(activity) == initial_stock - final_stock
- [ ] Replay DLQ; verify final state consistent
- [ ] Commit: `chore(test): correctness final`

### Day 26-27 — blog
- [ ] Draft "从零到 10W QPS:Go 秒杀系统全链路拆解"
- [ ] `writing-anti-ai` polish pass
- [ ] Publish: blog → 知乎 → 掘金 → InfoQ
- [ ] Commit: `docs(blog): seckill 10w qps`

### Day 28 — release
- [ ] Mermaid architecture in README
- [ ] Demo video (5 min): make up → make api → k6 → grafana
- [ ] `resume_blurbs.md` with real numbers
- [ ] **Tag**: `git tag m4-release`
