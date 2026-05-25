# flash-deal

Production-grade Go flash-sale (秒杀) system showcasing the canonical high-concurrency backend pattern: **Redis Lua atomic stock + Kafka async order + sharded MySQL + rate-limit + circuit-breaker + full observability**.

## Why
Flash-sale is the highest-frequency interview topic for Chinese internet backend roles. This project is engineered to be:
- **Provably correct** — concurrency tests prove zero overselling
- **Provably fast** — k6 load test reports with Grafana screenshots
- **Production-shaped** — auth + idempotency + rate-limit + circuit-breaker + OTel

## Stack
- **Language**: Go 1.22+
- **Web**: Gin
- **Cache**: Redis 7 (Lua scripts for atomicity)
- **MQ**: Kafka (sarama / segmentio)
- **DB**: MySQL 8 (4-shard horizontal split for `orders`)
- **Limit/Break**: `golang.org/x/time/rate` + `sony/gobreaker`
- **Observability**: OTel + Jaeger + Prometheus + Grafana
- **Bench**: k6

## Targets
| Metric | Target |
|---|---|
| Single-node QPS | ≥ 30,000 |
| Cluster QPS | ≥ 100,000 |
| P99 latency | ≤ 50 ms |
| Stock correctness | 100% (no oversell, no undersell) |

## Quickstart (M1)
```bash
make up              # docker-compose: mysql + redis (M1 only; kafka/jaeger/prom/grafana wake in M2/M3)
make migrate         # apply schema: activities + orders_0
make seed            # create demo activity id=1001 stock=1000 + warm Redis
make api             # start API on :8080
RATE=1000 DURATION=30s make bench   # k6 baseline
```

Smoke:
```bash
curl -X POST -H 'Content-Type: application/json' \
  -d '{"activity_id":1001,"user_id":1,"idempotency_token":"tok-A"}' \
  http://localhost:8080/v1/seckill
# → 202 {"queue_token":"...","remaining":999,"status":"queued"}
```

## Testing
```bash
go test -race -cover ./...                          # unit
go test -tags=integration -race ./internal/...      # integration (needs make up + make migrate)
```

`internal/repo.TestStockRepo_Deduct_NoOversell` — 1000 goroutines vs 100 stock, zero oversell.

## Repo layout
See [CLAUDE.md](./CLAUDE.md) and [plan/](./plan/) for full design.

Task-by-task implementation plans (subagent-driven, TDD):
- [M1 — MVP single-node sync](./docs/superpowers/plans/2026-05-25-flash-deal-m1-mvp.md) ✅ tag `m1`
- [M2 — Redis Lua + Kafka async](./docs/superpowers/plans/2026-05-25-flash-deal-m2-async.md)
- [M3 — Sharding + ratelimit/breaker/idempotency + OTel/Prom/Grafana + chaos](./docs/superpowers/plans/2026-05-25-flash-deal-m3-shard-observability.md)
- [M4 — pprof optimize + final report + release](./docs/superpowers/plans/2026-05-25-flash-deal-m4-optimize-release.md)

## Status
- [x] Scaffold
- [x] **M1 MVP single-node end-to-end** (tag `m1`, baseline: [`reports/week1_mvp.md`](./reports/week1_mvp.md))
- [ ] M2 Redis Lua + Kafka async
- [ ] M3 Sharding + rate-limit + circuit-breaker + observability
- [ ] M4 Load test + optimization + blog

## Prerequisites
- Docker Desktop
- Go 1.22+ (`brew install go`)
- k6 (`brew install k6`) — optional, only for `make bench`

## License
MIT
