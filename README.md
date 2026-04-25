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

## Quickstart
```bash
make up              # docker-compose: mysql + redis + kafka + jaeger + prom + grafana
make migrate         # apply schema
make seed            # create demo activity + warm Redis stock
make api             # start API server
make consumer        # start Kafka consumer (separate terminal)
make bench           # k6 load test
```

## Repo layout
See [CLAUDE.md](./CLAUDE.md) and [plan/](./plan/) for full design.

## Status
- [x] Scaffold (this commit)
- [ ] M1 MVP single-node end-to-end
- [ ] M2 Redis Lua + Kafka async
- [ ] M3 Sharding + rate-limit + circuit-breaker + observability
- [ ] M4 Load test + optimization + blog

## Note for first run
Install Go 1.22+ (`brew install go`) before running anything.

## License
MIT
