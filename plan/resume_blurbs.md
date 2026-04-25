# flash-deal — Resume & Interview Blurbs

Fill in real numbers from `reports/final.md` after Week 4.

## Resume bullet (English)

> **flash-deal** — Production-grade Go flash-sale system. Atomic Redis Lua stock reservation + Kafka async order materialization + 4-shard MySQL + token-bucket rate-limit + sliding-window circuit breaker + OTel/Prometheus.
> Sustained **__,000 QPS single-node** at P99 < **__ ms**; concurrency tests prove **zero overselling and zero underselling** under 10k goroutine contention; chaos tests verify graceful recovery from MySQL / Kafka / consumer outages.

## Resume bullet (中文)

> **flash-deal** — 生产级 Go 秒杀系统。Redis Lua 原子扣库存 + Kafka 异步落单 + MySQL 分库分表(×4)+ 令牌桶限流 + 滑动窗口熔断 + OTel/Prometheus 全链路。
> 单机 **__万 QPS**,P99 < **__ ms**;并发正确性测试验证 **零超卖、零少卖**;Chaos 测试验证 MySQL / Kafka / Consumer 故障下的优雅降级与恢复。

## STAR-format interview answer

**S**: Wanted a project that hits the canonical CN big-tech backend interview surface area: high concurrency + correctness + observability + sharding + MQ.

**T**: Build a complete seckill system in 4 weeks, hit 30k+ single-node QPS, prove correctness with a stress test, document everything for reproducibility.

**A**:
- **Reservation in Redis (Lua)**: single atomic script handles stock check, per-user limit, and decrement; returns enum
- **Async materialization via Kafka**: API returns 202 + queue token; consumer writes orders idempotently to sharded MySQL
- **Sharding by user_id % 4**: keeps "my orders" reads on a single shard
- **Idempotency**: Redis SETNX for hot path + UNIQUE KEY on orders as safety net
- **Middleware stack**: token-bucket per-user limit, gobreaker circuit breaker, OTel propagation across kafka boundary
- **Bench protocol** documented (`plan/bench_protocol.md`) so numbers are reproducible

**R**: ___k QPS sustained, P99 ___ ms, zero oversell across 10k-goroutine stress, full Grafana dashboard, blog at ___ reads.

## Likely deep-dive interview questions

1. **"Walk me through what happens when 10k users hit /seckill at the same moment."** — full path from middleware to Lua to Kafka, including idempotency
2. **"Why Redis Lua and not MySQL row lock?"** — single hot row in MySQL serializes; Lua is in-memory atomic
3. **"What if Redis crashes mid-deduction?"** — Lua is single-threaded, atomic per command; AOF for durability; on restart, reconcile from MySQL canonical stock
4. **"How do you guarantee no oversell?"** — Lua check-and-decrement is atomic; UNIQUE KEY on orders blocks duplicate persist; concurrency test in CI
5. **"What's the failure mode if Kafka is unavailable?"** — fail fast: API returns 503; do not deduct without persistence guarantee
6. **"Why partition_key=user_id?"** — preserves per-user message order; one consumer worker per partition can run lock-free
7. **"How do you scale to 1M QPS?"** — Redis Cluster (split hot key into virtual sub-stocks if needed), more API replicas + HPA, more MySQL shards, more Kafka partitions; explain CAP trade-offs
8. **"How do you reconcile Redis stock vs MySQL orders?"** — periodic sweeper that compares per-activity (out of scope v1, but explain the design)
9. **"What does your circuit breaker actually do?"** — sliding window error rate; when open, return 503 fast; half-open trial; closed on success — pull `decisions.md` ADR-008 + show code
10. **"How do you trace a single request end-to-end?"** — OTel context injected into Kafka headers; Jaeger shows api span → kafka publish → consumer span
11. **"What's the blast radius if one MySQL shard goes down?"** — orders for users where `user_id % 4 == down_shard` fail; others succeed; explain how to detect and route around
12. **"What's one number that surprised you in your bench?"** — pick a real finding from `reports/`

## Anti-patterns to avoid in interviews

- ❌ Quoting QPS without specifying P99 — always pair them
- ❌ "It's atomic because Redis is single-threaded" — be precise: Lua scripts are atomic per script invocation; pipelining is not
- ❌ "We use Kafka for everything" — Kafka is for the order persistence path; not for stock decrement (that's synchronous)
- ❌ Hand-waving consistency — the Redis ↔ MySQL eventual-consistency story has to be explicit, with concrete recovery steps

## What to point to during the interview

| If they ask about | Show them |
|-------------------|-----------|
| design / architecture | `plan/architecture.md` + Mermaid in README |
| API contract | `plan/api_spec.md` |
| concurrency safety | `internal/infra/redis/scripts/stock_deduct.lua` + concurrency test |
| sharding logic | `pkg/shardkey/shardkey.go` + tests |
| how you tested | `plan/bench_protocol.md` + `reports/` |
| reasoning behind choices | `plan/decisions.md` |
| what you would do differently | `plan/risks.md` + ADR consequences |
