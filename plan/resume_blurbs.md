# flash-deal — Resume & Interview Blurbs

Numbers below are from `reports/final.md` (M4, single-host bench).

## Resume bullet (English)

> **flash-deal** — Production-grade Go flash-sale system. Atomic Redis Lua stock reservation + Kafka async order materialization + 4-shard MySQL + token-bucket rate-limit + sliding-window circuit breaker + OTel/Prometheus.
> Drove **P95 from 2,380 ms (sync SQL row-lock) down to 11.6 ms (Lua + async Kafka) at 1,000 rps single-node — ~205× improvement** with the same correctness guarantee; concurrency tests prove **zero overselling** under 1,000-goroutine contention against 100-stock window; chaos scripts cover consumer / shard / Redis / Kafka kill scenarios; full distributed trace via OTel propagated across Kafka boundary.

## Resume bullet (中文)

> **flash-deal** — 生产级 Go 秒杀系统。Redis Lua 原子扣库存 + Kafka 异步落单 + MySQL 分库分表(×4)+ 令牌桶限流 + 幂等中间件(SETNX+缓存响应)+ gobreaker 熔断 + OTel/Prometheus/Grafana 全链路。
> 同 1,000 rps 单机条件下,**P95 从 2,380 ms(同步 SQL 行锁)优化到 11.6 ms(Lua + Kafka 异步)— 提升约 205×**,正确性不损;1,000 协程抢 100 库存并发测试 **success=100、soldOut=900、零超卖**;基于 pprof 数据决定**不做未验证的优化**(`sync.Pool` / pool 扩张 / GC 调优在该场景下都被 profile 否决);全链路 trace 跨 Kafka 边界透传。

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

**R**: P95 ~12 ms sustained @ 1.5k rps single-host (M3 stack: 4-shard + middleware + observability); ~205× P95 improvement vs M1 same workload; zero oversell across 1k-goroutine stress; Grafana 6-panel dashboard + Jaeger trace across api → kafka → consumer. Single-host client/server share = bench cap; multi-host 30k QPS path documented in [`docs/superpowers/plans/2026-05-25-flash-deal-m4-optimize-release.md`](../docs/superpowers/plans/2026-05-25-flash-deal-m4-optimize-release.md).

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
