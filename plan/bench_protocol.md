# flash-deal — Bench Protocol

The measurement contract. Same spirit as `llm-serve-bench/plan/bench_protocol.md` but tailored to seckill.

## 1. Topology

- **Bench day** uses 2 hosts:
  - Host A: k6 client only
  - Host B: api (3 replicas, behind Nginx) + middleware (mysql, redis, kafka)
- **Dev iteration** is fine on a single host but those numbers are for self-orientation, not for reports.

## 2. Workloads

| ID | Description | k6 scenario |
|----|------------|-------------|
| `cold_run_steady_30k` | constant 30k req/s for 60s | constant-arrival-rate |
| `cold_run_steady_50k` | constant 50k req/s for 60s | constant-arrival-rate |
| `burst_5w` | ramp 0 → 50k in 5s, hold 30s | ramping-arrival-rate |
| `sustained_1w_30min` | constant 10k req/s for 30 min | constant-arrival-rate |
| `oversold_pressure` | 10k VU competing for 100 stock | shared-iterations |

`oversold_pressure` is a **correctness** workload, not a perf workload — pass = exactly 100 successful, exactly 9900 sold-out.

## 3. Metrics required

| Metric | Source |
|--------|--------|
| QPS (success / total) | k6 |
| HTTP P50 / P95 / P99 / max | k6 |
| Lua latency (server side) | Prometheus histogram |
| MySQL insert latency per shard | Prometheus histogram |
| Kafka producer publish latency | Prometheus histogram |
| Consumer lag | Kafka exporter |
| GC pause + heap (api / consumer) | Go runtime metrics |
| Redis CPU + commands/sec | redis_exporter |
| MySQL Threads_running per shard | mysqld_exporter |

## 4. Statistical rules

- 3 runs per cell, **median** for headline; show range
- 60s warmup before measurement window
- 30s gap between runs
- Restart api + consumer between runs (clean GC, fresh KV-cache state etc.)
- Re-warm Redis stock between runs

## 5. Correctness checks (must pass before reporting)

- `select count(*) from orders_X where activity_id = ?` summed across shards == initial_stock - final_redis_stock
- No row in `orders` violates `(activity_id, user_id, idempotency_token)` unique constraint
- DLQ count after replay == 0
- All requests with `X-Request-Id` produce a trace in Jaeger (sample 100)

## 6. Failure injection (chaos)

| Scenario | Expected behavior |
|----------|-------------------|
| Kill consumer mid-run | Kafka lag grows; on restart consumer catches up; final correctness check passes |
| Kill 1 of 4 MySQL shards | requests routed to that shard return 503 (circuit breaker open); others succeed |
| Kill Redis | api returns 503 (no working stock); fail-fast |
| Kill Kafka broker | producer enters retry; api returns 503 after retry budget |
| Network blip (tc netem 100ms) | P99 spikes but no oversell |

Each failure scenario produces a section in `reports/chaos.md`.

## 7. Reporting template

```
# Bench summary

## Setup
- Bench: k6 vX.Y on host A (Y core, Z RAM)
- API: 3 replicas on host B, GOMAXPROCS=N, GOMEMLIMIT=Mi
- MySQL 8.0 (4 shards), Redis 7.x, Kafka 3.8 (3 brokers)
- Bench commit SHA / api commit SHA

## Workload: cold_run_steady_30k
- Successful QPS: ____
- HTTP P99: ____ ms
- Lua P99: ____ ms
- MySQL insert P99 per shard: ____
- Final correctness: PASS
- Notes: ...

## Charts
- qps_vs_time.png
- p99_breakdown.png
- consumer_lag.png

## Limitations
- ...
```

## 8. Out of scope (don't measure, document instead)

- TCP connection setup time (assume keep-alive)
- DNS resolution (use IP)
- TLS handshake (run benches over HTTP)
- Multi-region behavior
- Cold start of containers
