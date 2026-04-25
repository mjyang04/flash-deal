# flash-deal — Risk Register

| ID | Risk | Likelihood | Impact | Mitigation | Detection |
|----|------|------------|--------|------------|-----------|
| R1 | Local machine cannot sustain 30k QPS bench (CPU bound on client) | H | H | Multi-host bench day; or rent 2 cloud VMs ($5 total for an hour) | client CPU > 80% during run |
| R2 | MySQL becomes the bottleneck before reaching target QPS | M | M | Sharding (4) + tune `innodb_buffer_pool_size`; if still slow, consumer becomes the throttle (Kafka absorbs spike) | mysql `Threads_running` > 50 |
| R3 | Redis hot key on `stock:{aid}` saturates one Redis core | M | M | Single Redis can do 100k+ Lua/s on one core; if needed, split big activities into virtual sub-stocks | redis-cli LATENCY DOCTOR |
| R4 | Kafka under-partitioned → consumer lag | M | M | 16 partitions baseline; alert on lag > 1000 | consumer-group lag metric |
| R5 | Lua bug causes oversell | L | CRIT | Concurrency test: 1000 goroutines × 1 stock per activity = exact match (no oversell, no undersell) | concurrency test fails |
| R6 | Snowflake worker_id collision | L | H | Derive from POD_NAME hash + assert uniqueness on startup | startup probe |
| R7 | DLQ grows silently | M | M | Prometheus alert: `seckill_dlq_total` rate > 1/min | alert page |
| R8 | Idempotency cache miss (Redis eviction) leaks duplicate order to MySQL | L | M | UNIQUE KEY on orders table catches it | duplicate insert in logs |
| R9 | OTel context drops across kafka boundary | M | L | Inject trace headers into kafka message; verify in Jaeger | trace shows orphan span |
| R10 | k6 ramping-arrival-rate blows VU pool | L | L | preAllocatedVUs sized; maxVUs cap | k6 logs warning |
| R11 | Time slip — week 4 cut short | H | M | Week 4 buffer is profiling/blog only; if behind drop blog distribution but ship code | not at M3 by end of W3 |
| R12 | Connection pool too small starves goroutines | M | M | `MaxOpenConns` configurable; default 100/shard; alert on `wait_count` | sql_db_wait_count metric |
| R13 | Consumer crash during write loses Kafka offset → duplicates | M | L | Manual offset commit after MySQL insert OR DLQ; orders table UNIQUE KEY catches dup | spike in `idempotent_replay_total` |

## Cutoffs

- **End W1**: if no working v0 (synchronous) flow → simplify scope (drop k6 ramp-up, do constant-rate only)
- **End W2**: if Lua concurrency test fails → halt, fix before any further feature
- **End W3**: if observability not green → ship without chaos test; don't fake numbers
- **End W4**: if 30k single-node QPS not hit → ship with whatever number we hit + honest analysis

## Always-on guardrails

- `make test` runs concurrency correctness suite
- `golangci-lint run` clean before each tag
- Every report has env recording (machine specs, configs, git SHA)
- Hot key benchmarks must rotate activity_id to avoid cross-test cache interference
