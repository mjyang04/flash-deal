# Chaos scenarios (M3)

Each script:
1. assumes `make up && make migrate-all && make kafka-topic && make seed` already ran
2. starts api + consumer
3. drives load + kills a dependency
4. verifies recovery + correctness

Run from repo root.

| Script | Kill | Expected |
|--------|------|----------|
| `kill_consumer.sh` | consumer mid-load → restart 10s later | Kafka lag grows; on restart consumer catches up; orders ≤ stock; no duplicates |
| `kill_shard.sh` | one MySQL shard | requests routed to that shard fail; breaker opens; other shards continue |
| `kill_redis.sh` | redis | API returns 503 / not_warmed; fail-fast |
| `kill_kafka.sh` | kafka | producer errors → 503 after retry budget; consumer waits for re-up |
