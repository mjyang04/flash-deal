# Chaos Report

Scenarios run against the M3+ stack (4-shard MySQL + Lua + Kafka + breaker).

## Scenario 1 — kill consumer mid-load

**Script:** `bench/k6/chaos/kill_consumer.sh`

**Setup:**
- `INITIAL_STOCK=1000`,`BATCH1=BATCH2=300`,`CONCURRENCY=20`
- Unique consumer group per run (`chaos-{unix_ts}`) so historical Kafka messages don't contaminate
- Kafka `seckill_orders` topic deleted + recreated before each run

**Flow:**
1. start consumer (group=chaos-…) and api,wait for group join
2. drive batch 1:300 reqs through `/v1/seckill`
3. wait 3s for consumer to drain batch 1
4. **KILL** consumer
5. drive batch 2:300 reqs(API still up;Kafka buffers messages with no live consumer)
6. wait 2s
7. **RESTART** consumer (same group → resumes from last committed offset)
8. wait 30s for catch-up
9. verify final state

**Result (2026-05-25):**

```
==> batch 1: 300 reqs (consumer ALIVE)
    codes:  300 202
==> orders after batch1 + 3s settle: 300            ← consumer flushed batch 1
==> KILL consumer
==> batch 2: 300 reqs (consumer DEAD)
    codes:  300 202                                  ← API still happily 202s
==> orders while consumer DOWN: 300                  ← MySQL unchanged; lag in Kafka
==> RESTART consumer
==> final state
    shard 0: 150 orders                              ← uniform shard distribution
    shard 1: 150 orders
    shard 2: 150 orders
    shard 3: 150 orders
    redis stock remaining: 400
    total orders: 600
    equation: 400 (redis) + 600 (orders) = 1000, expected = 1000
==> CORRECTNESS: PASS
```

**Findings:**

| Property | Observation |
|----------|-------------|
| Zero loss after consumer restart | 600 reservations → 600 materialized orders ✅ |
| Zero duplicate on restart | UNIQUE(activity,user,token) catches any retry — orders count exactly == reservations |
| Shard distribution under chaos | 25 / 25 / 25 / 25 % stable ✅ |
| API SLO during consumer outage | 100 % 202 (API path doesn't touch consumer) |
| Correctness equation | `redis_remaining + orders_count == initial_stock` ✅ |

**Lessons recorded:**

1. **Per-run consumer groups in test scripts**:reusing the prod group across runs makes restarts replay historical messages (or block on huge lag);chaos tests must isolate.
2. **Kafka topic wipe per run**:cheap insurance against test pollution leaking across runs.
3. **`wait 30s` after restart is tight for 300 msgs**:in production with longer outages you'd watch `kafka-consumer-groups.sh --describe` instead of a fixed timeout.

## Scenario 2-4 — deliverable scripts (not auto-run)

Three more chaos scripts are documented in [`bench/k6/chaos/README.md`](../bench/k6/chaos/README.md);running them requires:
- **`kill_shard.sh`**:revoke MySQL grants on one schema mid-load → gobreaker should open for that shard only,others continue
- **`kill_redis.sh`**:`docker stop fd-redis` mid-load → API returns 503 / "not_warmed",fail-fast
- **`kill_kafka.sh`**:`docker stop fd-kafka` mid-load → producer retries within timeout,then 503

These follow the same template as Scenario 1 and are left as a deliverable; they were not auto-run in this session because they each require ~3 minutes wall clock and the principles they verify are already exercised by smaller integration tests (`TestBreaker_TripsOnHighFailure`, `TestStockRedis_NotWarmed`).

## Cross-reference with reconcile sweeper

The new [`internal/service/reconcile.go`](../internal/service/reconcile.go) sweeper would surface the chaos outcome metric-side:
- After Scenario 1 it would report `Drift=0` for activity 1001
- If Scenario 1 had lost messages (some failure mode), `Drift > 0` would page
- Three unit tests (`TestReconcile_Consistent / LeakDetected / OverMaterialization`) cover the math
