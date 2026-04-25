# flash-deal — Data Model

## 1. MySQL — single-DB tables

### `activities`
| Column | Type | Notes |
|--------|------|-------|
| id | BIGINT UNSIGNED PK | externally assigned by ops |
| product_id | BIGINT UNSIGNED | |
| total_stock | INT | canonical stock (Redis is the working copy) |
| start_at | DATETIME | |
| end_at | DATETIME | |
| per_user_limit | INT | default 1 |
| status | TINYINT | 0=draft 1=scheduled 2=running 3=finished 4=canceled |
| created_at / updated_at | DATETIME | auto |
| KEY `idx_status_window` | (status, start_at, end_at) | scan candidate windows |

### `idempotency_keys` (optional table — Redis is primary; MySQL as durable audit if needed)
Skip in v1; rely on Redis 24h TTL.

## 2. MySQL — sharded `orders_{N}` (N = 0..3)

### Sharding rule
`shard_idx = user_id % 4`. Same function used in `pkg/shardkey/shardkey.go`.

### Schema (identical across shards)
| Column | Type | Notes |
|--------|------|-------|
| id | BIGINT UNSIGNED PK | snowflake (worker id from POD_NAME hash) |
| user_id | BIGINT UNSIGNED | shard key — present in every where clause |
| activity_id | BIGINT UNSIGNED | |
| product_id | BIGINT UNSIGNED | |
| status | TINYINT | 0=queued 1=paid 2=canceled 3=refunded |
| idempotency_token | VARCHAR(64) | client UUID v4 |
| created_at | DATETIME | default now |
| UNIQUE KEY `uk_idem` | (activity_id, user_id, idempotency_token) | idempotency safety net |
| KEY `idx_user_created` | (user_id, created_at) | "my orders" listing |
| KEY `idx_activity` | (activity_id) | activity-level admin queries |

### Cross-shard read patterns
- "My orders": single-shard query (`shardkey.DBIndex(user_id)`)
- "All orders for activity X": fan-out to N shards (admin-only, low QPS)
- Reporting: nightly export to ClickHouse / Hive (out of scope)

## 3. Redis keyspace

| Pattern | Type | TTL | Purpose |
|---------|------|-----|---------|
| `stock:{activity_id}` | string (int) | until activity end | working stock; modified by Lua |
| `user_buy:{activity_id}:{user_id}` | string (int) | 86400s | per-user purchased count |
| `idem:{activity_id}:{user_id}:{token}` | string (json) | 86400s | idempotency state + cached response |
| `queue:{queue_token}` | string | 600s | "queued" → "success:{order_id}" / "failed:{reason}" |
| `activity:{id}` | hash | 60s | cached activity meta (read-through) |
| `ratelimit:user:{user_id}` | string (int) | 60s | sliding window count |
| `ratelimit:ip:{ip}` | string (int) | 60s | per-IP count |
| `circuit:backend:{name}` | hash | 60s | circuit breaker state (used by gateway-style middleware if added) |

### Lua script: `stock_deduct.lua`
See `internal/infra/redis/scripts/stock_deduct.lua`. Returns:
- `>= 0` → success, value is new remaining
- `-1` → not enough stock
- `-2` → user limit exceeded
- `-3` → stock key missing (activity not warmed)

## 4. Kafka topics

| Topic | Partitions | Retention | Producer | Consumer |
|-------|------------|-----------|----------|----------|
| `seckill_orders` | 16 | 7d | api | consumer (group=seckill-consumer) |
| `seckill_orders_dlq` | 4 | 30d | consumer | manual / replay tool |
| `seckill_audit` | 8 | 30d | api | analytics (out of scope) |

### Message envelope (`seckill_orders`)
```json
{
  "version": 1,
  "order_id": 1234567890,
  "activity_id": 1001,
  "user_id": 88888,
  "product_id": 555,
  "idempotency_token": "8b1c8e92-...",
  "queue_token": "01J...",
  "produced_at": "2026-04-25T...Z",
  "trace_id": "..."
}
```

### Partition key
`user_id` — guarantees per-user ordering; consumer can run one worker per partition without losing order semantics.

## 5. Counters & gauges (for observability)

Exported as Prometheus metrics (label cardinality kept low):

| Metric | Type | Labels |
|--------|------|--------|
| `seckill_request_total` | counter | activity_id (top 10 by traffic only — others bucketed `other`), result |
| `seckill_stock_remain` | gauge | activity_id (top 10) |
| `seckill_order_inserted_total` | counter | shard |
| `seckill_dlq_total` | counter | reason |
| `seckill_idempotent_replay_total` | counter | — |
| `seckill_lua_duration_seconds` | histogram | — |
| `mysql_query_duration_seconds` | histogram | shard, op |
| `kafka_produce_duration_seconds` | histogram | topic |
| `redis_command_duration_seconds` | histogram | command |

## 6. Capacity planning (rough)

- Stock writes: pure Redis after warm, ~100k ops/s on a single Redis instance
- Order writes: 4 shards × ~5k inserts/s each = 20k/s sustained
- Kafka: 16 partitions, single broker can do >50k msg/s for our payload size
- API node: ~10-15k req/s per instance with Gin + httpx-style upstream calls

If we need to push past these: add Redis Cluster, more shards, more API replicas (HPA).
