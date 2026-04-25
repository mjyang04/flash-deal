# flash-deal — Architecture

## 1. System diagram

```
                      ┌─────────────┐
                      │  k6 / wrk   │  (load gen)
                      └──────┬──────┘
                             │ HTTP
                      ┌──────▼──────┐
                      │  Nginx LB   │  (optional in dev)
                      └──────┬──────┘
                             │
        ┌────────────────────┼────────────────────┐
        ▼                    ▼                    ▼
   ┌─────────┐         ┌─────────┐          ┌─────────┐
   │  api-1  │         │  api-2  │          │  api-3  │   Gin + middleware
   │  :8080  │         │  :8080  │          │  :8080  │
   └────┬────┘         └────┬────┘          └────┬────┘
        │   middleware:      │                    │
        │   auth, ratelimit, │                    │
        │   idempotency,     │                    │
        │   trace, metrics   │                    │
        └─────────┬──────────┴────────────────────┘
                  │
        ┌─────────┼──────────────────────────────┐
        ▼         ▼                              ▼
   ┌─────────┐ ┌──────────┐                ┌─────────┐
   │  Redis  │ │  Kafka   │                │ MySQL × 4│
   │ stock + │ │ orders   │                │ (sharded │
   │   lua   │ │  topic   │                │  orders) │
   └─────────┘ └────┬─────┘                └─────────┘
                    │
                    ▼
              ┌──────────┐
              │ consumer │  writes orders to sharded MySQL
              └────┬─────┘
                   │
                   ▼ updates Redis queue token state
              ┌─────────┐
              │  Redis  │  (queue:{token} → success | failed)
              └─────────┘

  observability:
  ┌─────────────┐   ┌─────────────┐   ┌────────────┐
  │  Prometheus │   │   Grafana   │   │   Jaeger   │
  └──────┬──────┘   └──────┬──────┘   └──────┬─────┘
         │ scrape          │ view            │ OTLP
         └─────────────────┴─────────────────┘
                  ▲ from api / consumer
```

## 2. Request flow — happy path

```
client ─▶ POST /v1/seckill {activity_id, user_id, idempotency_token}
        │
        │  [trace] start root span "seckill.request"
        │  [auth] verify JWT (or skip in dev)
        │  [rate-limit] global QPS bucket; per-user RPM
        │  [idempotency] SETNX idem:{token} "processing" EX 60
        │     ├── set OK → continue
        │     └── exists → 409 with cached result
        │
        ▼
service.Seckill
  │  [activity-cache] local LRU(1s TTL) → fallback Redis(60s)
  │  [validate] window status, product enabled
  │
  │  [redis lua] EVAL stock_deduct.lua
  │     KEYS = stock:{aid}, user_buy:{aid}:{uid}
  │     ARGV = 1, per_user_limit
  │     return new_stock | -1 sold_out | -2 limit | -3 not_warmed
  │
  ├── -1 → return {status:"sold_out"}        (HTTP 410)
  ├── -2 → return {status:"exceeded"}        (HTTP 409)
  ├── -3 → return 503 (incident; alert)
  └── ≥0 → continue
  │
  │  [kafka produce] topic seckill_orders, key=user_id (partition affinity)
  │     value = {order_id, activity_id, user_id, token}
  │
  │  [redis] SET queue:{token} "queued" EX 600
  │
  ▼
return {status:"queued", token, remaining}    (HTTP 202)


  asynchronously:
consumer
  │  [poll] kafka topic
  │  [trace] continue trace via baggage / message header
  │  [shardkey] db = shardkey.DBIndex(user_id, dbCount)
  │  [insert] orders_{db} (idempotent via UNIQUE key)
  │     ├── duplicate → ignore (treat as success; can happen on retry)
  │     └── inserted  → continue
  │  [redis] SET queue:{token} "success:{order_id}" EX 3600
  │  [ack]
```

## 3. Failure flows

```
upstream MySQL down:
  consumer insert fails
  → retry with exponential backoff (max N)
  → after N → publish to seckill_orders_dlq
  → operator investigates DLQ; can replay

stock under-deducted (consumer fails persistently after stock reserved):
  → background sweeper compares Redis stock vs orders.count for each activity
  → reconcile by topping stock back up (stretch goal)

duplicate request from client retry:
  → idempotency middleware returns cached result
  → no new Kafka message produced
```

## 4. Deployment topology

### Local dev (single machine)
```
host
├── docker-compose: mysql, redis, kafka, jaeger, prometheus, grafana
├── api (go run)
└── consumer (go run, separate terminal)
```

### Bench day
```
client host (k6)  ──▶  api host (3 replicas behind nginx)  ──▶  middleware host (mysql/redis/kafka)
```

Rule: client and api on different hosts; mysql/redis/kafka can share if not bottlenecked (verify with iostat/iftop).

## 5. Module dependency

```
cmd/api ─▶ internal/handler ─▶ internal/service ─▶ internal/repo (redis, mysql)
                                                  └─▶ internal/infra/kafka (producer)

cmd/consumer ─▶ internal/service.OrderMaterializer ─▶ internal/repo (mysql, redis)
                                                  └─▶ internal/infra/kafka (consumer)

internal/infra/{otel, logger, redis, mysql, kafka}    cross-cutting
internal/middleware/{auth, ratelimit, idempotency, recovery, tracing}
pkg/{shardkey, ratelimit}                              reusable
```

## 6. Concurrency model

- API server: GOMAXPROCS = num cpu; Gin handlers are goroutines; no shared mutable state in handlers
- Per-request context with deadline (200ms default)
- Redis client: single shared `*redis.Client` with connection pool size 200
- MySQL: per-shard `*sqlx.DB` with pool size 100
- Kafka: producer pool size 10, consumer 16 partitions × 1 worker per partition

## 7. Versioning + ID schemes

- Order IDs: snowflake (worker id from POD_NAME hash)
- Activity IDs: pre-allocated by ops
- Idempotency tokens: client-generated UUIDv4
- Queue tokens: server-generated UUIDv7 (sortable)
