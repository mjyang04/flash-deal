# flash-deal — Decision Log

## ADR-001: Go 1.22 over Java/Kotlin
**Date**: 2026-04-25
**Status**: Accepted

**Context**: FoodOrderSystem is C++ (niche in CN backend hiring). Need to broaden to a mainstream JD-aligned language.

**Decision**: Go 1.22+. Reasoning: simpler concurrency story for showcase; smaller scaffolding; matches字节/字节系/腾讯/美团/拼多多 prevalent stack.

**Alternatives**:
- Java + Spring Boot: more JD coverage, but slower to scaffold; weaker per-line performance story
- Kotlin + Ktor / SpringBoot: niche outside ByteDance
- Rust: too aggressive learning curve for 4 weeks

**Consequences**: GC tuning (GOMEMLIMIT) is part of the bench narrative.

---

## ADR-002: Reserve stock in Redis (Lua), persist orders via Kafka
**Date**: 2026-04-25
**Status**: Accepted

**Context**: Bottleneck of seckill is the *write hot key* on stock. MySQL row lock contention kills throughput.

**Decision**:
1. Redis is the **authoritative working stock** during the activity window
2. Lua script atomically deducts + per-user limit
3. Successful deduction enqueues a Kafka message
4. Consumer materializes the order in MySQL (sharded)

**Trade-offs**:
- Eventual consistency between "reservation success" and "order in DB"
- Mitigated with idempotent insert (UNIQUE KEY) + DLQ + reconcile sweeper
- Hard requirement: never report success unless the Lua deduct succeeded

---

## ADR-003: User-id sharding for orders (4 shards)
**Date**: 2026-04-25
**Status**: Accepted

**Decision**: `shard_idx = user_id % 4` for `orders_{N}` tables.

**Why user_id, not order_id**:
- "List my orders" is the dominant read pattern → must hit one shard
- Order_id sharding scatters user-level reads → slow

**Why 4, not 16/32**:
- Realistic for resume project; documents methodology not scale
- All sharding reasoning is the same; scale is just config

---

## ADR-004: Snowflake IDs (worker_id from POD_NAME hash)
**Date**: 2026-04-25
**Status**: Accepted

**Why not UUID**: Wide PK kills BTree locality; B+ tree split rate spikes.

**Why not auto-increment**: Cross-shard auto-increment requires coordination; defeats sharding.

**Why snowflake**: Time-ordered, 8-byte, no coordination needed if worker_id is unique (we derive from `POD_NAME`).

---

## ADR-005: Idempotency lives in Redis, not MySQL
**Date**: 2026-04-25
**Status**: Accepted

**Why**: Hot path; MySQL roundtrip per request is too expensive at 30k QPS.

**Safety net**: `orders` table has `UNIQUE KEY (activity_id, user_id, idempotency_token)` so even if Redis is bypassed, MySQL refuses duplicates.

**Data loss tolerance**: idempotency tokens have 24h TTL; after that re-issue OK because activity is over.

---

## ADR-006: Self-host Kafka (KRaft mode), not Pulsar / Redis Streams
**Date**: 2026-04-25
**Status**: Accepted

**Why Kafka**: Highest interview signal; standard in CN big-tech; segmentio/sarama maturity.

**Why not Redis Streams**: Adds queue-state to Redis hot key; couples reservation and queueing.

**Why KRaft (no Zookeeper)**: Simpler docker-compose; reflects 2024+ deployments.

---

## ADR-007: Skip mTLS / SSO in v1
**Date**: 2026-04-25
**Status**: Accepted

**Decision**: JWT bearer for auth; no service-mesh mTLS in v1.

**Reason**: Time-boxed; demonstrates middleware pattern not infosec rabbit hole.

**Note in interview**: be ready to articulate how to add Istio mTLS or service-account auth in production.

---

## ADR-008: gobreaker over hystrix-go
**Date**: 2026-04-25
**Status**: Accepted

**Why**: hystrix-go is unmaintained; sony/gobreaker is small, well-tested, good API.
