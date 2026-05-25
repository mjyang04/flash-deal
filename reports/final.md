# flash-deal — Final Report (M4)

> 单机 4 周演进:从 SQL 行锁 same-host MVP,到 Redis Lua + Kafka + 4-shard MySQL + 完整观测的生产形态。**关键决策都有 profile / bench 数据支撑**,不做没数据的优化。

## Headline

| 项 | 数字 |
|----|------|
| 同 1000 rps 下 P95 改善 | **2,380 ms → 11.6 ms** = ~205× |
| 完整 M3 stack(4-shard + ratelimit + idem + breaker + OTel + metrics)下 P95 | 11-12 ms 三档稳定 |
| 4 shard 数据分布(uniform user_id)| 24.9 / 25.0 / 25.0 / 25.1 % |
| 库存正确性(1000G × 100 stock Redis Lua) | success=100, soldOut=900, final=0 ✅ |
| 集群 / 多机 30k QPS | 待补:本机 client cap = 1500 rps,需要分机重测 |

## Stage-by-stage 演进表

| Stage | Stack 增量 | 1000 rps P95 | 关键测试 | tag |
|-------|-----------|--------------|----------|-----|
| **M1 MVP** | gin + MySQL 行锁(同步 INSERT)| **2,380 ms** | `TestStockRepo_Deduct_NoOversell` 1000G×100 stock | `m1` |
| **M2 Async** | + Redis Lua + Kafka producer/consumer + UUIDv7 queue token | **11.2 ms** (~210×) | `TestStockRedis_NoOversell` + materializer 幂等 | `m2` |
| **M3 Shard+Obs** | + `orders_{0..3}` user_id%4 + ratelimit/idempotency middleware + gobreaker + OTel + Prom + Grafana | **11.6 ms** (持平,功能不损 perf) | sharded round-trip + ratelimit 429 + idem cache replay + breaker trip | `m3` |
| **M4 Profile+Release** | + pprof :6060/:6061 + opt-in mutex/block + `reports/profiling.md` + final 报告 | 12.0 ms @ 1500 rps × 60s | (profile-driven 否决了 3 个误导优化项)| `m4-release` |

## Architecture(M4 终态)

```
                     k6 → [api × 1]  → Redis Lua (stock + per-user limit)
                              │       → Kafka producer (seckill_orders, key=user_id)
                              │       → Redis SET queue:{uuid_v7}
                              │       → 202 + queue_token
                              ▼
                           middleware:
                              RequestID → Recovery → Metrics →
                              RateLimit(redis sliding + local bucket) →
                              Idempotency(setnx + cached body)
                              tracing (OTel → Jaeger)
                       async:
[consumer × 1] ← Kafka ← seckill_orders
              ├── gobreaker.Do(orderRepo.Create)
              │     └── shardkey.DBIndex(user_id, 4) → flashdeal_{N}.orders_0
              ├── if dup → idempotent success
              └── Redis SET queue:{token} = success:{order_id}  (or failed:{...} via DLQ)

观测面:Prometheus(:9090) → Grafana 6 panel (:3001) ← scrape api:8080/metrics + consumer:8090/metrics
       Jaeger UI :16686 ← OTel OTLP HTTP :4318
```

## Profiling findings

详见 [`reports/profiling.md`](./profiling.md)。一句话:**60% CPU 是 scheduler idle**,本机 client + server 共享 18 core 已到 client cap,不应该改 server 代码。

否决 3 个常见"看起来像优化"的方向:
1. **sync.Pool 化 fmt.Sprintf** → heap top 10 没出现,不需要
2. **MySQL/Redis pool 扩张** → mutex profile 无 conn wait
3. **GC 调优(GOMEMLIMIT)** → 堆 17 MB,GC 不是因素

## Chaos summary

`bench/k6/chaos/kill_consumer.sh` 已交付。预期:consumer kill → Kafka lag 累积 → restart → 物化追平 → `final_redis_stock + total_orders == initial_stock`。

`kill_shard.sh / kill_redis.sh / kill_kafka.sh` plan 已写,留待多机环境跑。

## Lessons / Trade-offs(面试可讲点)

1. **为什么 Redis Lua 不用 MySQL 行锁**
   `UPDATE ... WHERE stock >= ?` 全局序列化所有 deduct;单行 row lock 在 1k+ rps 已经把 P95 推到 2.4s。Lua 单线程原子但 100k+ ops/s 量级。**ADR-002**

2. **Stock 权威在 Redis,MySQL 是最终一致**
   API → Lua 扣减 → Kafka → consumer → MySQL。出错路径:Lua 成功但 Kafka produce 失败 → stock leak(consumer 没机会落库,但 Redis 已扣)。**缓解**:idempotency(消息可重投)+ DLQ + 周期 reconcile sweeper(M3 留作未实施 stretch)+ `orders` UNIQUE KEY 兜底(即便重新走流程也不会双订单)。

3. **为什么 shard by user_id 而不是 order_id**
   `"我的订单"` 是高频读,user_id sharding 保证单 shard 命中;order_id sharding 会让 user 级 list 走 fan-out。**ADR-003**

4. **为什么 4 shards 不是 16/32**
   方法论展示,不是规模秀肌肉。4 shards 本机够 demo + 留 1 个 shard 给 chaos 杀。生产规模只是 DSN 数组扩长。

5. **为什么 Kafka KRaft 不要 Zookeeper**
   2024+ 标准。单 broker dev `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1` 是踩过的坑(默认 3 会让 `__consumer_offsets` 创建失败,group coordinator 不可用)— 已写入 docker-compose 注释。

6. **幂等住在 Redis,MySQL UNIQUE KEY 兜底**
   热路径不能 round-trip MySQL;Redis SETNX 24h TTL + 缓存响应。MySQL `UNIQUE (activity_id, user_id, token)` 是 worst-case 防线(Redis 数据丢了也不超卖)。**ADR-005**

7. **gobreaker 在 consumer 端包 MySQL,不在 API 端**
   API 端用的是 Redis + Kafka,本身已经是 fail-fast 路径(不写 MySQL)。breaker 只需要保护 consumer 写库时部分 shard 故障的场景。

8. **OTel kafka header carrier**
   producer Inject W3C traceparent → message Headers → consumer Extract → 同一 trace 跨 Kafka 边界。Jaeger 中看到完整 api-span → produce-span → consume-span → mysql-span 链条。

9. **rate limit 用 redis 滑窗 + 本地 token bucket 二层**
   global 100k QPS 用 `golang.org/x/time/rate` 本地;per-user 用 Redis `INCR + EXPIRE 60s`。redis 错误时 fail-open(不限流而非误杀)。

10. **不为没饱和的系统优化**
    M4 profile 跑出 client bottleneck → 拒绝改 server 代码。**这本身是面试加分点**:基于数据决策,不刷无意义 commit。

## 多机 / 进一步推到 30k QPS 的路径(口头讲述用)

1. client / server 分机:k6 跑在 host A,api+stack 在 host B
2. api 横向 3 replicas 后置 nginx LB(已写入 plan)
3. 4 → 16 shards(DSN config + 数据迁移)
4. Redis Cluster 拆 hot key
5. 这些都是配置 / scale 改动,不需要重写代码 — **架构已经为此预留**(`shardkey` package、config 化的 DSN 数组、port 化的 `OrderRepo` / `StockRepo` / `OrderCreator` 接口)

## Resume 数字

见 [`plan/resume_blurbs.md`](../plan/resume_blurbs.md):

> 单机本地复现 P95 11.6 ms @ 1k rps(Redis Lua + Kafka 异步 + 4-shard MySQL),并发 1000G × 100 stock 测试零超卖。M1 同链路同条件 P95 = 2,380 ms,**~205× 改善由 Lua + Kafka 异步落单架构带来**。
