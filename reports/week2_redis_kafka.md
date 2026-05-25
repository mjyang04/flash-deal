# Week 2 — Redis Lua + Kafka Async (M2)

## Setup

| 项目 | 值 |
|------|---|
| Host | Apple Silicon arm64,18 cores,48 GiB RAM |
| Go | 1.26.3 |
| API | 单实例 `go build -o /tmp/fd-api ./cmd/api`,Lua + Kafka switches on |
| Consumer | 单实例 `go build -o /tmp/fd-consumer ./cmd/consumer` |
| Kafka | `apache/kafka:3.8.0` KRaft,RF=1 |
| Bench | k6 v2.0.0,scenario `m2_baseline`(constant-arrival-rate)+ 30% queue token polling |
| Commit | `3edea87` |

## M1 vs M2 Headline 对照

| Rate (rps) | M1 P50 / P95 | M2 P50 / P95 | 改善倍数 |
|------------|--------------|--------------|----------|
| 500        | 4.4 / 288 ms | 3.2 / 11.6 ms | P95 **~25×** |
| 1,000      | 1.3 / 2,380 ms | 0.64 / 11.2 ms | P95 **~210×** |
| 2,000      | 0.67 / 2,220 ms | 0.48 / 11.0 ms | P95 **~200×** |

> **本质改变**:M1 单行 MySQL `UPDATE ... WHERE total_stock >= ?` 是全局序列点(同库行锁串行化);M2 把扣减搬到 Redis Lua(单线程原子但 100k+ ops/s 量级),订单落库异步走 Kafka,API 路径只剩 Lua + Kafka produce + Redis SET。

## Detailed Workloads

| Rate (rps) | http_reqs(含 30% polling) | 完成 iter rps | dropped | failed(4xx,含 410) | P50 (ms) | P90 (ms) | P95 (ms) | max (ms) |
|------------|---------------------------|---------------|---------|---------------------|----------|----------|----------|----------|
| 500        | 17,361                    | 500           | 0       | 41.6%               | 3.2      | 11.4     | 11.6     | 26       |
| 1,000      | 32,817                    | 1,000         | 0       | 62.5%               | 0.64     | 11.1     | 11.2     | 14.5     |
| 2,000      | 63,015                    | 2,000         | 0       | 79.4%               | 0.48     | 10.8     | 11.0     | 23.8     |

说明:
- `iter rps == target rate`(M1 同档全部出现大量 dropped_iterations,client VU 池 1k 在 P95 > 1s 时撑不住;M2 P95 < 12ms,200 VU 足以打 2000 rps)
- failed% 跟 stock 用尽进度成正比:rate=2000 跑完后大量请求落到 `410 sold_out`(预期业务结果)
- final orders 表 = 27,260 行,跨三档累计 + 含 dup 路径产生的少量额外落库

## Correctness

- Redis Lua 1000G × 100 stock 测试:**success=100、soldOut=900、final stock=0** ✅(`TestStockRedis_NoOversell`)
- per-user limit 测试 PASS(`TestStockRedis_PerUserLimit`)
- not-warmed 测试 PASS(`TestStockRedis_NotWarmed`)
- Order materializer 幂等测试 PASS(同 token 二次消费不重复落库)
- 端到端冒烟:POST /v1/seckill → UUIDv7 queue_token → consumer 物化 → GET /v1/order/by-token/:token 返回 `success:{order_id}` ✅

## Findings

1. **Lua latency ≈ 1 RTT**:Redis Lua 单次 `EVALSHA` 在本机环境约 0.3-0.5ms,几乎是 Redis loopback 的下限;不再成为瓶颈
2. **Kafka produce 是新瓶颈来源**:RequiredAcks=All + BatchTimeout=10ms,单条 produce 约 8-10ms(本机 single broker);批量化(M3 加入)预期能进一步降到 < 5ms
3. **Stock leak 仍存在**:Lua 扣减成功后 Kafka produce 失败的窗口里 stock 已扣未落单。M3 会用 reconcile sweeper 解决
4. **Single-broker kafka 调参**:必须 `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1` 否则 `__consumer_offsets` 创建失败,group coordinator 不可用(已写入 docker-compose,文档化在 ADR)

## What's Next (M3)

- 4-shard `orders_{N}.orders_0`(按 `user_id % 4`)→ 单库写入瓶颈分散
- 限流(Redis 滑窗 + 本地 token bucket)+ idempotency middleware(M2 仅靠 MySQL UNIQUE KEY 兜底)
- gobreaker 包 consumer 写 MySQL → 部分 shard 故障时快速隔离
- OTel 全链路 trace(api → kafka → consumer)
- Prometheus + Grafana dashboard,chaos 测试(kill consumer / kill shard / kill redis / kill kafka)

## Artifacts

- k6 summary JSON:`/tmp/fd-bench-m2/rate-{500,1000,2000}.json`
- API + consumer log:`/tmp/api.log` `/tmp/consumer.log`
- 单元 + 集成测试:`go test -race ./...` + `go test -tags=integration -race ./...` 全 PASS
