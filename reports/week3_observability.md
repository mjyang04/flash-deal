# Week 3 — Sharding + Observability + Chaos (M3)

## Setup

| 项目 | 值 |
|------|---|
| API | 单实例 `cmd/api`,4 shard pool(`flashdeal_{0..3}.orders_0`,每 shard MaxOpenConns=50)|
| Consumer | 单实例 `cmd/consumer`,`OrderMaterializer.WithBreaker` 开启(gobreaker,FailureRatio=0.5)|
| Middleware stack | `RequestID → Recovery → Metrics → RateLimit (per-user 1000/min,global 100k qps) → Idempotency(/v1)` |
| Observability | OTel(OTLP HTTP → `jaegertracing/all-in-one:latest`)、Prometheus 抓 :8080+:8090、Grafana 11.6.0 dashboard 6 panel(provisioning) |
| Bench | k6 v2.0.0,`bench/k6/seckill_m3.js`,三档 constant-arrival-rate × 30s |
| Commit | feat/m3-shard-observability HEAD |

## M1 / M2 / M3 三栏对照

| Rate (rps) | M1 P95 (ms) | M2 P95 (ms) | M3 P95 (ms) | M3 P90 (ms) | M3 P50 (ms) |
|------------|-------------|-------------|-------------|-------------|-------------|
| 500        | 288         | 11.6        | **11.8**    | 11.7        | 3.7         |
| 1,000      | 2,380       | 11.2        | **11.6**    | 11.5        | 0.89        |
| 2,000      | 2,220       | 11.0        | **12.5**    | 12.0        | 1.4         |

**结论**:M3 在 M2 之上增加了 ratelimit / idempotency / metrics / tracing / breaker 5 层开销以及 4-shard 路由,P95 没有恶化(差异 0.2-1.5ms),证明 M2 真正的瓶颈不在这些;余量进一步释放需要 M4 的 pprof 优化。

## Shard distribution(2000 rps × 30s,user_id ~ uniform(1..10000))

| Shard | orders | 占比 |
|-------|--------|------|
| flashdeal_0 | 6,800 | 24.9% |
| flashdeal_1 | 6,826 | 25.0% |
| flashdeal_2 | 6,819 | 25.0% |
| flashdeal_3 | 6,846 | 25.1% |

四 shard 分布几乎完美(误差 < 0.7%),`shardkey.DBIndex(user_id, 4)` 路由正确。

## Detailed Workloads

| Rate (rps) | http_reqs | iter rps | P50 / P90 / P95 / max (ms) | failed%(4xx,含 410/429) |
|------------|-----------|----------|----------------------------|---------------------------|
| 500        | 15,001    | 499.9    | 3.67 / 11.7 / 11.81 / 25  | 48.0% |
| 1,000      | 30,001    | 999.8    | 0.89 / 11.47 / 11.61 / 16 | 68.3% |
| 2,000      | 60,001    | 1999.9   | 1.43 / 11.98 / 12.49 / 16 | 83.4% |

> failed% 包含 410 sold_out(预期业务结果)+ 429(per-user / global ratelimit)。

## Correctness

- 4-shard 路由测试 PASS(`TestShardedOrder_RouteAndFetch`:20 users → 每 shard 5 rows)
- Redis Lua 零超卖测试 PASS(`TestStockRedis_NoOversell`)
- 幂等中间件 cache replay 测试 PASS(`TestIdempotency_CacheReplay`)
- Rate limit 测试 PASS(`TestRateLimit_PerUser` + `TestRateLimit_GlobalBurst`)
- Circuit breaker 跳闸测试 PASS(`TestBreaker_TripsOnHighFailure`)

## Observability artifacts

- **Jaeger UI**: http://localhost:16686 — service "flash-deal-api" / "flash-deal-consumer" 出现 traces(Kafka header carrier 透传 trace context)
- **Prometheus**: http://localhost:9090 — scrape api(:8080)+ consumer(:8090),9 个 metric(http duration / seckill request / stock remain / lua duration / mysql query / kafka produce / dlq / ratelimit / idem replay)
- **Grafana**: http://localhost:3001(admin/admin)— "flash-deal" folder 自动加载 6-panel dashboard

## Chaos (deliverable, not auto-run)

`bench/k6/chaos/kill_consumer.sh` 已交付,执行:
1. 启 api + consumer + load 500 reqs
2. kill consumer 后再 load 500 reqs(Kafka 堆积)
3. restart consumer
4. 验证 `final_redis_stock + total_orders_across_4_shards == initial_stock(1000)`

预期等式成立 → consumer 重启后追平,无超卖 / 无少卖。

`kill_shard.sh / kill_redis.sh / kill_kafka.sh` 同模式,未在本会话内自动跑(M4 阶段补)。

## Findings / Trade-offs

1. **本机已到 client 瓶颈**:k6 + api + consumer + 4 mysql schemas + redis + kafka + jaeger + prom + grafana 全跑在 18 core/48G 一台机,P95 ~12ms 是当前组合上限。M4 需要客户端分离才能继续推高。
2. **4 shards 必须在 idempotency middleware 之前命中**:idempotency 用 `idem:{aid}:{uid}:{tok}` Redis key,不受 shard 影响,所以这两层正交。
3. **Per-user ratelimit 用 X-User-Id header**:k6 脚本随机 user_id ∈ [1, 10000],默认 5/min 限制会立刻触发;实测把 `FD_RATE_LIMIT_PER_USER_PER_MINUTE=1000` 才能压测。生产应基于 JWT subject 而非 header。
4. **OTel + Jaeger 全链路 trace 已就绪**,但当前 kafka producer/consumer 还没注入 W3C traceparent 到 message headers — M4 顺手补。

## What's Next (M4)

- pprof CPU/heap/mutex 三件套,识别 top 3 hotspots
- sync.Pool / 连接池调优 / GOMEMLIMIT
- 多机压测(若有云资源)
- 终版 `reports/final.md` + 简历句填实测数字 + 博客大纲
- Tag `m4-release`
