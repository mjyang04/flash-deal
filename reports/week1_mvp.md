# Week 1 — MVP Baseline (M1)

## Setup

| 项目 | 值 |
|------|---|
| Host | Apple Silicon arm64,18 cores,48 GiB RAM,macOS Darwin 25.5.0 |
| Go | 1.26.3 |
| API | 单实例 `go build -o /tmp/fd-api-bin ./cmd/api`,默认 `GOMAXPROCS=18` |
| MySQL | `mysql:8.0` via docker-compose,`innodb_buffer_pool_size=512M`,`max_connections=2000`,port 3307 |
| Redis | `redis:7-alpine`,M1 仅在 admin warm 时写 `stock:{id}` key,**不参与 deduct 路径**(M1 库存权威是 MySQL 行锁) |
| Bench | k6 v2.0.0,scenario `baseline_m1` (constant-arrival-rate),client + server 同机器 |
| Commit | `1e81c4e` |

> ⚠️ **Single-host bench**:k6 与 API 共享 host,数字仅作 v0→v1 对照基线,不是 M4 报告数。M3/M4 会换两机分布式压测。

## Workloads

每档前 reset:`UPDATE activities SET total_stock=10000` + `SET stock:1001 10000`。

| Rate (rps) | Duration | http_reqs | 完成 rps | failed%(4xx) | 412sold_out / 202queued | P50 (ms) | P90 (ms) | P95 (ms) | max (s) | dropped iters |
|------------|----------|-----------|--------|---------------|-------------------------|----------|----------|----------|---------|---------------|
| 500        | 30s      | 14,983    | 499    | 33.25%        | 4,983 / 10,000          | 4.4      | 218      | 288      | 0.74    | 18            |
| 1,000      | 30s      | 24,056    | 802    | 58.43%        | 14,056 / 10,000         | 1.26     | 1,900    | 2,380    | 5.63    | 5,944         |
| 2,000      | 30s      | 36,962    | 1,232  | 72.94%        | 26,962 / 10,000         | 0.67     | 1,700    | 2,220    | 5.79    | 23,038        |

说明:
- "failed" = HTTP 4xx,包括 410 sold_out / 409 dup — 都是**业务上预期**的 outcome,不是故障。
- k6 默认把 4xx 算 `http_req_failed`,所以这一列高不是 bug,是设计。
- 410 比例约 = `(total - 10000) / total`,因为每档 10k 库存被消耗完后剩余请求都 sold_out。
- `dropped_iterations` 反映 client 端 VU 池(maxVUs=1000)+ ramp-up 限制,1000/2000 rps 时已成为 client 自身的瓶颈,server 实际 sustained < 设定 rate。

## Correctness

```
三档累计 successful (queued) orders = 10,000 + 10,000 + 10,000 = 30,000
final stock (after rate=2000) = 0
sum(initial stock per round) = 30,000
orders rows in MySQL = 30,000
```

等式:**`final_stock(0) + total_orders_inserted(30,000) == sum_initial_stocks(30,000)`** ✅

并发零超卖单测(`TestStockRepo_Deduct_NoOversell`,1000 协程抢 100):**PASS**,success=100、soldOut=900、final stock=0。

无 UNIQUE KEY 违例(idempotency token 每请求 uuidv4)。

## Findings

1. **MySQL 行锁串行化是 M1 的明确瓶颈**:`UPDATE activities SET total_stock=total_stock-1 WHERE id=? AND total_stock>=?` 在单库单行上 1232 rps 已经把 P95 推到 2.2s。这正是 M2 切 Redis Lua 要解决的根因。
2. **dropped_iterations 增长比 rps 还快**:rate=1000 时 25% iters 被 drop,rate=2000 时 38%。client 端 VU 池在 P95 > 1s 时撑不住,后续 M3 需要把 client 移到另一台 host。
3. **idempotent_replay 路径正确生效**:同 token 二次请求稳定返回 409 IDEMPOTENT_REPLAY(由 MySQL `UNIQUE (activity_id, user_id, token)` 兜底,不是 Redis SETNX,因为 M1 没接 idempotency middleware)。
4. **Stock leak 已知**:duplicate 路径下 stock 已扣但 order 失败,M1 未补偿。M3 reconcile sweeper 处理。
5. **MySQL pool**:`MaxOpenConns=100` 在 2000 rps 下没饱和(`Threads_running` 未飙),瓶颈在锁不在连接。

## Next (M2 目标)

| 切换 | 期望效果 |
|------|----------|
| stock deduct → Redis Lua | 单档 1000 rps 时 P95 从 2.4s → < 50ms(单核 Redis Lua 实测可吃 10w+ ops/s) |
| order insert → Kafka 异步 | API 路径只剩 produce + redis 两次 round-trip,P99 < 20ms 在 client 不饱和前提下应可达 |
| 加排队 token 轮询接口 | 客户端可以 polling `GET /v1/order/by-token/:token` 拿落单结果 |

M2 重跑同三档,做对照表。

## Artifacts

- k6 summary JSON:`/tmp/fd-bench/rate-{500,1000,2000}.json`
- API log:`/tmp/fd-api.log`
- 测试:`go test -tags=integration -race ./...` 全 PASS(含 `TestStockRepo_Deduct_NoOversell`)
