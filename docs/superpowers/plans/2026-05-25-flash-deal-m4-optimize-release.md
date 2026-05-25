# flash-deal M4 — pprof 优化 + 终版压测 + 发布 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 M3 完整可观测系统上做 **pprof 三件套**(CPU / alloc / mutex)→ 定位 top 3 hotspots → 针对性优化(sync.Pool / 连接池 / GC 参数)→ 终版多机压测(若资源允许)→ 写 `reports/final.md` + 简历句 + 博客大纲 + tag `m4-release`。

**Architecture:** M4 只优化与发布,不引入新功能。技术栈不变,所有改动都是参数调优、热路径优化(对象复用 / 减少分配 / 缩短 GC pause)、新增 pprof endpoint、新增多机 k6 worker 编排脚本。

**Tech Stack:** 接续 M3 · 新增 `net/http/pprof` 标准库 endpoint · `runtime/debug.SetGCPercent` + `GOMEMLIMIT` env · 可选 `vegeta` / 第二台 host 协调多机压测

**Scope:** Week 4 / Day 22-28 / Tag `m4-release`。**目标硬指标**(plan/PRD.md):单机 ≥ 30k QPS、P99 ≤ 50ms、零超卖、压测报告含 Grafana 截图。**博客发布是 stretch goal**(若时间不够先 ship code + report + resume)。

**前置状态(假设 M3 已完成):**
- tag `m3` 已打,功能上完整(Lua + Kafka + 4 shards + ratelimit + breaker + idempotency + OTel + Prom + Grafana + chaos)
- `cfg.Switches.*` 8 个开关全部默认 on
- `reports/week3_observability.md` + `reports/chaos.md` 已存在

---

## File Map

### 修改

| 路径 | 改动 |
|------|------|
| `cmd/api/main.go` | 加 `net/http/pprof` 注册(`/debug/pprof/*`)|
| `cmd/consumer/main.go` | 同上,exposed on :8090 |
| `internal/service/seckill.go` | sync.Pool 重用 hot path 对象(JSON encoder / OrderMessage struct)|
| `internal/infra/kafka/producer.go` | 调 `BatchSize` / `BatchTimeout` 优化 throughput |
| `internal/infra/mysql/conn.go` | 暴露 `MaxIdleConns` / `MaxOpenConns` env override(M3 已经 config 化,这里只调推荐默认) |
| `Makefile` | 加 `pprof-cpu` / `pprof-heap` / `pprof-mutex` 一键采集 target |
| `bench/k6/seckill_final.js` | 终版压测(constant 30k QPS 60s + ramp 0→30k 5s)|

### 新建

| 路径 | 单一职责 |
|------|---------|
| `bench/k6/multi-host/coordinator.sh` | 多机压测协调脚本(host A 跑 k6 攻击 host B 上的 api+stack)|
| `bench/k6/multi-host/k6-worker.sh` | k6 worker 容器化(`grafana/k6:latest`)|
| `reports/profiling.md` | pprof 三件套结果 + 优化前后对照 |
| `reports/final.md` | 最终报告,合并 M1/M2/M3/M4 数据 + Grafana 截图 + 决策回顾 |
| `reports/img/*.png` | Grafana / Jaeger / pprof 截图(M3/M4 共用)|
| `docs/blog/seckill-10w-qps.md` | 博客大纲 + 草稿(若时间允许) |
| `plan/resume_blurbs.md` | M1 时就有的简历模板,M4 填实测数字 |

---

## Task 1:暴露 pprof endpoint

**Files:** `cmd/api/main.go`, `cmd/consumer/main.go`, `Makefile`

- [ ] **Step 1.1:`cmd/api/main.go` import + register**

```go
import (
    _ "net/http/pprof"
    "net/http"
)

// in main, after http.Server is built:
go func() {
    log.Println("pprof on :6060")
    if err := http.ListenAndServe(":6060", nil); err != nil {
        log.Printf("pprof: %v", err)
    }
}()
```

- [ ] **Step 1.2:cmd/consumer 同样,在 :6061**(避免与 api 冲突)

- [ ] **Step 1.3:Makefile**

```makefile
PPROF_DURATION ?= 30s

pprof-cpu:
	@echo "Capturing CPU profile for $(PPROF_DURATION)..."
	curl -sS "http://localhost:6060/debug/pprof/profile?seconds=$$(echo $(PPROF_DURATION) | tr -d s)" -o /tmp/cpu.pprof
	go tool pprof -text /tmp/cpu.pprof | head -30

pprof-heap:
	curl -sS http://localhost:6060/debug/pprof/heap -o /tmp/heap.pprof
	go tool pprof -text /tmp/heap.pprof | head -30

pprof-mutex:
	curl -sS http://localhost:6060/debug/pprof/mutex -o /tmp/mutex.pprof
	go tool pprof -text /tmp/mutex.pprof | head -30
```

- [ ] **Step 1.4:验证**

```bash
go run ./cmd/api &
sleep 2
curl -sS http://localhost:6060/debug/pprof/ | head -20
pkill -f cmd/api
```

Expected:列出 `goroutine / heap / threadcreate / block / mutex / profile / cmdline / symbol / trace`。

- [ ] **Step 1.5:Commit**

```bash
git add cmd/api/main.go cmd/consumer/main.go Makefile
git commit -m "feat(profile): pprof endpoints on :6060 (api) / :6061 (consumer) + make pprof-{cpu,heap,mutex}"
```

---

## Task 2:开 Mutex / Block profiling

**Files:** `cmd/api/main.go`, `cmd/consumer/main.go`

> 默认 Go runtime 不采 mutex / block profile;需 `runtime.SetMutexProfileFraction(1)` 和 `runtime.SetBlockProfileRate(1)`。M4 临时打开做 profile,生产关掉(性能开销小但非零)。

- [ ] **Step 2.1:加初始化代码**

```go
import "runtime"

if os.Getenv("FD_PROFILE") == "1" {
    runtime.SetMutexProfileFraction(1)
    runtime.SetBlockProfileRate(1)
    log.Println("mutex + block profiling enabled")
}
```

- [ ] **Step 2.2:Commit**

```bash
git commit -am "feat(profile): opt-in mutex/block profiling via FD_PROFILE=1"
```

---

## Task 3:采集 baseline pprof(M3 终态)

**Files:** `reports/profiling.md`(初稿)

- [ ] **Step 3.1:启全栈跑稳态压测同时抓 profile**

```bash
make up && make migrate-all && make seed
FD_PROFILE=1 go run ./cmd/api > /tmp/api.log 2>&1 &
FD_PROFILE=1 go run ./cmd/consumer > /tmp/consumer.log 2>&1 &
sleep 5
# 稳态 1500 rps × 60s
RATE=1500 DURATION=60s k6 run bench/k6/seckill_m3.js &
sleep 20  # 等稳态
make pprof-cpu PPROF_DURATION=30s > /tmp/cpu.txt
make pprof-heap > /tmp/heap.txt
make pprof-mutex > /tmp/mutex.txt
wait
pkill -f cmd/api; pkill -f cmd/consumer
```

- [ ] **Step 3.2:写 profiling.md 初稿,列 top 10 函数**

```markdown
# Profiling — pre-optimization baseline (M3 terminal state)

## CPU (30s)
<paste `go tool pprof -text` top 30>
分析:top hotspots are X / Y / Z

## Heap
分析:top allocators are A / B / C(可能:JSON marshal, fmt.Sprintf, gin context)

## Mutex / Block
分析:contended locks are sql.DB pool / sync.Map / ...
```

- [ ] **Step 3.3:基于 profile 制定 3 个具体优化项**(填到 profiling.md 末尾 "Hypotheses"):

例(实际以 profile 为准):
1. `fmt.Sprintf` 在 hot path 用 byte-buffer 替换
2. `OrderMessage` 通过 `sync.Pool` 重用
3. MySQL `MaxOpenConns` 从 100 调到 200(若 pool wait 是 mutex 热点)

- [ ] **Step 3.4:Commit**

```bash
git add reports/profiling.md
git commit -m "chore(profile): pre-optimization pprof captures + hypotheses"
```

---

## Task 4-6:逐个优化(每个 Task 是一个独立优化 + 微基准测试或重跑稳态对比)

> 实际优化项取决于 Task 3 的 profile 结果。这里给 3 个最常见的优化作为占位 task,实施时根据 profile 替换。

### Task 4:sync.Pool 复用 OrderMessage / json buffer

- [ ] **Step 4.1:加 pool**

```go
// internal/infra/kafka/producer.go
var msgPool = sync.Pool{ New: func() any { return &OrderMessage{} } }

func (p *Producer) SendOrder(...) error {
    m := msgPool.Get().(*OrderMessage)
    defer func() { *m = OrderMessage{}; msgPool.Put(m) }()
    // fill m, marshal, write
}
```

- [ ] **Step 4.2:重跑 30s 稳态 + 抓 heap profile 对比**

记录 alloc/s 变化。期望 alloc 降 N%。

- [ ] **Step 4.3:Commit**

### Task 5:MySQL/Redis pool 调优

- [ ] **Step 5.1:根据 mutex profile,如果 `*sql.DB.Conn` 是热点,把 `MaxOpenConns` 调大;如果 Redis Pool 是热点,加 `PoolSize`。改 config 默认值 + 文档。**

- [ ] **Step 5.2:对照重跑 + commit**

### Task 6:GC 调优

- [ ] **Step 6.1:`GOMEMLIMIT=2GiB GOGC=200 go run ./cmd/api`,对照默认设置**

- [ ] **Step 6.2:用 `runtime.ReadMemStats` 在 profile 周期内记录 NumGC / PauseTotalNs,对比**

- [ ] **Step 6.3:把推荐 env 写进 Makefile / docs**

---

## Task 7:终版 bench script + 数据采集

**Files:** `bench/k6/seckill_final.js`, `reports/final.md`

- [ ] **Step 7.1:`seckill_final.js`** — 三个 scenario 并存

```js
export const options = {
  scenarios: {
    cold_steady_30k: { executor: 'constant-arrival-rate', rate: 30000, ... },
    burst_5w: { executor: 'ramping-arrival-rate', stages: [{target:50000,duration:'5s'},{target:50000,duration:'30s'}], ... },
    long_sustained: { executor: 'constant-arrival-rate', rate: 10000, duration: '30m', startTime: '90s' },
  },
};
```

- [ ] **Step 7.2:跑全套**(若单机 CPU 已被 k6 吃掉,只跑 1 个 scenario;否则全跑)

- [ ] **Step 7.3:correctness final**

```sql
SELECT SUM(cnt) FROM (
  SELECT COUNT(*) cnt FROM flashdeal_0.orders_0
  UNION ALL SELECT COUNT(*) FROM flashdeal_1.orders_0
  UNION ALL SELECT COUNT(*) FROM flashdeal_2.orders_0
  UNION ALL SELECT COUNT(*) FROM flashdeal_3.orders_0
) t;
-- 应 == initial_stock - final_redis_stock
```

---

## Task 8:多机压测(若有 cloud 资源)

**Files:** `bench/k6/multi-host/*.sh`

> 若没条件,跳过 Task 8 + 在 final.md 中说明单机限制。

- [ ] **Step 8.1:租 2 个云 VM(任一 cloud,$5 一小时),一个跑 docker-compose + flash-deal,一个跑 k6**

- [ ] **Step 8.2:在 k6 host 安装 k6**:`apt install k6` 或下 binary

- [ ] **Step 8.3:跑 `seckill_final.js`,记录 single-host vs multi-host 数据差异**

- [ ] **Step 8.4:把数据填进 final.md**

---

## Task 9:终版 final.md + Grafana / Jaeger / pprof 截图

**Files:** `reports/final.md`, `reports/img/*.png`

骨架:

```markdown
# flash-deal — Final Report (M4)

## Headline
- Single-node QPS:**XXk** (P99 **YY ms**)
- Cluster QPS:**XXXk** (3 replicas, multi-host)
- Stock correctness:zero overselling across 100 trials

## Architecture
(mermaid diagram)

## Stage-by-stage table
| Stage | Stack | 1k rps P99 | Max sustained QPS | Correctness |
|-------|-------|-----------|-------------------|-------------|
| M1 | SQL row-lock | 2.4 s | 1.2k | PASS |
| M2 | + Redis Lua + Kafka | ... | ... | PASS |
| M3 | + Sharding + RateLimit + Breaker | ... | ... | PASS |
| M4 | + pprof optimization | ... | ... | PASS |

## Profiling findings
- Hotspot 1: ... → fix → -N% CPU
- Hotspot 2: ... → fix → -N% alloc
- Hotspot 3: ... → fix → -N% latency

## Chaos summary
(指向 reports/chaos.md)

## Lessons
- Why Redis Lua not MySQL row lock
- Why partition_key = user_id
- Why 4 shards not more
- Trade-off: Kafka adds eventual consistency, mitigated by ...
- What I'd do at 10x scale
```

附 Grafana / Jaeger 截图:

- `reports/img/grafana_qps.png`
- `reports/img/grafana_p99.png`
- `reports/img/grafana_stock_burndown.png`
- `reports/img/jaeger_trace.png`
- `reports/img/pprof_before_after.png`

---

## Task 10:更新 `plan/resume_blurbs.md` 填实测数字

**Files:** `plan/resume_blurbs.md`

把 M1 时的 `___k QPS` / `___ ms` 占位替换为 final.md 中的实测数字。

---

## Task 11:博客大纲(stretch)

**Files:** `docs/blog/seckill-10w-qps.md`

7-8 个 section 的提纲:
1. 问题陈述与目标
2. 架构演进 M1 → M4(数据驱动)
3. Redis Lua 为什么是关键(对比 MySQL 行锁)
4. Kafka 异步落单的 trade-off
5. 分库分表的方法论(选 4 不选 16 的理由)
6. 限流 / 熔断 / 幂等三件套
7. pprof 实战:从 1.2k 到 30k QPS
8. 复盘:如果重做会怎么做

writing-anti-ai pass(用本仓库 SKILLs/writing-anti-ai),然后发到博客平台。

---

## Task 12:收尾 + Tag m4-release

```bash
go mod tidy && gofmt -s -w . && go vet ./...
go test -race -cover ./... && go test -tags=integration -race ./internal/...

# README 加 M4 完成 + final.md 链接
git add README.md plan/resume_blurbs.md reports/final.md
git commit -m "release(m4): final report + resume numbers"
git tag -a m4-release -m "M4: pprof optimized + final report + Xk single-node QPS"
```

---

## 验收清单

- [ ] `reports/final.md` 含真实压测数字 + correctness 等式 + 至少 3 张 Grafana 截图
- [ ] `reports/profiling.md` 含 before/after pprof 对比
- [ ] `plan/resume_blurbs.md` 中文 + 英文版本数字都填好
- [ ] git tag `m4-release` 已打
- [ ] `make up && make migrate-all && make seed && make api && make consumer && make bench` 仍一键跑
- [ ] 全部 test PASS(race + cover + integration)
- [ ] (stretch) `docs/blog/seckill-10w-qps.md` 完稿

---

## Self-Review 结果

| 检查 | 状态 |
|------|------|
| Spec 覆盖 milestone.md Day 22-28 | ✅ Day 22→T1+T2+T3;Day 23→T4-6;Day 24→T7;Day 25→T7 correctness;Day 26-27→T11;Day 28→T12 |
| Placeholder | M4 的优化项天然依赖 profile 结果,所以 Task 4-6 用 "占位 + 实施时替换" 模式;不算 placeholder,而是"profile-driven"。每个 Task 自身仍有具体步骤 |
| 类型一致 | M4 不改类型,只调参数 / 加 sync.Pool / 加 pprof endpoint |
