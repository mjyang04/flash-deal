# M4 Profiling — pre-optimization baseline

## Capture setup

- Workload: `RATE=1500 DURATION=60s k6 run bench/k6/seckill_m3.js`
- Sample window: 30s CPU profile + heap + mutex snapshot,starting at 15s into the run(过 steady-state)
- Stack: M3 终态(4 shards + Lua + Kafka + tracing + metrics + ratelimit + idempotency + breaker)
- `FD_PROFILE=1` enabled mutex + block profiling

## CPU(30s,top by cumulative)

```
Total: 10.21s sampled / 30.11s wall = 33.9% utilization

  cum %   func
  59.4%   runtime.mcall → runtime.schedule         ← 60% in scheduler idle
  53.2%   runtime.park_m
  43.0%   runtime.kevent                            ← kqueue wait for next request
  41.4%   runtime.netpoll
  29.0%   net/http.(*conn).serve
  27.6%   syscall.syscalln
  24.8%   gin.(*Engine).ServeHTTP → middleware chain
  24.6%     RateLimit (redis INCR/Expire)
  24.6%     Recovery / Metrics observe
```

**主要结论**:**60% time 是 scheduler 空转**,说明请求间隔大于处理时间 —— **系统未饱和**,client(k6 + curl + 共享 18-core host)是瓶颈。

## Heap(inuse_space)

```
Total: 16.9 MB
  21.6%  bufio.NewReaderSize    (gin/http req buffer)
  15.1%  runtime.mallocgc
  12.3%  bufio.NewWriterSize    (gin/http resp buffer)
   9.1%  regexp/syntax (一次性 init,prometheus label sanitize)
   9.1%  context.(*cancelCtx).Done
   6.2%  StartCPUProfile (this profile itself)
   ... 
```

**结论**:堆 < 20 MB,没有泄漏迹象;`fmt.Sprintf`、JSON marshal 等典型秒杀热点对象**没有进入 top 10**(因为请求 rate 在本机 1500 rps 已经是 client cap,server 处理能力远超此)。

## Mutex / Block

Snapshot 显示**无明显热锁**;`sql.DB.Conn` 等待路径未出现(MaxOpenConns=50 × 4 shards = 200 充裕)。

## Hypotheses(plan T3 要求,实测后结论)

| 假设 | 结论 |
|------|------|
| H1: `fmt.Sprintf` 在 hot path 应 sync.Pool 化 | **否决** — heap top 10 中未出现 |
| H2: `OrderMessage` 需 sync.Pool | **否决** — 1500 rps 下 alloc 不显著 |
| H3: MySQL pool 太小 | **否决** — mutex profile 无 conn wait |
| H4: Redis pool 太小 | **否决** — 同 H3 |
| H5: client 是瓶颈 | **确认** — 60% scheduler idle |

## 决定:不做引入性优化

profile 已经给出了清晰信号 —— 单机 client + server 共享 18 core 下,本机已到 client cap(k6 ramp 200 VU + 8 个其他容器 + GUI),server 端 P95 ~12ms 是真实下限。**继续推高 QPS 的正确路径是 client / server 分机**,而不是改 server 内部代码。

不引入未经数据支持的 sync.Pool / GC 调优 / 连接池扩张 —— 这些"看起来像优化"的改动在本场景里不会改善任何 percentile,只会增加复杂度。

## 仍然可做(后续 / 不阻塞 tag)

- 把 client 拆到第二台云 VM,重测 30k QPS scenario,验证 `cold_run_steady_30k`
- 加 `seckill_final.js` 含 30k constant 和 50k burst scenario(代码已就绪,跑实验需多机)
- 跑 `bench/k6/chaos/*.sh` 验证恢复路径

## Artifacts

- `/tmp/fd-pprof/cpu-before.pprof` — `go tool pprof -http=:8082 /tmp/fd-pprof/cpu-before.pprof` 交互查看
- `/tmp/fd-pprof/heap-before.pprof`
- `/tmp/fd-pprof/mutex-before.pprof`
- `/tmp/m4-bench-base.log` — 60s @ 1500 rps 全部 90,000 iters 完成
