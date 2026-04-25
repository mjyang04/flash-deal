# flash-deal — Reading & Reference List

## Books (read for depth, refer back for interview)

- **Designing Data-Intensive Applications** — Kleppmann. Chapters on replication, partitioning, transactions are directly relevant.
- **Database Internals** — Petrov. B+ tree, LSM, indexing.
- **Site Reliability Engineering** — Google. SLO/SLI vocabulary; chapter on overload handling.
- **The Linux Programming Interface** — Kerrisk. When investigating syscall-level bottlenecks.

## Papers / classic posts

- **Amazon Dynamo** — Decandia et al., 2007. Quorum, consistent hashing.
- **Cassandra** — Lakshman & Malik, 2010. Wide-row design.
- **Aurora** — Verbitski et al., 2017. Cloud-native MySQL replication.
- **Hystrix** — Netflix wiki. Circuit breaker patterns (also see Sony's gobreaker).
- **TCP Incast** — incident classic for understanding bursty failure.

## Go-specific

- *Go Concurrency Patterns* — Sameer Ajmani (Google I/O)
- *The Go Memory Model* — official spec
- *uber-go/guide* — style + concurrency anti-patterns
- *Effective Go* — official
- pprof tutorial — Russ Cox
- *Go GC pacer* — official runtime docs (for tuning GOGC / GOMEMLIMIT)

## Reference implementations

| Repo | What to study |
|------|--------------|
| `bilibili/kratos` | production Go microservice framework |
| `go-kratos/beer-shop` | sample app with Kratos |
| `cloudwego/kitex` | ByteDance Go RPC + microservice |
| `gin-gonic/examples` | Gin patterns |
| `redis/go-redis` | client + Lua + pipelining |
| `segmentio/kafka-go` | producer/consumer patterns |
| `IBM/sarama` | alternative kafka client (older but popular) |
| `sony/gobreaker` | circuit breaker reference |

## Seckill-specific reading

- 美团 *秒杀系统设计与优化* (well-known internal blog)
- 字节跳动 *高并发场景下的稳定性建设*
- 阿里 *双11 稳定性技术演进*
- *淘宝技术这十年* — book

## Bench tools

- `grafana/k6` docs — scenarios, executors, thresholds
- `wrk` README — Lua scripting
- `vegeta` — alt for steady-rate attacks
- `pprof` — official Go docs

## Things NOT to read (yet)

- Distributed transactions (Saga / Seata) — out of scope; we use idempotent insert + DLQ
- Service mesh (Istio / Linkerd) — out of scope; use plain Nginx LB
- Multi-region active-active — out of scope
- GraphQL gateway — irrelevant

## Interview prep companions

- *Why does your Lua script return -3 instead of -1 when stock not warmed?* — distinguish operational vs business outcome
- *Walk me through the consistency story* — Redis is working copy, MySQL eventually consistent via Kafka, UNIQUE KEY is safety net
- *What if Kafka is down?* — fail-fast at the API (return 503), don't keep deducting Redis without persistence
- *How do you prevent overselling under client retries?* — idempotency token + UNIQUE KEY
- *Walk me through scaling from 30k to 1M QPS* — Redis Cluster, more shards, sticky-routing API by user, HPA
- *How do you reconcile Redis stock vs MySQL orders?* — periodic sweeper job (stretch goal)
- *Where would you cache activity metadata?* — local LRU(1s) → Redis(60s) → MySQL; explain TTL choice
- *Why is partition_key=user_id?* — per-user ordering preserved; consumer parallelism = partition count
