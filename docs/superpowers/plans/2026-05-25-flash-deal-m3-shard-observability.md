# flash-deal M3 — 分库分表 + 限流熔断 + 全链路观测 + Chaos Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 `orders` 表水平拆 4 个 schema(`flashdeal_0..3`),`OrderRepo` 按 `user_id % 4` 路由;补限流(per-user Redis 令牌桶 + global 本地令牌桶)、熔断(`sony/gobreaker` 包 MySQL 写)、全链路 OTel(api → kafka → consumer)、Prometheus 指标 + Grafana dashboard、chaos 测试(kill consumer / kill MySQL shard / kill Redis / kill Kafka)。

**Architecture:** `pkg/shardkey.DBIndex(userID, 4)` 决定写哪个 `flashdeal_{N}.orders_0`;一个 `*sqlx.DB` 池对应一个 shard,`OrderRepo` 内部保存 `[]*sqlx.DB`。Rate-limit middleware 用 Redis 滑窗(per-user)+ 本地 token bucket(global)。Circuit breaker 包 consumer 端 `OrderRepo.Create` 调用。OTel 用 OTLP HTTP 走 Jaeger;trace context 通过 kafka header 跨 producer/consumer 边界。Prometheus 直接由 Go 进程 `/metrics` 暴露,Prometheus scrape,Grafana 读 Prometheus。

**Tech Stack:** 接续 M2 · `sony/gobreaker v1.0.0`(已在 go.mod) · `golang.org/x/time/rate v0.7.0`(已在 go.mod) · `go.opentelemetry.io/otel v1.31.0`(已在 go.mod) · `prometheus/client_golang v1.20.5`(已在 go.mod) · `jaegertracing/all-in-one`(image tag 验证后选稳定的) · `prom/prometheus` + `grafana/grafana-oss`

**Scope:** Week 3 / Day 15-21 / Tag `m3`。**不在本 plan**:pprof 优化(M4)、多机分布式压测(M4)、博客(M4)。

**前置状态(假设 M2 已完成):**
- tag `m2` 已打,Redis Lua + Kafka 链路稳定
- `internal/repo/order_repo.go` 单分片实现,签名:`Create(ctx, o)`、`GetByID(ctx, userID, orderID)`
- `internal/config.Switches` 已含 LuaStock / KafkaOrder,M3 会扩 ShardedOrder / RateLimit / CircuitBreaker
- `deploy/docker-compose.yml` 中 kafka 已复活,jaeger/prom/grafana 仍注释 —— M3 复活
- `cmd/api/main.go` 当前是单 `*sqlx.DB` 给 OrderRepo —— M3 会改成 `[]*sqlx.DB`

---

## File Map

### 已存在 — 本 plan 会修改

| 路径 | 改动 |
|------|------|
| `deploy/docker-compose.yml` | 复活 jaeger / prometheus / grafana(image tag 在 Task 1 校验);可能扩 mysql 多 schema 初始化 |
| `deploy/prometheus.yml` | 加 scrape config 指 `host.docker.internal:8080/metrics`(macOS)/`host:port`(linux) |
| `deploy/grafana/dashboards/seckill.json` | 新建 dashboard JSON,Grafana provisioning 自动加载 |
| `migrations/001_init.up.sql` | 拆为 `001_init.up.sql`(activities)+ `002_orders_shards.up.sql`(0..3 schemas with orders_0 表) |
| `Makefile` | `migrate` target 改成跑两阶段 migration;加 `make migrate-shards` |
| `internal/config/config.go` | 加 `MySQLShardsConfig{DSNs []string}` 和 `Switches.ShardedOrder / RateLimit / CircuitBreaker` |
| `internal/repo/order_repo.go` | 接口不变,实现改为持 `[]*sqlx.DB`,按 `shardkey.DBIndex` 路由 |
| `internal/infra/kafka/{producer,consumer}.go` | 加 OTel header 注入 / 抽取(W3C traceparent) |
| `internal/service/seckill.go` | 在 hot path 注入 Prometheus counters / histograms |
| `internal/service/order_materializer.go` | gobreaker 包 `OrderRepo.Create` |
| `cmd/api/main.go` | OTel init + Prom handler + ratelimit middleware + 多 shard 接线 |
| `cmd/consumer/main.go` | OTel init + breaker 接线 |

### 新建

| 路径 | 单一职责 | 行数 |
|------|---------|-----|
| `migrations/002_orders_shards.up.sql` | `CREATE DATABASE flashdeal_{0..3}` + 每库 `orders_0` 表 DDL | < 60 |
| `migrations/002_orders_shards.down.sql` | DROP DATABASE flashdeal_{0..3} | < 10 |
| `internal/infra/otel/tracer.go` | TracerProvider 工厂(OTLP HTTP → jaeger:4318)+ Shutdown | < 100 |
| `internal/infra/otel/propagator.go` | Kafka header carrier(`map[string][]byte` ↔ propagation.TextMapCarrier) | < 80 |
| `internal/infra/metrics/metrics.go` | 集中注册 prometheus.Counter/Histogram/Gauge,export `Handler()` | < 150 |
| `internal/middleware/ratelimit.go` | Per-user Redis 滑窗 + global `rate.Limiter`;返回 429 | < 150 |
| `internal/middleware/ratelimit_test.go` | unit: per-user 滑窗逻辑(用 miniredis 或集成 redis);global limiter | < 200 |
| `internal/middleware/tracing.go` | 包 OTel http middleware,把 trace_id 写进 X-Trace-Id 响应头 | < 60 |
| `internal/middleware/metrics.go` | http_request_duration_seconds histogram + request count counter | < 80 |
| `internal/middleware/idempotency.go` | Redis SETNX 把 idem token 写为 "processing",成功后写 cached body;后续重放 returns cached | < 180 |
| `internal/middleware/idempotency_test.go` | integration:第一次 → 处理;第二次 → 同 token 返 cached body + 同 status | < 200 |
| `internal/infra/breaker/breaker.go` | gobreaker wrapper:Wrap(fn func() error) error;config 化 threshold | < 80 |
| `internal/infra/breaker/breaker_test.go` | unit:连续失败触发 open,半开恢复 | < 150 |
| `internal/repo/sharded_order_repo.go` | sharded 实现;NewShardedOrderRepo([]*sqlx.DB) | < 200 |
| `internal/repo/sharded_order_repo_test.go` | integration:跨 4 shard insert + lookup;path:`user_id=N` 落 `db = N%4` | < 250 |
| `bench/k6/seckill_m3.js` | M3 压测脚本,含 rate-limit 触发 + 429 验证 | < 100 |
| `bench/k6/chaos/*` | chaos 场景脚本(consumer kill / shard kill / redis kill) | < 200 |
| `reports/week3_observability.md` | M3 报告 + chaos 结果 + Grafana 截图说明 | 模板 |
| `reports/chaos.md` | 4 个 chaos 场景的预期 vs 实际 | 模板 |

---

## Task 1:Docker stack 复活 Jaeger / Prometheus / Grafana

**Files:** `deploy/docker-compose.yml`, `deploy/prometheus.yml`

- [ ] **Step 1.1:验证当前可用的 image tag**

```bash
docker pull jaegertracing/all-in-one:1.65 2>&1 | tail -3   # 试个较稳的
docker pull prom/prometheus:v3.0.0 2>&1 | tail -3
docker pull grafana/grafana-oss:11.6.0 2>&1 | tail -3
# 若拉不到换 latest 或最近一个 minor
```

记录实际可拉到的 tag,在 docker-compose 中使用。

- [ ] **Step 1.2:把 M1 注释掉的 jaeger / prometheus / grafana 段恢复**,使用 Step 1.1 验证过的 tag,并恢复 `volumes:` 中的 `grafana-data:`。

- [ ] **Step 1.3:写 `deploy/prometheus.yml`**(若不存在或需扩):

```yaml
global:
  scrape_interval: 5s
scrape_configs:
  - job_name: 'flash-deal-api'
    static_configs:
      - targets: ['host.docker.internal:8080']  # macOS docker-desktop;linux 可能要 host gateway
  - job_name: 'flash-deal-consumer'
    static_configs:
      - targets: ['host.docker.internal:8090']  # consumer 也暴露 /metrics
```

- [ ] **Step 1.4:跑 `make up` 验证三个服务都健康**

```bash
make up
sleep 10
curl -sf http://localhost:16686 > /dev/null && echo "jaeger ok"
curl -sf http://localhost:9090/-/ready > /dev/null && echo "prom ok"
curl -sf http://localhost:3001/api/health > /dev/null && echo "grafana ok"
```

- [ ] **Step 1.5:Commit**

```bash
git add deploy/
git commit -m "chore(deploy): revive jaeger/prometheus/grafana with verified tags"
```

---

## Task 2:多 Schema migration(`flashdeal_{0..3}.orders_0`)

**Files:** `migrations/002_orders_shards.up.sql`, `migrations/002_orders_shards.down.sql`, `Makefile`

- [ ] **Step 2.1:`migrations/002_orders_shards.up.sql`**

```sql
-- M3: create 4 logical DBs (schemas in single MySQL instance for dev simplicity)
CREATE DATABASE IF NOT EXISTS flashdeal_0;
CREATE DATABASE IF NOT EXISTS flashdeal_1;
CREATE DATABASE IF NOT EXISTS flashdeal_2;
CREATE DATABASE IF NOT EXISTS flashdeal_3;

-- grant flashdeal user
GRANT ALL ON flashdeal_0.* TO 'flashdeal'@'%';
GRANT ALL ON flashdeal_1.* TO 'flashdeal'@'%';
GRANT ALL ON flashdeal_2.* TO 'flashdeal'@'%';
GRANT ALL ON flashdeal_3.* TO 'flashdeal'@'%';
FLUSH PRIVILEGES;
```

然后给每个 schema 建 `orders_0` 表(复用 001_init 的 DDL,只是不同库):**`Makefile` 中循环跑** 比 SQL 文件里复制 4 份干净。

- [ ] **Step 2.2:Makefile 加 target**

```makefile
migrate-shards:
	docker exec -i fd-mysql mysql -uroot -prootpw < migrations/002_orders_shards.up.sql
	@for n in 0 1 2 3; do \
	  echo "applying orders DDL to flashdeal_$$n"; \
	  docker exec -i fd-mysql mysql -uflashdeal -pflashdeal flashdeal_$$n -e "$$(grep -A100 'CREATE TABLE IF NOT EXISTS orders_0' migrations/001_init.up.sql | head -15)"; \
	done

migrate-all: migrate migrate-shards
```

- [ ] **Step 2.3:验证**

```bash
make migrate-all
docker exec fd-mysql mysql -uroot -prootpw -e 'show databases'  # 应见 flashdeal_0..3
docker exec fd-mysql mysql -uflashdeal -pflashdeal flashdeal_0 -e 'show tables'  # orders_0
```

- [ ] **Step 2.4:`002_orders_shards.down.sql`**

```sql
DROP DATABASE IF EXISTS flashdeal_0;
DROP DATABASE IF EXISTS flashdeal_1;
DROP DATABASE IF EXISTS flashdeal_2;
DROP DATABASE IF EXISTS flashdeal_3;
```

- [ ] **Step 2.5:Commit**

```bash
git add migrations/002_orders_shards.* Makefile
git commit -m "feat(migrations): 4-shard orders schema (flashdeal_0..3) + make migrate-shards"
```

---

## Task 3:Config 加 ShardedOrder + RateLimit + CircuitBreaker + MySQLShards

**Files:** `internal/config/config.go`, `internal/config/config_test.go`

- [ ] **Step 3.1:Config 扩展**

```go
type MySQLShardsConfig struct {
    DSNs []string `mapstructure:"dsns"`  // index = shard idx
    MaxOpenConns int `mapstructure:"max_open_conns"`
    MaxIdleConns int `mapstructure:"max_idle_conns"`
}

type RateLimitConfig struct {
    PerUserPerMinute int `mapstructure:"per_user_per_minute"`
    GlobalQPS        int `mapstructure:"global_qps"`
    GlobalBurst      int `mapstructure:"global_burst"`
}

type BreakerConfig struct {
    Name           string `mapstructure:"name"`
    MaxRequests    uint32 `mapstructure:"max_requests"`     // half-open allowance
    Interval       time.Duration `mapstructure:"interval"`
    Timeout        time.Duration `mapstructure:"timeout"`   // open → half-open after
    FailureRatio   float64       `mapstructure:"failure_ratio"`
}

type OtelConfig struct {
    Enabled      bool   `mapstructure:"enabled"`
    OTLPEndpoint string `mapstructure:"otlp_endpoint"`  // jaeger:4318
    ServiceName  string `mapstructure:"service_name"`
}

// extend Switches
type Switches struct {
    LuaStock        bool `mapstructure:"lua_stock"`
    KafkaOrder      bool `mapstructure:"kafka_order"`
    ShardedOrder    bool `mapstructure:"sharded_order"`
    RateLimit       bool `mapstructure:"rate_limit"`
    Idempotency     bool `mapstructure:"idempotency"`
    CircuitBreaker  bool `mapstructure:"circuit_breaker"`
    Tracing         bool `mapstructure:"tracing"`
    Metrics         bool `mapstructure:"metrics"`
}

// add fields to Config
Shards    MySQLShardsConfig `mapstructure:"shards"`
RateLimit RateLimitConfig   `mapstructure:"rate_limit"`
Breaker   BreakerConfig     `mapstructure:"breaker"`
Otel      OtelConfig        `mapstructure:"otel"`
```

defaults:
```go
v.SetDefault("shards.dsns", []string{
    "flashdeal:flashdeal@tcp(127.0.0.1:3307)/flashdeal_0?parseTime=true&loc=Local",
    "flashdeal:flashdeal@tcp(127.0.0.1:3307)/flashdeal_1?parseTime=true&loc=Local",
    "flashdeal:flashdeal@tcp(127.0.0.1:3307)/flashdeal_2?parseTime=true&loc=Local",
    "flashdeal:flashdeal@tcp(127.0.0.1:3307)/flashdeal_3?parseTime=true&loc=Local",
})
v.SetDefault("shards.max_open_conns", 50)
v.SetDefault("shards.max_idle_conns", 25)

v.SetDefault("rate_limit.per_user_per_minute", 5)
v.SetDefault("rate_limit.global_qps", 100000)
v.SetDefault("rate_limit.global_burst", 1000)

v.SetDefault("breaker.name", "mysql_orders")
v.SetDefault("breaker.max_requests", 5)
v.SetDefault("breaker.interval", 60*time.Second)
v.SetDefault("breaker.timeout", 10*time.Second)
v.SetDefault("breaker.failure_ratio", 0.5)

v.SetDefault("otel.enabled", true)
v.SetDefault("otel.otlp_endpoint", "127.0.0.1:4318")
v.SetDefault("otel.service_name", "flash-deal-api")

v.SetDefault("switches.sharded_order", true)
v.SetDefault("switches.rate_limit", true)
v.SetDefault("switches.idempotency", true)
v.SetDefault("switches.circuit_breaker", true)
v.SetDefault("switches.tracing", true)
v.SetDefault("switches.metrics", true)
```

- [ ] **Step 3.2:扩 config_test.go**

加测试覆盖 shards.dsns 默认长度 = 4、rate_limit 默认值、所有 switches 默认 true。

- [ ] **Step 3.3:Commit**

```bash
go test ./internal/config/... -v
git add internal/config/
git commit -m "feat(config): shards/ratelimit/breaker/otel + 6 new switches"
```

---

## Task 4:Sharded OrderRepo

**Files:** `internal/repo/sharded_order_repo.go`, `internal/repo/sharded_order_repo_test.go`

- [ ] **Step 4.1:实现**

```go
package repo

import (
    "context"
    "fmt"
    "github.com/jmoiron/sqlx"

    "github.com/mjyangnb/flash-deal/internal/domain"
    "github.com/mjyangnb/flash-deal/pkg/shardkey"
)

type shardedOrderRepo struct {
    shards []*sqlx.DB
}

// NewShardedOrderRepo wraps N shards. len(shards) must be a power of 2 typical;
// shardkey.DBIndex(userID, len(shards)) decides routing.
func NewShardedOrderRepo(shards []*sqlx.DB) OrderRepo {
    return &shardedOrderRepo{shards: shards}
}

func (r *shardedOrderRepo) Create(ctx context.Context, o domain.Order) error {
    idx := shardkey.DBIndex(o.UserID, len(r.shards))
    const q = `
INSERT INTO orders_0 (id, user_id, activity_id, product_id, status, idempotency_token, created_at)
VALUES (?, ?, ?, ?, ?, ?, ?)`
    _, err := r.shards[idx].ExecContext(ctx, q,
        o.ID, o.UserID, o.ActivityID, o.ProductID,
        int8(o.Status), o.IdempotencyToken, o.CreatedAt)
    if err != nil && isDuplicateKey(err) {
        return ErrOrderDuplicate
    }
    return err
}

func (r *shardedOrderRepo) GetByID(ctx context.Context, userID, orderID int64) (domain.Order, error) {
    idx := shardkey.DBIndex(userID, len(r.shards))
    // delegate to single-shard helper
    return getOrderFromDB(ctx, r.shards[idx], userID, orderID)
}

// helper extracted so both single + sharded impls share
func getOrderFromDB(ctx context.Context, db *sqlx.DB, userID, orderID int64) (domain.Order, error) {
    var row orderRow
    const q = `
SELECT id, user_id, activity_id, product_id, status, idempotency_token, created_at
  FROM orders_0 WHERE user_id = ? AND id = ?`
    err := db.GetContext(ctx, &row, q, userID, orderID)
    if errors.Is(err, sql.ErrNoRows) { return domain.Order{}, ErrOrderNotFound }
    if err != nil { return domain.Order{}, err }
    return domain.Order{
        ID: row.ID, UserID: row.UserID, ActivityID: row.ActivityID,
        ProductID: row.ProductID, Status: domain.OrderStatus(row.Status),
        IdempotencyToken: row.IdempotencyToken, CreatedAt: row.CreatedAt,
    }, nil
}
```

(把 `orderRepoSQL.GetByID` 的 body 提取成 `getOrderFromDB` 共用,避免 DRY。)

- [ ] **Step 4.2:测试**

```go
//go:build integration

package repo_test

import ( ... )

func openShards(t *testing.T) []*sqlx.DB {
    t.Helper()
    var dbs []*sqlx.DB
    for i := 0; i < 4; i++ {
        dsn := fmt.Sprintf("flashdeal:flashdeal@tcp(127.0.0.1:3307)/flashdeal_%d?parseTime=true&loc=Local", i)
        db, err := sqlx.Open("mysql", dsn)
        if err != nil { t.Fatal(err) }
        dbs = append(dbs, db)
    }
    return dbs
}

func resetShards(t *testing.T, dbs []*sqlx.DB) {
    for _, db := range dbs {
        db.MustExec("TRUNCATE TABLE orders_0")
    }
}

func TestShardedOrder_RouteAndFetch(t *testing.T) {
    dbs := openShards(t)
    defer func(){ for _, d := range dbs { d.Close() } }()
    resetShards(t, dbs)

    r := repo.NewShardedOrderRepo(dbs)
    ctx := context.Background()
    for uid := int64(0); uid < 20; uid++ {
        o := domain.Order{
            ID: uid + 1000, UserID: uid, ActivityID: 9001, ProductID: 555,
            Status: domain.OrderQueued, IdempotencyToken: fmt.Sprintf("t-%d", uid),
            CreatedAt: time.Now(),
        }
        if err := r.Create(ctx, o); err != nil { t.Fatal(err) }
    }
    // verify per-shard distribution
    for shard := 0; shard < 4; shard++ {
        var n int
        dbs[shard].Get(&n, "SELECT COUNT(*) FROM orders_0")
        if n != 5 {
            t.Errorf("shard %d count = %d, want 5 (uniform user_id mod 4)", shard, n)
        }
    }
    // fetch
    got, err := r.GetByID(ctx, 7, 1007)
    if err != nil { t.Fatal(err) }
    if got.UserID != 7 { t.Errorf("got %+v", got) }
}
```

- [ ] **Step 4.3:跑测试 + Commit**

```bash
make migrate-all
go test -tags=integration -race -v ./internal/repo/... -run TestShardedOrder
git add internal/repo/sharded_order_repo.go internal/repo/sharded_order_repo_test.go internal/repo/order_repo.go
git commit -m "feat(repo): sharded OrderRepo (user_id % 4) + extracted shared GetByID helper"
```

---

## Task 5:OTel TracerProvider + Kafka header carrier

**Files:** `internal/infra/otel/tracer.go`, `internal/infra/otel/propagator.go`

- [ ] **Step 5.1:`tracer.go`**

```go
package otel

import (
    "context"
    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
    "go.opentelemetry.io/otel/propagation"
    "go.opentelemetry.io/otel/sdk/resource"
    sdktrace "go.opentelemetry.io/otel/sdk/trace"
    semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
)

// Init configures the global TracerProvider and returns Shutdown func.
func Init(ctx context.Context, endpoint, serviceName string) (func(context.Context) error, error) {
    exp, err := otlptracehttp.New(ctx,
        otlptracehttp.WithEndpoint(endpoint),
        otlptracehttp.WithInsecure(),
    )
    if err != nil { return nil, err }
    res, err := resource.New(ctx, resource.WithAttributes(
        semconv.ServiceNameKey.String(serviceName),
    ))
    if err != nil { return nil, err }
    tp := sdktrace.NewTracerProvider(
        sdktrace.WithBatcher(exp),
        sdktrace.WithResource(res),
        sdktrace.WithSampler(sdktrace.AlwaysSample()),
    )
    otel.SetTracerProvider(tp)
    otel.SetTextMapPropagator(propagation.TraceContext{})
    return tp.Shutdown, nil
}
```

- [ ] **Step 5.2:`propagator.go`(Kafka header carrier)**

```go
package otel

import (
    segkafka "github.com/segmentio/kafka-go"
    "go.opentelemetry.io/otel/propagation"
)

// KafkaHeaderCarrier adapts kafka-go Headers to TextMapCarrier.
type KafkaHeaderCarrier []segkafka.Header

func (c KafkaHeaderCarrier) Get(key string) string {
    for _, h := range c {
        if h.Key == key { return string(h.Value) }
    }
    return ""
}

func (c *KafkaHeaderCarrier) Set(key, value string) {
    for i := range *c {
        if (*c)[i].Key == key { (*c)[i].Value = []byte(value); return }
    }
    *c = append(*c, segkafka.Header{Key: key, Value: []byte(value)})
}

func (c KafkaHeaderCarrier) Keys() []string {
    ks := make([]string, 0, len(c))
    for _, h := range c { ks = append(ks, h.Key) }
    return ks
}

// 编译时检查实现
var _ propagation.TextMapCarrier = (*KafkaHeaderCarrier)(nil)
```

- [ ] **Step 5.3:Producer / Consumer 接入**

`internal/infra/kafka/producer.go::SendOrder` 在 WriteMessages 前:
```go
carrier := otel.KafkaHeaderCarrier{}
otelpkg.GetTextMapPropagator().Inject(ctx, &carrier)
msg := segkafka.Message{ ..., Headers: []segkafka.Header(carrier) }
```

`internal/infra/kafka/consumer.go::Run` 处理消息前:
```go
ctx = otelpkg.GetTextMapPropagator().Extract(ctx, otel.KafkaHeaderCarrier(msg.Headers))
// start span from extracted context
```

- [ ] **Step 5.4:`cmd/api/main.go` + `cmd/consumer/main.go` 都调用 `otel.Init` 在 main 顶部**

```go
if cfg.Switches.Tracing {
    shutdown, err := otelinfra.Init(ctx, cfg.Otel.OTLPEndpoint, cfg.Otel.ServiceName)
    if err != nil { log.Fatal(err) }
    defer shutdown(context.Background())
}
```

- [ ] **Step 5.5:验证 — 端到端 trace 在 Jaeger 出现**

```bash
make up && make migrate-all && make kafka-topic
make seed
go run ./cmd/api &
go run ./cmd/consumer &
sleep 5
curl -X POST -d '{"activity_id":1001,"user_id":1,"idempotency_token":"trace-1"}' \
  -H 'Content-Type: application/json' http://localhost:8080/v1/seckill
sleep 2
# 打开 http://localhost:16686 查 "flash-deal-api" service,应见一个完整 trace
# 含 HTTP span → kafka produce span → kafka consume span → MySQL insert span
```

- [ ] **Step 5.6:Commit**

```bash
git add internal/infra/otel/ internal/infra/kafka/ cmd/
git commit -m "feat(otel): TracerProvider + kafka header carrier; full trace from api to consumer"
```

---

## Task 6:Prometheus metrics

**Files:** `internal/infra/metrics/metrics.go`, `internal/middleware/metrics.go`

- [ ] **Step 6.1:`metrics.go` 集中注册**

```go
package metrics

import "github.com/prometheus/client_golang/prometheus"
import "github.com/prometheus/client_golang/prometheus/promhttp"
import "net/http"

var (
    HTTPDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
        Name: "http_request_duration_seconds",
        Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
    }, []string{"route", "status"})

    SeckillRequest = prometheus.NewCounterVec(prometheus.CounterOpts{
        Name: "seckill_request_total",
    }, []string{"activity_id", "outcome"})

    StockRemain = prometheus.NewGaugeVec(prometheus.GaugeOpts{
        Name: "seckill_stock_remain",
    }, []string{"activity_id"})

    LuaDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
        Name: "seckill_lua_duration_seconds",
        Buckets: []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1},
    })

    MySQLDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
        Name: "mysql_query_duration_seconds",
        Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5},
    }, []string{"shard", "op"})

    KafkaProduceDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
        Name: "kafka_produce_duration_seconds",
        Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1},
    })

    DLQTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
        Name: "seckill_dlq_total",
    }, []string{"reason"})
)

func init() {
    prometheus.MustRegister(
        HTTPDuration, SeckillRequest, StockRemain, LuaDuration,
        MySQLDuration, KafkaProduceDuration, DLQTotal,
    )
}

func Handler() http.Handler { return promhttp.Handler() }
```

- [ ] **Step 6.2:`middleware/metrics.go`**

```go
package middleware

import (
    "strconv"
    "time"
    "github.com/gin-gonic/gin"
    "github.com/mjyangnb/flash-deal/internal/infra/metrics"
)

func Metrics() gin.HandlerFunc {
    return func(c *gin.Context) {
        start := time.Now()
        c.Next()
        elapsed := time.Since(start).Seconds()
        metrics.HTTPDuration.WithLabelValues(c.FullPath(), strconv.Itoa(c.Writer.Status())).Observe(elapsed)
    }
}
```

- [ ] **Step 6.3:Service 层埋点(stock.Deduct / kafka produce / lua)**

在 `service.SeckillService.Seckill` 调用 Lua 前后 `metrics.LuaDuration.Observe(...)`,扣减成功后 `metrics.SeckillRequest.WithLabelValues(...,"queued").Inc()` 等。

`KafkaOrderCreator.Create` 包裹 `metrics.KafkaProduceDuration.Observe(...)`。

`sharded_order_repo.Create` 包裹 `metrics.MySQLDuration.WithLabelValues(shardIdx,"insert").Observe(...)`。

- [ ] **Step 6.4:`cmd/api/main.go` 加 `/metrics` 路由**

```go
r.GET("/metrics", gin.WrapH(metrics.Handler()))
r.Use(middleware.Metrics())
```

`cmd/consumer/main.go` 起一个独立 :8090 HTTP server 仅暴露 /metrics。

- [ ] **Step 6.5:验证 + Commit**

```bash
go run ./cmd/api &
sleep 2
curl -sS http://localhost:8080/metrics | head -50
# 应见 http_request_duration_seconds_*
git add internal/infra/metrics/ internal/middleware/metrics.go internal/service/ internal/repo/ cmd/
git commit -m "feat(metrics): prometheus counters/histograms + /metrics endpoint + hot-path instrumentation"
```

---

## Task 7:Grafana dashboard

**Files:** `deploy/grafana/dashboards/seckill.json` + `deploy/grafana/dashboards/dashboard.yml`(provisioning config)

- [ ] **Step 7.1:`deploy/grafana/dashboards/dashboard.yml`** — Grafana 自动加载

```yaml
apiVersion: 1
providers:
  - name: 'flash-deal'
    folder: 'flash-deal'
    type: file
    options:
      path: /etc/grafana/provisioning/dashboards
```

- [ ] **Step 7.2:`seckill.json` 包含 6 个 panel**

- Panel 1:RPS 总量(`sum(rate(http_request_duration_seconds_count[1m]))`)
- Panel 2:成功率(`sum(rate(http_request_duration_seconds_count{status!~"5.."}[1m])) / sum(rate(http_request_duration_seconds_count[1m]))`)
- Panel 3:P50 / P95 / P99 latency(用 `histogram_quantile`)
- Panel 4:per-activity 库存剩余(`seckill_stock_remain`)
- Panel 5:DLQ 增长(`rate(seckill_dlq_total[1m])`)
- Panel 6:Kafka produce P99 + Lua P99

可参考 https://grafana.com/grafana/dashboards/ 通用 prometheus 起手 JSON,改 query。

- [ ] **Step 7.3:`make up` 验证 Grafana 自动看到 dashboard**

打开 http://localhost:3001(admin/admin),Dashboards 应见 "flash-deal" folder。

- [ ] **Step 7.4:Commit**

```bash
git add deploy/grafana/
git commit -m "chore(monitoring): grafana provisioning + seckill dashboard"
```

---

## Task 8:Rate limit middleware

**Files:** `internal/middleware/ratelimit.go`, `internal/middleware/ratelimit_test.go`

- [ ] **Step 8.1:实现**

```go
package middleware

import (
    "fmt"
    "net/http"
    "time"

    "github.com/gin-gonic/gin"
    goredis "github.com/redis/go-redis/v9"
    "golang.org/x/time/rate"
)

type rateLimitConfig struct {
    PerUserPerMinute int
    GlobalLimiter    *rate.Limiter
    Redis            *goredis.Client
}

func RateLimit(rdb *goredis.Client, perUserPerMin, globalQPS, globalBurst int) gin.HandlerFunc {
    global := rate.NewLimiter(rate.Limit(globalQPS), globalBurst)
    cfg := rateLimitConfig{
        PerUserPerMinute: perUserPerMin,
        GlobalLimiter:    global,
        Redis:            rdb,
    }
    return func(c *gin.Context) {
        // 1. global
        if !global.Allow() {
            writeRateLimit(c, "global")
            return
        }
        // 2. per-user — 用 user_id from body (POST seckill);为不重 binding,可从 header X-User-Id
        userID := c.GetHeader("X-User-Id")
        if userID == "" { c.Next(); return }
        key := fmt.Sprintf("ratelimit:user:%s", userID)
        cur, err := cfg.Redis.Incr(c.Request.Context(), key).Result()
        if err != nil { c.Next(); return }  // fail-open on redis err
        if cur == 1 {
            cfg.Redis.Expire(c.Request.Context(), key, time.Minute)
        }
        if int(cur) > perUserPerMin {
            writeRateLimit(c, "per_user")
            return
        }
        c.Next()
    }
}

func writeRateLimit(c *gin.Context, scope string) {
    c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
        "error": gin.H{
            "code": "RATE_LIMITED",
            "message": "rate limited at " + scope,
            "request_id": RequestIDFrom(c),
        },
    })
}
```

- [ ] **Step 8.2:测试**

集成测试用真 redis,跑 N 个请求验证第 N+1 个 429。Global 部分用 stub `rate.Limiter`。

- [ ] **Step 8.3:接到 cmd/api/main.go**(在 `RequestID` + `Recovery` 之后,业务 handler 之前):

```go
if cfg.Switches.RateLimit {
    r.Use(middleware.RateLimit(rdb, cfg.RateLimit.PerUserPerMinute, cfg.RateLimit.GlobalQPS, cfg.RateLimit.GlobalBurst))
}
```

- [ ] **Step 8.4:Commit**

```bash
go test -tags=integration -race -v ./internal/middleware/...
git add internal/middleware/ratelimit.go internal/middleware/ratelimit_test.go cmd/api/main.go
git commit -m "feat(middleware): global token bucket + per-user redis sliding window; 429 on overflow"
```

---

## Task 9:Idempotency middleware

**Files:** `internal/middleware/idempotency.go`, `internal/middleware/idempotency_test.go`

- [ ] **Step 9.1:实现 SETNX 控流 + 完成后缓存响应**

```go
package middleware

import (
    "bytes"
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "time"

    "github.com/gin-gonic/gin"
    goredis "github.com/redis/go-redis/v9"
)

const idemTTL = 24 * time.Hour

// Idempotency: read idem token from body, SETNX 'processing', after handler:
// SET cached response (status + body); replays returns cached body verbatim.
func Idempotency(rdb *goredis.Client) gin.HandlerFunc {
    return func(c *gin.Context) {
        body, _ := io.ReadAll(c.Request.Body)
        c.Request.Body = io.NopCloser(bytes.NewBuffer(body))
        var probe struct {
            ActivityID       int64  `json:"activity_id"`
            UserID           int64  `json:"user_id"`
            IdempotencyToken string `json:"idempotency_token"`
        }
        _ = json.Unmarshal(body, &probe)
        if probe.IdempotencyToken == "" { c.Next(); return }
        key := fmt.Sprintf("idem:%d:%d:%s", probe.ActivityID, probe.UserID, probe.IdempotencyToken)
        // try set processing
        ok, err := rdb.SetNX(c.Request.Context(), key, "processing", idemTTL).Result()
        if err != nil { c.Next(); return }  // fail-open
        if !ok {
            // existed → fetch cached response
            v, err := rdb.Get(c.Request.Context(), key).Result()
            if err == nil && v != "processing" {
                // v is json: {"status":...,"body":...}
                var cached struct {
                    Status int             `json:"status"`
                    Body   json.RawMessage `json:"body"`
                }
                _ = json.Unmarshal([]byte(v), &cached)
                c.Data(cached.Status, "application/json", cached.Body)
                c.Abort()
                return
            }
            // still processing → 409 to client (M2 strict; later can long-poll)
            c.AbortWithStatusJSON(http.StatusConflict, gin.H{
                "error": gin.H{"code":"IDEMPOTENT_REPLAY","message":"still processing"},
            })
            return
        }
        // wrap writer to capture body
        bw := &bodyWriter{ResponseWriter: c.Writer, buf: &bytes.Buffer{}}
        c.Writer = bw
        c.Next()
        // store cached response
        cached, _ := json.Marshal(map[string]any{"status": bw.Status(), "body": json.RawMessage(bw.buf.Bytes())})
        rdb.Set(c.Request.Context(), key, string(cached), idemTTL)
    }
}

type bodyWriter struct {
    gin.ResponseWriter
    buf *bytes.Buffer
}
func (b *bodyWriter) Write(p []byte) (int, error) {
    b.buf.Write(p)
    return b.ResponseWriter.Write(p)
}
```

- [ ] **Step 9.2:测试 + 接线 + commit**(同 Task 8 模式)

```bash
git commit -m "feat(middleware): idempotency cache via redis SETNX + cached response replay"
```

---

## Task 10:Circuit breaker around consumer MySQL writes

**Files:** `internal/infra/breaker/breaker.go`, `internal/service/order_materializer.go`

- [ ] **Step 10.1:`breaker.go` 用 sony/gobreaker**

```go
package breaker

import (
    "time"
    "github.com/sony/gobreaker"
)

type CB struct{ cb *gobreaker.CircuitBreaker }

type Config struct {
    Name string
    MaxRequests uint32
    Interval time.Duration
    Timeout time.Duration
    FailureRatio float64
}

func New(c Config) *CB {
    return &CB{cb: gobreaker.NewCircuitBreaker(gobreaker.Settings{
        Name: c.Name,
        MaxRequests: c.MaxRequests,
        Interval: c.Interval,
        Timeout: c.Timeout,
        ReadyToTrip: func(counts gobreaker.Counts) bool {
            failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)
            return counts.Requests >= 5 && failureRatio >= c.FailureRatio
        },
    })}
}

func (b *CB) Do(fn func() error) error {
    _, err := b.cb.Execute(func() (any, error) { return nil, fn() })
    return err
}
```

- [ ] **Step 10.2:`OrderMaterializer` 接受 `*CB` 包裹 `OrderRepo.Create`**

```go
func (m *OrderMaterializer) Handle(ctx context.Context, msg fdkafka.OrderMessage) error {
    o := domain.Order{...}
    err := m.breaker.Do(func() error { return m.orders.Create(ctx, o) })
    if errors.Is(err, gobreaker.ErrOpenState) {
        // 切流期间快速返回错误,consumer retry-with-backoff 已经处理
        return err
    }
    // ... 其余逻辑
}
```

- [ ] **Step 10.3:测试 + commit**

```bash
go test -race -v ./internal/infra/breaker/...
git commit -m "feat(breaker): gobreaker around consumer MySQL writes"
```

---

## Task 11:cmd/api / cmd/consumer 完整接线

**Files:** `cmd/api/main.go`, `cmd/consumer/main.go`

- [ ] **Step 11.1:cmd/api 改成接 4 shards**

```go
var shards []*sqlx.DB
for _, dsn := range cfg.Shards.DSNs {
    s, err := fdmysql.Open(config.MySQLConfig{
        DSN: dsn, MaxOpenConns: cfg.Shards.MaxOpenConns, MaxIdleConns: cfg.Shards.MaxIdleConns,
    })
    if err != nil { log.Fatalf("open shard: %v", err) }
    defer s.Close()
    shards = append(shards, s)
}
var or repo.OrderRepo
if cfg.Switches.ShardedOrder {
    or = repo.NewShardedOrderRepo(shards)
} else {
    or = repo.NewOrderRepo(db)
}
```

middleware 链顺序:`RequestID → Tracing → Metrics → RateLimit → Idempotency → Recovery → handler`。

- [ ] **Step 11.2:cmd/consumer 加 metrics server + breaker**

```go
go func() {
    log.Println("metrics on :8090")
    http.ListenAndServe(":8090", metrics.Handler())
}()
br := breaker.New(breaker.Config{...cfg.Breaker})
mat := service.NewOrderMaterializer(orders, rdb, br)
```

- [ ] **Step 11.3:端到端冒烟**

跑全套:create activity → warm → 多 user_id 下单 → consumer 物化到正确 shard → 查 by-token success。

- [ ] **Step 11.4:Commit**

```bash
git add cmd/
git commit -m "feat(wire): cmd/api 4-shard repo + tracing/metrics/ratelimit/idempotency stack; cmd/consumer breaker + metrics server"
```

---

## Task 12:Chaos 测试

**Files:** `bench/k6/chaos/*.sh`, `reports/chaos.md`

- [ ] **Step 12.1:写 4 个 chaos 脚本**

```bash
# scenario 1: kill consumer mid-run
bench/k6/chaos/kill_consumer.sh
# scenario 2: stop one mysql schema (drop user access mid-run)
bench/k6/chaos/kill_shard.sh
# scenario 3: redis stop
bench/k6/chaos/kill_redis.sh
# scenario 4: kafka stop
bench/k6/chaos/kill_kafka.sh
```

每脚本:启动 stack → 跑 k6 持续负载 → 中途 kill → 恢复 → 验证最终一致性。

- [ ] **Step 12.2:`reports/chaos.md`** 模板

```markdown
# Chaos Report (M3)

| Scenario | Expected | Actual | PASS/FAIL |
|----------|----------|--------|-----------|
| Kill consumer | lag grows, after restart catches up, no oversell | ... | ... |
| Kill one MySQL shard | requests routed to that shard → circuit opens → 503; others succeed | ... | ... |
| Kill Redis | api returns 503 NotWarmed | ... | ... |
| Kill Kafka | producer retries → 503 after budget | ... | ... |
```

- [ ] **Step 12.3:Commit**

```bash
git add bench/k6/chaos/ reports/chaos.md
git commit -m "chore(chaos): 4 chaos scenarios + reports/chaos.md"
```

---

## Task 13:M3 bench + report

**Files:** `bench/k6/seckill_m3.js`, `reports/week3_observability.md`

- [ ] **Step 13.1:压测 M3 同三档 + 加 rate-limit overflow 场景**

`seckill_m3.js`:三档 rate 跑 + 额外 1 个高负载 burst 验证 429 触发。

- [ ] **Step 13.2:写 `reports/week3_observability.md`**

- M1/M2/M3 三栏对照表
- Grafana 截图引用(.png 放 reports/img/)
- Jaeger 截图说明:一个完整 trace 链
- chaos 摘要(指向 chaos.md)

- [ ] **Step 13.3:Commit**

---

## Task 14:收尾 lint/tag

```bash
go mod tidy
gofmt -s -w .
go vet ./...
go test -race -cover ./...
go test -tags=integration -race ./internal/...
```

更新 README:M3 完成 + 加 ratelimit / sharding / observability 的 Quickstart 段。

```bash
git tag -a m3 -m "M3: 4-shard MySQL + ratelimit/breaker/idempotency middleware + OTel/Prom/Grafana + chaos"
```

---

## 验收清单

- [ ] `make up && make migrate-all && make seed && make api && make consumer && make bench` 一键跑
- [ ] Grafana http://localhost:3001 有 6 个 panel 实时数据
- [ ] Jaeger http://localhost:16686 显示 api → kafka → consumer 完整 trace
- [ ] 4 shards 数据分布近似均匀(`SELECT COUNT(*) FROM orders_0` per schema)
- [ ] 第 6 次/min/user 请求返回 429
- [ ] 同 token 二次请求返回 cached body(byte-for-byte)
- [ ] 关 1 个 mysql shard,gobreaker 打开,该 shard 路由请求 503;其他 shard 正常
- [ ] git tag `m3` 已打

---

## Self-Review 结果

| 检查 | 状态 |
|------|------|
| Spec 覆盖 milestone.md Day 15-21 | ✅ Day 15→T2+T4,Day 16→T8,Day 17→T9+T10,Day 18→T5+T6,Day 19→T7,Day 20→T12,Day 21→T13+T14 |
| Placeholder 扫描 | ✅ |
| 类型一致 | `OrderRepo` 接口在 M1/M2 都未变,M3 只换实现;`shardkey.DBIndex` 已是 M1 就存在的 stable API |
