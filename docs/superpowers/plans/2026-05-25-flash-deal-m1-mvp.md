# flash-deal M1 — MVP 单机同步版 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 跑通 flash-deal 秒杀的最短同步路径(校验活动 → MySQL 行锁扣库存 → INSERT 订单),拿到 baseline QPS / P99 数据,作为 M2 异步化的对照基线。

**Architecture:** 单机 Gin + 单库 MySQL + 单实例 Redis(M1 仅用 Redis 缓存活动元数据,不走 Lua、不走 Kafka)。以 SQL 行锁(`UPDATE ... SET stock = stock - 1 WHERE id = ? AND stock >= 1`)保证 M1 阶段不超卖,目的是建立测试 / 压测 / 监控的脚手架。M2 会用 Lua 替换扣库存,用 Kafka 替换 INSERT。

**Tech Stack:** Go 1.22 · Gin v1.10 · sqlx + mysql driver · go-redis v9 · zap · viper · sony/gobreaker(M3 启用)· k6 · docker-compose(MySQL 8 / Redis 7 / Kafka 3.8 KRaft / Jaeger / Prometheus / Grafana 已就绪)

**Scope:** Week 1 / Day 1-7 / Tag `m1` 之前的全部工作。**不在本 plan**:Redis Lua 原子扣减(M2)、Kafka producer/consumer(M2)、分库分表(M3)、限流熔断(M3)、OTel 全链路(M3)、压测优化(M4)。

**前置状态(已就绪,无需重新做):**
- `go.mod` 已声明 module `github.com/mjyang04/flash-deal`,依赖完整(gin / go-redis / kafka-go / sqlx / viper / zap / gobreaker / otel / prometheus)
- `internal/domain/seckill.go` 已含 `Activity`、`Order`、`SeckillRequest`、`SeckillResult`、枚举常量
- `pkg/shardkey/shardkey.go` + 测试已就绪
- `internal/infra/redis/scripts/stock_deduct.lua` 已就绪(M2 才会调用)
- `migrations/001_init.{up,down}.sql` 已就绪(单库 `activities` + 单分片 `orders_0`)
- `deploy/docker-compose.yml` 完整 6 个服务,`Makefile` 含 `up/down/migrate/api/test/bench`
- `bench/k6/seckill.js` 已是 ramping-arrival-rate 骨架,本 plan Task 15 会调到 M1 baseline

---

## File Map

### 已存在 — 本 plan 会修改

| 路径 | 用途 | 改动类型 |
|------|------|---------|
| `cmd/api/main.go` | API 入口 | 接线 config / logger / mysql / redis / handler / middleware |
| `Makefile` | 任务入口 | 新增 `seed` 实现、`fmt` 别名、`test-integration` 目标 |
| `migrations/001_init.up.sql` | DDL | 不动(M3 才扩 orders_1..3) |
| `bench/k6/seckill.js` | 压测脚本 | 调到 M1 baseline(scenario 改为 constant-arrival-rate × 30s) |
| `internal/domain/seckill.go` | 领域类型 | 加 `SeckillOutcome` 枚举 + 错误 sentinel |

### 新建 — 本 plan 创建

| 路径 | 单一职责 | 行数预算 |
|------|---------|---------|
| `internal/config/config.go` | viper 装载 + 强类型 `Config` struct | < 120 |
| `internal/config/config_test.go` | 装载与缺省值验证 | < 80 |
| `internal/infra/logger/logger.go` | zap 单例工厂 | < 50 |
| `internal/infra/mysql/conn.go` | sqlx `*sqlx.DB` 工厂 + 池参数 | < 80 |
| `internal/infra/mysql/conn_test.go` | 集成测试:连接 + ping | < 50 |
| `internal/infra/redis/client.go` | `*redis.Client` 工厂 | < 60 |
| `internal/infra/redis/client_test.go` | 集成测试:连接 + ping | < 50 |
| `internal/repo/activity_repo.go` | `ActivityRepo` 接口 + sqlx 实现 | < 150 |
| `internal/repo/activity_repo_test.go` | 集成:Create / GetByID / 状态过滤 | < 200 |
| `internal/repo/order_repo.go` | `OrderRepo` 接口 + sqlx 实现(M1 只写 shard 0) | < 150 |
| `internal/repo/order_repo_test.go` | 集成:Create(幂等冲突)/GetByID | < 200 |
| `internal/repo/stock_repo.go` | `StockRepo` SQL 版:`DeductInTx` 用行锁 UPDATE | < 120 |
| `internal/repo/stock_repo_test.go` | 集成:并发 1000G × 100 stock 零超卖 | < 200 |
| `internal/service/seckill.go` | `SeckillService` 编排:校验→扣减→落单→返回 | < 200 |
| `internal/service/seckill_test.go` | 单元:用 mock repo 覆盖 6 类 outcome | < 300 |
| `internal/middleware/requestid.go` | `X-Request-Id` 注入 + 透传 | < 50 |
| `internal/middleware/recovery.go` | panic 捕获 + zap 记录 | < 50 |
| `internal/handler/seckill.go` | `POST /v1/seckill` 入口 + 错误码映射 | < 120 |
| `internal/handler/seckill_test.go` | httptest:6 个 outcome → 对应 HTTP 码 | < 250 |
| `internal/handler/admin.go` | `POST /admin/activities` + `POST /admin/activities/:id/warm` | < 150 |
| `internal/handler/admin_test.go` | httptest:创建 + 预热 | < 150 |
| `cmd/seed/main.go` | CLI:插入 demo 活动 + 预热 Redis | < 100 |
| `reports/week1_mvp.md` | M1 baseline 报告 | 模板 |

### 不在 M1 范围(留 M2+)

`cmd/consumer/*`、`internal/infra/kafka/*`、`internal/infra/otel/*`、`pkg/ratelimit/*` —— Task 12 main.go 接线时只引用 M1 必需模块,不引这些。

---

## 测试环境约定

- **单元测试**:`go test ./...` 默认跑;不依赖外部进程,用 mock repo / fake clock
- **集成测试**:文件加 build tag `//go:build integration`,通过 `make test-integration` 触发,前置要求 `make up` 已启动 docker-compose
- **集成测试连接参数**:从环境变量读
  - `FD_TEST_MYSQL_DSN`:默认 `flashdeal:flashdeal@tcp(127.0.0.1:3307)/flashdeal?parseTime=true&loc=Local`
  - `FD_TEST_REDIS_ADDR`:默认 `127.0.0.1:6380`
- **测试隔离**:每个集成测试在自己的 schema 下跑(`flashdeal_test_<random>`),或 `TRUNCATE` 收尾 —— 本 plan 采用后者(M1 阶段单库,够用)
- **clock**:`service` 层接 `clock func() time.Time` 注入,默认 `time.Now`,测试传 fake

---

## 错误码映射(Task 9 / Task 11 共同遵守)

`SeckillOutcome` 与 HTTP 状态码 / `error.code` 的固定映射:

| Outcome | HTTP | error.code |
|---------|------|-----------|
| `OutcomeQueued` | 202 | — |
| `OutcomeSoldOut` | 410 | `STOCK_NOT_ENOUGH` |
| `OutcomeUserLimit` | 409 | `USER_LIMIT_EXCEEDED` |
| `OutcomeDuplicate` | 409 | `IDEMPOTENT_REPLAY` |
| `OutcomeNotStarted` | 403 | `ACTIVITY_NOT_STARTED` |
| `OutcomeEnded` | 410 | `ACTIVITY_ENDED` |
| `OutcomeNotFound` | 404 | `NOT_FOUND` |
| `OutcomeInternal` | 500 | `INTERNAL` |

---

## Task 0:基础设施冒烟

**Files:**
- 触碰:`deploy/docker-compose.yml`(只读 / 启动)
- 触碰:`migrations/001_init.up.sql`(应用)

- [ ] **Step 0.1:启动全栈**

```bash
make up
```

Expected:`fd-mysql / fd-redis / fd-kafka / fd-jaeger / fd-prometheus / fd-grafana` 6 个容器 Up;最多等 30s 让 MySQL healthcheck 通过。

- [ ] **Step 0.2:验证 MySQL 可连**

```bash
docker exec -i fd-mysql mysql -uflashdeal -pflashdeal -e 'select 1' flashdeal
```

Expected:`1\n1`

- [ ] **Step 0.3:验证 Redis 可连**

```bash
docker exec -i fd-redis redis-cli ping
```

Expected:`PONG`

- [ ] **Step 0.4:应用 migration**

```bash
make migrate
docker exec -i fd-mysql mysql -uflashdeal -pflashdeal -e 'show tables' flashdeal
```

Expected:`activities` + `orders_0` 出现

- [ ] **Step 0.5:Commit(不改文件的话跳过)**

本 Task 不产生文件改动;若 healthcheck 调整请单独提交。

---

## Task 1:Config 装载

**Files:**
- Create: `internal/config/config.go`
- Create: `internal/config/config_test.go`

- [ ] **Step 1.1:写失败的测试**

`internal/config/config_test.go`:

```go
package config_test

import (
	"os"
	"testing"

	"github.com/mjyang04/flash-deal/internal/config"
)

func TestLoad_Defaults(t *testing.T) {
	// 清掉可能干扰的 env
	os.Unsetenv("FD_HTTP_ADDR")
	os.Unsetenv("FD_MYSQL_DSN")
	os.Unsetenv("FD_REDIS_ADDR")

	cfg, err := config.Load("")
	if err != nil {
		t.Fatalf("Load default: %v", err)
	}
	if cfg.HTTP.Addr != ":8080" {
		t.Errorf("HTTP.Addr = %q, want :8080", cfg.HTTP.Addr)
	}
	if cfg.MySQL.MaxOpenConns != 100 {
		t.Errorf("MySQL.MaxOpenConns = %d, want 100", cfg.MySQL.MaxOpenConns)
	}
	if cfg.Redis.Addr != "127.0.0.1:6380" {
		t.Errorf("Redis.Addr = %q, want 127.0.0.1:6380", cfg.Redis.Addr)
	}
}

func TestLoad_EnvOverride(t *testing.T) {
	t.Setenv("FD_HTTP_ADDR", ":9090")
	t.Setenv("FD_MYSQL_DSN", "user:pw@tcp(db:3306)/x")

	cfg, err := config.Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.HTTP.Addr != ":9090" {
		t.Errorf("HTTP.Addr = %q, want :9090", cfg.HTTP.Addr)
	}
	if cfg.MySQL.DSN != "user:pw@tcp(db:3306)/x" {
		t.Errorf("DSN override missed: %q", cfg.MySQL.DSN)
	}
}
```

- [ ] **Step 1.2:跑测试确认 FAIL**

```bash
go test ./internal/config/...
```

Expected:`no Go files` 或 `undefined: config.Load`

- [ ] **Step 1.3:实现 `internal/config/config.go`**

```go
// Package config loads strongly-typed runtime config from env / file via viper.
// All env vars use prefix FD_ and underscore-separated paths, e.g. FD_HTTP_ADDR.
package config

import (
	"strings"
	"time"

	"github.com/spf13/viper"
)

type HTTPConfig struct {
	Addr            string        `mapstructure:"addr"`
	ReadHeaderWait  time.Duration `mapstructure:"read_header_wait"`
	ShutdownTimeout time.Duration `mapstructure:"shutdown_timeout"`
}

type MySQLConfig struct {
	DSN             string        `mapstructure:"dsn"`
	MaxOpenConns    int           `mapstructure:"max_open_conns"`
	MaxIdleConns    int           `mapstructure:"max_idle_conns"`
	ConnMaxLifetime time.Duration `mapstructure:"conn_max_lifetime"`
}

type RedisConfig struct {
	Addr     string `mapstructure:"addr"`
	Password string `mapstructure:"password"`
	DB       int    `mapstructure:"db"`
	PoolSize int    `mapstructure:"pool_size"`
}

type Config struct {
	HTTP  HTTPConfig  `mapstructure:"http"`
	MySQL MySQLConfig `mapstructure:"mysql"`
	Redis RedisConfig `mapstructure:"redis"`
}

// Load reads config from optional YAML at `path` and overlays env vars.
// path == "" → skip file read.
func Load(path string) (*Config, error) {
	v := viper.New()
	v.SetEnvPrefix("FD")
	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// Defaults
	v.SetDefault("http.addr", ":8080")
	v.SetDefault("http.read_header_wait", 5*time.Second)
	v.SetDefault("http.shutdown_timeout", 10*time.Second)

	v.SetDefault("mysql.dsn", "flashdeal:flashdeal@tcp(127.0.0.1:3307)/flashdeal?parseTime=true&loc=Local")
	v.SetDefault("mysql.max_open_conns", 100)
	v.SetDefault("mysql.max_idle_conns", 50)
	v.SetDefault("mysql.conn_max_lifetime", 30*time.Minute)

	v.SetDefault("redis.addr", "127.0.0.1:6380")
	v.SetDefault("redis.db", 0)
	v.SetDefault("redis.pool_size", 200)

	if path != "" {
		v.SetConfigFile(path)
		if err := v.ReadInConfig(); err != nil {
			return nil, err
		}
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}
```

- [ ] **Step 1.4:跑测试确认 PASS**

```bash
go test ./internal/config/... -v
```

Expected:`PASS` 两个测试

- [ ] **Step 1.5:Commit**

```bash
git add internal/config/
git commit -m "feat(config): viper-based config loader with env override"
```

---

## Task 2:Zap Logger

**Files:**
- Create: `internal/infra/logger/logger.go`

- [ ] **Step 2.1:写实现(无测试 —— 薄包装)**

```go
// Package logger provides a process-wide zap logger configured for prod or dev.
package logger

import (
	"sync"

	"go.uber.org/zap"
)

var (
	once sync.Once
	log  *zap.Logger
)

// Init configures the singleton. mode = "dev" enables console+colors,
// any other value uses production JSON encoding.
func Init(mode string) (*zap.Logger, error) {
	var err error
	once.Do(func() {
		if mode == "dev" {
			log, err = zap.NewDevelopment()
		} else {
			log, err = zap.NewProduction()
		}
	})
	return log, err
}

// L returns the initialized logger. Call Init first; otherwise this returns a no-op.
func L() *zap.Logger {
	if log == nil {
		return zap.NewNop()
	}
	return log
}
```

- [ ] **Step 2.2:编译确认**

```bash
go build ./internal/infra/logger/...
```

Expected:无输出(成功)

- [ ] **Step 2.3:Commit**

```bash
git add internal/infra/logger/
git commit -m "feat(logger): zap singleton with dev/prod modes"
```

---

## Task 3:MySQL 连接池

**Files:**
- Create: `internal/infra/mysql/conn.go`
- Create: `internal/infra/mysql/conn_test.go`

- [ ] **Step 3.1:写失败的集成测试**

`internal/infra/mysql/conn_test.go`:

```go
//go:build integration

package mysql_test

import (
	"context"
	"os"
	"testing"
	"time"

	fdmysql "github.com/mjyang04/flash-deal/internal/infra/mysql"
	"github.com/mjyang04/flash-deal/internal/config"
)

func TestOpen_Ping(t *testing.T) {
	dsn := os.Getenv("FD_TEST_MYSQL_DSN")
	if dsn == "" {
		dsn = "flashdeal:flashdeal@tcp(127.0.0.1:3307)/flashdeal?parseTime=true&loc=Local"
	}
	cfg := config.MySQLConfig{
		DSN:             dsn,
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Minute,
	}
	db, err := fdmysql.Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}
```

- [ ] **Step 3.2:跑测试确认 FAIL**

```bash
go test -tags=integration ./internal/infra/mysql/...
```

Expected:`undefined: fdmysql.Open`

- [ ] **Step 3.3:实现 `internal/infra/mysql/conn.go`**

```go
// Package mysql wraps sqlx.DB with pool tuning + sensible defaults.
package mysql

import (
	"github.com/jmoiron/sqlx"
	_ "github.com/go-sql-driver/mysql" // driver

	"github.com/mjyang04/flash-deal/internal/config"
)

// Open returns a tuned *sqlx.DB. Caller closes.
func Open(cfg config.MySQLConfig) (*sqlx.DB, error) {
	db, err := sqlx.Open("mysql", cfg.DSN)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	return db, nil
}
```

- [ ] **Step 3.4:跑测试确认 PASS(需要 docker-compose 已起)**

```bash
make up
go test -tags=integration -v ./internal/infra/mysql/...
```

Expected:`PASS TestOpen_Ping`

- [ ] **Step 3.5:Commit**

```bash
git add internal/infra/mysql/
git commit -m "feat(mysql): sqlx connection pool with config-driven tuning"
```

---

## Task 4:Redis 客户端

**Files:**
- Create: `internal/infra/redis/client.go`
- Create: `internal/infra/redis/client_test.go`

- [ ] **Step 4.1:写失败的集成测试**

`internal/infra/redis/client_test.go`:

```go
//go:build integration

package redis_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/mjyang04/flash-deal/internal/config"
	fdredis "github.com/mjyang04/flash-deal/internal/infra/redis"
)

func TestNew_Ping(t *testing.T) {
	addr := os.Getenv("FD_TEST_REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6380"
	}
	cfg := config.RedisConfig{Addr: addr, DB: 0, PoolSize: 10}
	cli := fdredis.New(cfg)
	defer cli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := cli.Ping(ctx).Err(); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}
```

- [ ] **Step 4.2:跑测试确认 FAIL**

```bash
go test -tags=integration ./internal/infra/redis/...
```

Expected:`undefined: fdredis.New`

- [ ] **Step 4.3:实现 `internal/infra/redis/client.go`**

```go
// Package redis wraps go-redis v9 with our config types.
package redis

import (
	goredis "github.com/redis/go-redis/v9"

	"github.com/mjyang04/flash-deal/internal/config"
)

// New returns a configured *goredis.Client. Caller closes.
func New(cfg config.RedisConfig) *goredis.Client {
	return goredis.NewClient(&goredis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
		PoolSize: cfg.PoolSize,
	})
}
```

- [ ] **Step 4.4:跑测试确认 PASS**

```bash
go test -tags=integration -v ./internal/infra/redis/...
```

Expected:`PASS TestNew_Ping`

- [ ] **Step 4.5:Commit**

```bash
git add internal/infra/redis/client.go internal/infra/redis/client_test.go
git commit -m "feat(redis): go-redis v9 client factory"
```

---

## Task 5:Domain 扩充(Outcome + Sentinels)

**Files:**
- Modify: `internal/domain/seckill.go`

- [ ] **Step 5.1:在 `internal/domain/seckill.go` 末尾追加**

```go
// SeckillOutcome enumerates terminal states of a single seckill attempt.
type SeckillOutcome int

const (
	OutcomeQueued SeckillOutcome = iota
	OutcomeSoldOut
	OutcomeUserLimit
	OutcomeDuplicate
	OutcomeNotStarted
	OutcomeEnded
	OutcomeNotFound
	OutcomeInternal
)

// String returns the canonical wire-name used in the JSON response status field.
func (o SeckillOutcome) String() string {
	switch o {
	case OutcomeQueued:
		return "queued"
	case OutcomeSoldOut:
		return "sold_out"
	case OutcomeUserLimit:
		return "user_limit"
	case OutcomeDuplicate:
		return "duplicate"
	case OutcomeNotStarted:
		return "not_started"
	case OutcomeEnded:
		return "ended"
	case OutcomeNotFound:
		return "not_found"
	default:
		return "internal"
	}
}
```

- [ ] **Step 5.2:加单元测试 `internal/domain/seckill_test.go`**

```go
package domain_test

import (
	"testing"

	"github.com/mjyang04/flash-deal/internal/domain"
)

func TestSeckillOutcome_String(t *testing.T) {
	cases := []struct {
		o    domain.SeckillOutcome
		want string
	}{
		{domain.OutcomeQueued, "queued"},
		{domain.OutcomeSoldOut, "sold_out"},
		{domain.OutcomeUserLimit, "user_limit"},
		{domain.OutcomeDuplicate, "duplicate"},
		{domain.OutcomeNotStarted, "not_started"},
		{domain.OutcomeEnded, "ended"},
		{domain.OutcomeNotFound, "not_found"},
		{domain.OutcomeInternal, "internal"},
	}
	for _, c := range cases {
		if got := c.o.String(); got != c.want {
			t.Errorf("%d.String() = %q, want %q", c.o, got, c.want)
		}
	}
}
```

- [ ] **Step 5.3:跑测试确认 PASS**

```bash
go test ./internal/domain/... -v
```

Expected:`PASS TestSeckillOutcome_String`

- [ ] **Step 5.4:Commit**

```bash
git add internal/domain/
git commit -m "feat(domain): SeckillOutcome enum with wire strings"
```

---

## Task 6:ActivityRepo

**Files:**
- Create: `internal/repo/activity_repo.go`
- Create: `internal/repo/activity_repo_test.go`

- [ ] **Step 6.1:写失败的集成测试**

`internal/repo/activity_repo_test.go`:

```go
//go:build integration

package repo_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/go-sql-driver/mysql"

	"github.com/mjyang04/flash-deal/internal/domain"
	"github.com/mjyang04/flash-deal/internal/repo"
)

func openTestDB(t *testing.T) *sqlx.DB {
	t.Helper()
	dsn := os.Getenv("FD_TEST_MYSQL_DSN")
	if dsn == "" {
		dsn = "flashdeal:flashdeal@tcp(127.0.0.1:3307)/flashdeal?parseTime=true&loc=Local"
	}
	db, err := sqlx.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func resetActivities(t *testing.T, db *sqlx.DB) {
	t.Helper()
	if _, err := db.Exec("TRUNCATE TABLE activities"); err != nil {
		t.Fatal(err)
	}
}

func TestActivityRepo_CreateAndGet(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	resetActivities(t, db)

	r := repo.NewActivityRepo(db)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	a := domain.Activity{
		ID: 9001, ProductID: 555, TotalStock: 100,
		StartAt: now.Add(-time.Minute), EndAt: now.Add(time.Hour),
		PerUserLimit: 1, Status: domain.ActivityRunning,
	}
	if err := r.Create(ctx, a); err != nil {
		t.Fatalf("Create: %v", err)
	}
	got, err := r.GetByID(ctx, 9001)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got.ProductID != 555 || got.TotalStock != 100 || got.PerUserLimit != 1 {
		t.Errorf("round-trip mismatch: %+v", got)
	}
}

func TestActivityRepo_GetByID_NotFound(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	resetActivities(t, db)

	r := repo.NewActivityRepo(db)
	_, err := r.GetByID(context.Background(), 404404)
	if err != repo.ErrActivityNotFound {
		t.Fatalf("got %v, want ErrActivityNotFound", err)
	}
}
```

- [ ] **Step 6.2:跑测试确认 FAIL**

```bash
go test -tags=integration ./internal/repo/...
```

Expected:`undefined: repo.NewActivityRepo`

- [ ] **Step 6.3:实现 `internal/repo/activity_repo.go`**

```go
// Package repo holds DB-backed implementations of the domain repository ports.
package repo

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/mjyang04/flash-deal/internal/domain"
)

// ErrActivityNotFound is returned by ActivityRepo.GetByID when no row matches.
var ErrActivityNotFound = errors.New("activity not found")

// ActivityRepo is the persistence port for activities.
type ActivityRepo interface {
	Create(ctx context.Context, a domain.Activity) error
	GetByID(ctx context.Context, id int64) (domain.Activity, error)
}

type activityRepoSQL struct {
	db *sqlx.DB
}

// NewActivityRepo wires the sqlx-backed implementation.
func NewActivityRepo(db *sqlx.DB) ActivityRepo {
	return &activityRepoSQL{db: db}
}

type activityRow struct {
	ID           int64     `db:"id"`
	ProductID    int64     `db:"product_id"`
	TotalStock   int       `db:"total_stock"`
	StartAt      time.Time `db:"start_at"`
	EndAt        time.Time `db:"end_at"`
	PerUserLimit int       `db:"per_user_limit"`
	Status       int8      `db:"status"`
}

func (r *activityRepoSQL) Create(ctx context.Context, a domain.Activity) error {
	const q = `
INSERT INTO activities (id, product_id, total_stock, start_at, end_at, per_user_limit, status)
VALUES (?, ?, ?, ?, ?, ?, ?)`
	_, err := r.db.ExecContext(ctx, q,
		a.ID, a.ProductID, a.TotalStock,
		a.StartAt, a.EndAt, a.PerUserLimit, int8(a.Status),
	)
	return err
}

func (r *activityRepoSQL) GetByID(ctx context.Context, id int64) (domain.Activity, error) {
	var row activityRow
	const q = `
SELECT id, product_id, total_stock, start_at, end_at, per_user_limit, status
  FROM activities WHERE id = ?`
	err := r.db.GetContext(ctx, &row, q, id)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Activity{}, ErrActivityNotFound
	}
	if err != nil {
		return domain.Activity{}, err
	}
	return domain.Activity{
		ID:           row.ID,
		ProductID:    row.ProductID,
		TotalStock:   row.TotalStock,
		StartAt:      row.StartAt,
		EndAt:        row.EndAt,
		PerUserLimit: row.PerUserLimit,
		Status:       domain.ActivityStatus(row.Status),
	}, nil
}
```

- [ ] **Step 6.4:跑测试确认 PASS**

```bash
go test -tags=integration -v ./internal/repo/...
```

Expected:`PASS TestActivityRepo_CreateAndGet`、`PASS TestActivityRepo_GetByID_NotFound`

- [ ] **Step 6.5:Commit**

```bash
git add internal/repo/activity_repo.go internal/repo/activity_repo_test.go
git commit -m "feat(repo): ActivityRepo with sqlx + NotFound sentinel"
```

---

## Task 7:OrderRepo(M1 单分片)

**Files:**
- Create: `internal/repo/order_repo.go`
- Create: `internal/repo/order_repo_test.go`

- [ ] **Step 7.1:写失败的集成测试**

`internal/repo/order_repo_test.go`:

```go
//go:build integration

package repo_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/mjyang04/flash-deal/internal/domain"
	"github.com/mjyang04/flash-deal/internal/repo"
)

func resetOrders(t *testing.T, db *sqlx.DB) {
	t.Helper()
	if _, err := db.Exec("TRUNCATE TABLE orders_0"); err != nil {
		t.Fatal(err)
	}
}

func TestOrderRepo_Create_Then_GetByID(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	resetOrders(t, db)

	r := repo.NewOrderRepo(db)
	ctx := context.Background()
	o := domain.Order{
		ID: 7001, UserID: 42, ActivityID: 9001, ProductID: 555,
		Status: domain.OrderQueued, IdempotencyToken: "tok-1", CreatedAt: time.Now(),
	}
	if err := r.Create(ctx, o); err != nil {
		t.Fatalf("Create: %v", err)
	}
	got, err := r.GetByID(ctx, 42, 7001)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got.IdempotencyToken != "tok-1" {
		t.Errorf("token round-trip mismatch")
	}
}

func TestOrderRepo_Create_DuplicateIdem(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	resetOrders(t, db)

	r := repo.NewOrderRepo(db)
	ctx := context.Background()
	o := domain.Order{
		ID: 7002, UserID: 42, ActivityID: 9001, ProductID: 555,
		Status: domain.OrderQueued, IdempotencyToken: "tok-dup", CreatedAt: time.Now(),
	}
	if err := r.Create(ctx, o); err != nil {
		t.Fatalf("first Create: %v", err)
	}
	o2 := o
	o2.ID = 7003 // distinct PK; same (activity_id, user_id, idem) tuple
	err := r.Create(ctx, o2)
	if !errors.Is(err, repo.ErrOrderDuplicate) {
		t.Fatalf("got %v, want ErrOrderDuplicate", err)
	}
}
```

- [ ] **Step 7.2:跑测试确认 FAIL**

```bash
go test -tags=integration ./internal/repo/...
```

Expected:`undefined: repo.NewOrderRepo`

- [ ] **Step 7.3:实现 `internal/repo/order_repo.go`**

```go
package repo

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/mjyang04/flash-deal/internal/domain"
)

// ErrOrderNotFound — no order for the given (user_id, order_id).
var ErrOrderNotFound = errors.New("order not found")

// ErrOrderDuplicate — UNIQUE (activity_id, user_id, idempotency_token) hit.
var ErrOrderDuplicate = errors.New("duplicate order")

// OrderRepo is the persistence port for orders. M1 routes everything to shard 0;
// M3 will switch the impl to per-shard *sqlx.DB selected via pkg/shardkey.
type OrderRepo interface {
	Create(ctx context.Context, o domain.Order) error
	GetByID(ctx context.Context, userID, orderID int64) (domain.Order, error)
}

type orderRepoSQL struct {
	db *sqlx.DB
}

func NewOrderRepo(db *sqlx.DB) OrderRepo {
	return &orderRepoSQL{db: db}
}

type orderRow struct {
	ID               int64     `db:"id"`
	UserID           int64     `db:"user_id"`
	ActivityID       int64     `db:"activity_id"`
	ProductID        int64     `db:"product_id"`
	Status           int8      `db:"status"`
	IdempotencyToken string    `db:"idempotency_token"`
	CreatedAt        time.Time `db:"created_at"`
}

func (r *orderRepoSQL) Create(ctx context.Context, o domain.Order) error {
	const q = `
INSERT INTO orders_0 (id, user_id, activity_id, product_id, status, idempotency_token, created_at)
VALUES (?, ?, ?, ?, ?, ?, ?)`
	_, err := r.db.ExecContext(ctx, q,
		o.ID, o.UserID, o.ActivityID, o.ProductID,
		int8(o.Status), o.IdempotencyToken, o.CreatedAt,
	)
	if err != nil && isDuplicateKey(err) {
		return ErrOrderDuplicate
	}
	return err
}

func (r *orderRepoSQL) GetByID(ctx context.Context, userID, orderID int64) (domain.Order, error) {
	var row orderRow
	const q = `
SELECT id, user_id, activity_id, product_id, status, idempotency_token, created_at
  FROM orders_0 WHERE user_id = ? AND id = ?`
	err := r.db.GetContext(ctx, &row, q, userID, orderID)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Order{}, ErrOrderNotFound
	}
	if err != nil {
		return domain.Order{}, err
	}
	return domain.Order{
		ID:               row.ID,
		UserID:           row.UserID,
		ActivityID:       row.ActivityID,
		ProductID:        row.ProductID,
		Status:           domain.OrderStatus(row.Status),
		IdempotencyToken: row.IdempotencyToken,
		CreatedAt:        row.CreatedAt,
	}, nil
}

// isDuplicateKey detects MySQL error 1062 without importing the driver type.
func isDuplicateKey(err error) bool {
	return strings.Contains(err.Error(), "Error 1062") ||
		strings.Contains(err.Error(), "Duplicate entry")
}
```

- [ ] **Step 7.4:跑测试确认 PASS**

```bash
go test -tags=integration -v ./internal/repo/...
```

Expected:全部 PASS

- [ ] **Step 7.5:Commit**

```bash
git add internal/repo/order_repo.go internal/repo/order_repo_test.go
git commit -m "feat(repo): OrderRepo single-shard with duplicate-key detection"
```

---

## Task 8:StockRepo(M1 SQL 行锁版)

**Files:**
- Create: `internal/repo/stock_repo.go`
- Create: `internal/repo/stock_repo_test.go`

> 说明:M1 用 `UPDATE activities SET total_stock = total_stock - 1 WHERE id = ? AND total_stock >= 1` 行锁保证不超卖。M2 将切到 Redis Lua 路径,接口签名不变(`Deduct(ctx, activityID, userID, n) (remaining int, err error)`)。

- [ ] **Step 8.1:写失败的"并发零超卖"集成测试**

`internal/repo/stock_repo_test.go`:

```go
//go:build integration

package repo_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mjyang04/flash-deal/internal/domain"
	"github.com/mjyang04/flash-deal/internal/repo"
)

func TestStockRepo_Deduct_NoOversell(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	resetActivities(t, db)

	ar := repo.NewActivityRepo(db)
	now := time.Now().UTC()
	if err := ar.Create(context.Background(), domain.Activity{
		ID: 8001, ProductID: 1, TotalStock: 100,
		StartAt: now.Add(-time.Minute), EndAt: now.Add(time.Hour),
		PerUserLimit: 1, Status: domain.ActivityRunning,
	}); err != nil {
		t.Fatal(err)
	}

	sr := repo.NewStockRepo(db)
	var success, soldOut int32
	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := sr.Deduct(context.Background(), 8001, 1)
			switch err {
			case nil:
				atomic.AddInt32(&success, 1)
			case repo.ErrStockNotEnough:
				atomic.AddInt32(&soldOut, 1)
			}
		}()
	}
	wg.Wait()

	if success != 100 {
		t.Errorf("success = %d, want exactly 100", success)
	}
	if soldOut != 900 {
		t.Errorf("soldOut = %d, want exactly 900", soldOut)
	}

	// verify DB final state is 0
	got, _ := ar.GetByID(context.Background(), 8001)
	if got.TotalStock != 0 {
		t.Errorf("final stock = %d, want 0", got.TotalStock)
	}
}
```

- [ ] **Step 8.2:跑测试确认 FAIL**

```bash
go test -tags=integration ./internal/repo/...
```

Expected:`undefined: repo.NewStockRepo`

- [ ] **Step 8.3:实现 `internal/repo/stock_repo.go`**

```go
package repo

import (
	"context"
	"errors"

	"github.com/jmoiron/sqlx"
)

// ErrStockNotEnough — UPDATE matched 0 rows, stock would go negative.
var ErrStockNotEnough = errors.New("stock not enough")

// StockRepo is the seckill stock port. M1 = SQL row lock; M2 = Redis Lua.
type StockRepo interface {
	// Deduct atomically subtracts n from activity's total_stock when remaining >= n.
	// Returns the post-deduction remaining stock, or ErrStockNotEnough.
	Deduct(ctx context.Context, activityID int64, n int) (remaining int, err error)
}

type stockRepoSQL struct {
	db *sqlx.DB
}

func NewStockRepo(db *sqlx.DB) StockRepo {
	return &stockRepoSQL{db: db}
}

func (r *stockRepoSQL) Deduct(ctx context.Context, activityID int64, n int) (int, error) {
	const upd = `
UPDATE activities
   SET total_stock = total_stock - ?
 WHERE id = ? AND total_stock >= ?`
	res, err := r.db.ExecContext(ctx, upd, n, activityID, n)
	if err != nil {
		return 0, err
	}
	aff, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	if aff == 0 {
		return 0, ErrStockNotEnough
	}
	var remaining int
	if err := r.db.GetContext(ctx, &remaining,
		"SELECT total_stock FROM activities WHERE id = ?", activityID); err != nil {
		return 0, err
	}
	return remaining, nil
}
```

- [ ] **Step 8.4:跑测试确认 PASS**

```bash
go test -tags=integration -v -run TestStockRepo ./internal/repo/...
```

Expected:`PASS TestStockRepo_Deduct_NoOversell`(可能耗时数秒)

- [ ] **Step 8.5:Commit**

```bash
git add internal/repo/stock_repo.go internal/repo/stock_repo_test.go
git commit -m "feat(repo): StockRepo SQL row-lock deduct with no-oversell concurrency test"
```

---

## Task 9:SeckillService v0(编排)

**Files:**
- Create: `internal/service/seckill.go`
- Create: `internal/service/seckill_test.go`

> 设计:Service 只依赖三个 Repo 接口 + clock + ID generator。M1 不引 Kafka,落单走 `OrderRepo.Create` 同步路径;返回 `OutcomeQueued` + `QueueToken`(M1 阶段 `QueueToken = strconv.FormatInt(order.ID, 10)`,M2 会改成独立 UUIDv7)。

- [ ] **Step 9.1:定义 mock(嵌入测试文件即可)+ 写失败的测试**

`internal/service/seckill_test.go`:

```go
package service_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mjyang04/flash-deal/internal/domain"
	"github.com/mjyang04/flash-deal/internal/repo"
	"github.com/mjyang04/flash-deal/internal/service"
)

// ---- in-memory mocks ----

type fakeActivityRepo struct {
	store map[int64]domain.Activity
}

func (f *fakeActivityRepo) Create(_ context.Context, a domain.Activity) error {
	f.store[a.ID] = a
	return nil
}
func (f *fakeActivityRepo) GetByID(_ context.Context, id int64) (domain.Activity, error) {
	a, ok := f.store[id]
	if !ok {
		return domain.Activity{}, repo.ErrActivityNotFound
	}
	return a, nil
}

type fakeStockRepo struct {
	remaining int
	err       error
}

func (f *fakeStockRepo) Deduct(_ context.Context, _ int64, n int) (int, error) {
	if f.err != nil {
		return 0, f.err
	}
	f.remaining -= n
	return f.remaining, nil
}

type fakeOrderRepo struct {
	created []domain.Order
	err     error
}

func (f *fakeOrderRepo) Create(_ context.Context, o domain.Order) error {
	if f.err != nil {
		return f.err
	}
	f.created = append(f.created, o)
	return nil
}
func (f *fakeOrderRepo) GetByID(_ context.Context, userID, orderID int64) (domain.Order, error) {
	for _, o := range f.created {
		if o.UserID == userID && o.ID == orderID {
			return o, nil
		}
	}
	return domain.Order{}, repo.ErrOrderNotFound
}

// ---- helpers ----

func runningActivity() domain.Activity {
	now := time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC)
	return domain.Activity{
		ID: 9001, ProductID: 555, TotalStock: 10,
		StartAt: now.Add(-time.Minute), EndAt: now.Add(time.Hour),
		PerUserLimit: 1, Status: domain.ActivityRunning,
	}
}

func newSvc(ar service.ActivityFetcher, sr service.StockDeducter, or service.OrderCreator) *service.SeckillService {
	return service.New(ar, sr, or,
		func() time.Time { return time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC) },
		func() int64 { return 7777 },
	)
}

// ---- tests ----

func TestSeckill_Queued(t *testing.T) {
	ar := &fakeActivityRepo{store: map[int64]domain.Activity{9001: runningActivity()}}
	sr := &fakeStockRepo{remaining: 10}
	or := &fakeOrderRepo{}
	svc := newSvc(ar, sr, or)

	res, err := svc.Seckill(context.Background(), domain.SeckillRequest{
		ActivityID: 9001, UserID: 42, IdempotencyToken: "tok-1",
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if res.Outcome != domain.OutcomeQueued {
		t.Errorf("outcome = %v, want Queued", res.Outcome)
	}
	if res.Remaining != 9 {
		t.Errorf("remaining = %d, want 9", res.Remaining)
	}
	if len(or.created) != 1 {
		t.Errorf("created = %d, want 1", len(or.created))
	}
}

func TestSeckill_NotFound(t *testing.T) {
	svc := newSvc(&fakeActivityRepo{store: map[int64]domain.Activity{}}, &fakeStockRepo{}, &fakeOrderRepo{})
	res, _ := svc.Seckill(context.Background(), domain.SeckillRequest{ActivityID: 1, UserID: 1, IdempotencyToken: "x"})
	if res.Outcome != domain.OutcomeNotFound {
		t.Errorf("outcome = %v, want NotFound", res.Outcome)
	}
}

func TestSeckill_NotStarted(t *testing.T) {
	a := runningActivity()
	a.StartAt = time.Date(2026, 5, 25, 13, 0, 0, 0, time.UTC) // in the future
	svc := newSvc(&fakeActivityRepo{store: map[int64]domain.Activity{9001: a}}, &fakeStockRepo{remaining: 10}, &fakeOrderRepo{})
	res, _ := svc.Seckill(context.Background(), domain.SeckillRequest{ActivityID: 9001, UserID: 1, IdempotencyToken: "x"})
	if res.Outcome != domain.OutcomeNotStarted {
		t.Errorf("outcome = %v, want NotStarted", res.Outcome)
	}
}

func TestSeckill_Ended(t *testing.T) {
	a := runningActivity()
	a.EndAt = time.Date(2026, 5, 25, 11, 0, 0, 0, time.UTC)
	svc := newSvc(&fakeActivityRepo{store: map[int64]domain.Activity{9001: a}}, &fakeStockRepo{remaining: 10}, &fakeOrderRepo{})
	res, _ := svc.Seckill(context.Background(), domain.SeckillRequest{ActivityID: 9001, UserID: 1, IdempotencyToken: "x"})
	if res.Outcome != domain.OutcomeEnded {
		t.Errorf("outcome = %v, want Ended", res.Outcome)
	}
}

func TestSeckill_SoldOut(t *testing.T) {
	ar := &fakeActivityRepo{store: map[int64]domain.Activity{9001: runningActivity()}}
	sr := &fakeStockRepo{err: repo.ErrStockNotEnough}
	svc := newSvc(ar, sr, &fakeOrderRepo{})
	res, _ := svc.Seckill(context.Background(), domain.SeckillRequest{ActivityID: 9001, UserID: 1, IdempotencyToken: "x"})
	if res.Outcome != domain.OutcomeSoldOut {
		t.Errorf("outcome = %v, want SoldOut", res.Outcome)
	}
}

func TestSeckill_Duplicate(t *testing.T) {
	ar := &fakeActivityRepo{store: map[int64]domain.Activity{9001: runningActivity()}}
	sr := &fakeStockRepo{remaining: 10}
	or := &fakeOrderRepo{err: repo.ErrOrderDuplicate}
	svc := newSvc(ar, sr, or)
	res, _ := svc.Seckill(context.Background(), domain.SeckillRequest{ActivityID: 9001, UserID: 1, IdempotencyToken: "x"})
	if res.Outcome != domain.OutcomeDuplicate {
		t.Errorf("outcome = %v, want Duplicate", res.Outcome)
	}
}

func TestSeckill_InternalOnUnknown(t *testing.T) {
	ar := &fakeActivityRepo{store: map[int64]domain.Activity{9001: runningActivity()}}
	sr := &fakeStockRepo{err: errors.New("boom")}
	svc := newSvc(ar, sr, &fakeOrderRepo{})
	res, _ := svc.Seckill(context.Background(), domain.SeckillRequest{ActivityID: 9001, UserID: 1, IdempotencyToken: "x"})
	if res.Outcome != domain.OutcomeInternal {
		t.Errorf("outcome = %v, want Internal", res.Outcome)
	}
}
```

- [ ] **Step 9.2:跑测试确认 FAIL**

```bash
go test ./internal/service/...
```

Expected:`undefined: service.New / service.SeckillService / service.ActivityFetcher ...`

- [ ] **Step 9.3:实现 `internal/service/seckill.go`**

```go
// Package service holds the seckill application service.
// The service depends on small ports (not the concrete repo structs) so that
// M2 can swap StockDeducter to Redis-Lua and OrderCreator to a Kafka producer
// without changing this file.
package service

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/mjyang04/flash-deal/internal/domain"
	"github.com/mjyang04/flash-deal/internal/repo"
)

// ---- ports (subset of repo interfaces) ----

type ActivityFetcher interface {
	GetByID(ctx context.Context, id int64) (domain.Activity, error)
}

type StockDeducter interface {
	Deduct(ctx context.Context, activityID int64, n int) (remaining int, err error)
}

type OrderCreator interface {
	Create(ctx context.Context, o domain.Order) error
}

// ---- service ----

type SeckillService struct {
	activities ActivityFetcher
	stock      StockDeducter
	orders     OrderCreator
	now        func() time.Time
	nextID     func() int64
}

func New(
	activities ActivityFetcher, stock StockDeducter, orders OrderCreator,
	now func() time.Time, nextID func() int64,
) *SeckillService {
	return &SeckillService{
		activities: activities, stock: stock, orders: orders,
		now: now, nextID: nextID,
	}
}

// SeckillOutput is what Seckill hands back to the handler.
type SeckillOutput struct {
	Outcome    domain.SeckillOutcome
	QueueToken string
	Remaining  int
}

// Seckill is the M1 synchronous path.
// Order of checks: activity exists → window open → deduct stock → create order.
func (s *SeckillService) Seckill(ctx context.Context, req domain.SeckillRequest) (SeckillOutput, error) {
	a, err := s.activities.GetByID(ctx, req.ActivityID)
	if errors.Is(err, repo.ErrActivityNotFound) {
		return SeckillOutput{Outcome: domain.OutcomeNotFound}, nil
	}
	if err != nil {
		return SeckillOutput{Outcome: domain.OutcomeInternal}, err
	}

	now := s.now()
	switch {
	case now.Before(a.StartAt):
		return SeckillOutput{Outcome: domain.OutcomeNotStarted}, nil
	case !now.Before(a.EndAt):
		return SeckillOutput{Outcome: domain.OutcomeEnded}, nil
	}

	remaining, err := s.stock.Deduct(ctx, req.ActivityID, 1)
	if errors.Is(err, repo.ErrStockNotEnough) {
		return SeckillOutput{Outcome: domain.OutcomeSoldOut}, nil
	}
	if err != nil {
		return SeckillOutput{Outcome: domain.OutcomeInternal}, err
	}

	orderID := s.nextID()
	err = s.orders.Create(ctx, domain.Order{
		ID: orderID, UserID: req.UserID, ActivityID: req.ActivityID, ProductID: a.ProductID,
		Status: domain.OrderQueued, IdempotencyToken: req.IdempotencyToken, CreatedAt: now,
	})
	if errors.Is(err, repo.ErrOrderDuplicate) {
		return SeckillOutput{Outcome: domain.OutcomeDuplicate}, nil
	}
	if err != nil {
		// NOTE: stock was decremented but order failed; M3 will add a reconcile sweeper.
		return SeckillOutput{Outcome: domain.OutcomeInternal}, err
	}

	return SeckillOutput{
		Outcome:    domain.OutcomeQueued,
		QueueToken: strconv.FormatInt(orderID, 10),
		Remaining:  remaining,
	}, nil
}
```

- [ ] **Step 9.4:跑测试确认 PASS**

```bash
go test -v -race ./internal/service/...
```

Expected:6 个用例全部 PASS

- [ ] **Step 9.5:Commit**

```bash
git add internal/service/
git commit -m "feat(service): synchronous SeckillService v0 with outcome enum + mocked tests"
```

---

## Task 10:Middleware(requestid + recovery)

**Files:**
- Create: `internal/middleware/requestid.go`
- Create: `internal/middleware/recovery.go`

- [ ] **Step 10.1:实现 `internal/middleware/requestid.go`**

```go
// Package middleware holds Gin middleware used by cmd/api.
package middleware

import (
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

const RequestIDHeader = "X-Request-Id"
const requestIDKey = "fd.request_id"

// RequestID echoes (or generates) X-Request-Id and stashes it in gin.Context.
func RequestID() gin.HandlerFunc {
	return func(c *gin.Context) {
		rid := c.GetHeader(RequestIDHeader)
		if rid == "" {
			rid = uuid.NewString()
		}
		c.Set(requestIDKey, rid)
		c.Writer.Header().Set(RequestIDHeader, rid)
		c.Next()
	}
}

// RequestIDFrom returns the stashed id or empty string.
func RequestIDFrom(c *gin.Context) string {
	v, ok := c.Get(requestIDKey)
	if !ok {
		return ""
	}
	s, _ := v.(string)
	return s
}
```

- [ ] **Step 10.2:实现 `internal/middleware/recovery.go`**

```go
package middleware

import (
	"fmt"
	"net/http"
	"runtime/debug"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/mjyang04/flash-deal/internal/infra/logger"
)

// Recovery converts panics into 500 + structured log including X-Request-Id.
func Recovery() gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if r := recover(); r != nil {
				logger.L().Error("panic",
					zap.String("request_id", RequestIDFrom(c)),
					zap.String("path", c.FullPath()),
					zap.Any("panic", r),
					zap.String("stack", string(debug.Stack())),
				)
				c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
					"error": gin.H{
						"code":       "INTERNAL",
						"message":    fmt.Sprintf("internal: %v", r),
						"request_id": RequestIDFrom(c),
					},
				})
			}
		}()
		c.Next()
	}
}
```

- [ ] **Step 10.3:编译确认**

```bash
go build ./internal/middleware/...
```

Expected:无输出

- [ ] **Step 10.4:Commit**

```bash
git add internal/middleware/
git commit -m "feat(middleware): X-Request-Id + zap recovery"
```

---

## Task 11:Handler(seckill + admin)

**Files:**
- Create: `internal/handler/seckill.go`
- Create: `internal/handler/seckill_test.go`
- Create: `internal/handler/admin.go`
- Create: `internal/handler/admin_test.go`

- [ ] **Step 11.1:写失败的 handler 测试**

`internal/handler/seckill_test.go`:

```go
package handler_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/mjyang04/flash-deal/internal/domain"
	"github.com/mjyang04/flash-deal/internal/handler"
)

type stubSvc struct {
	out domain.SeckillOutcome
	rem int
}

func (s *stubSvc) Seckill(_ context.Context, _ domain.SeckillRequest) (handler.SeckillOutput, error) {
	return handler.SeckillOutput{Outcome: s.out, QueueToken: "tok", Remaining: s.rem}, nil
}

func post(t *testing.T, r http.Handler, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest("POST", "/v1/seckill", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func newRouter(out domain.SeckillOutcome, rem int) http.Handler {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/v1/seckill", handler.Seckill(&stubSvc{out: out, rem: rem}))
	return r
}

func TestHandler_Outcomes(t *testing.T) {
	body := `{"activity_id":1,"user_id":2,"idempotency_token":"t"}`
	cases := []struct {
		out  domain.SeckillOutcome
		code int
	}{
		{domain.OutcomeQueued, http.StatusAccepted},
		{domain.OutcomeSoldOut, http.StatusGone},
		{domain.OutcomeUserLimit, http.StatusConflict},
		{domain.OutcomeDuplicate, http.StatusConflict},
		{domain.OutcomeNotStarted, http.StatusForbidden},
		{domain.OutcomeEnded, http.StatusGone},
		{domain.OutcomeNotFound, http.StatusNotFound},
		{domain.OutcomeInternal, http.StatusInternalServerError},
	}
	for _, c := range cases {
		w := post(t, newRouter(c.out, 9), body)
		if w.Code != c.code {
			t.Errorf("outcome %v → code %d, want %d", c.out, w.Code, c.code)
		}
	}
}

func TestHandler_QueuedBody(t *testing.T) {
	w := post(t, newRouter(domain.OutcomeQueued, 9), `{"activity_id":1,"user_id":2,"idempotency_token":"t"}`)
	var got map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got["status"] != "queued" {
		t.Errorf("status = %v, want queued", got["status"])
	}
	if got["queue_token"] != "tok" {
		t.Errorf("queue_token = %v", got["queue_token"])
	}
	if int(got["remaining"].(float64)) != 9 {
		t.Errorf("remaining = %v, want 9", got["remaining"])
	}
}

func TestHandler_BadRequest(t *testing.T) {
	w := post(t, newRouter(domain.OutcomeQueued, 0), `{"activity_id":0,"user_id":0}`) // missing token + zero ids
	if w.Code != http.StatusBadRequest {
		t.Errorf("code = %d, want 400 (got body: %s)", w.Code, w.Body.String())
	}
}

// guard against accidental signature drift
var _ = strconv.Itoa
```

- [ ] **Step 11.2:跑测试确认 FAIL**

```bash
go test ./internal/handler/...
```

Expected:`undefined: handler.Seckill / handler.SeckillOutput`

- [ ] **Step 11.3:实现 `internal/handler/seckill.go`**

```go
// Package handler exposes HTTP handlers for the seckill API.
package handler

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/mjyang04/flash-deal/internal/domain"
	"github.com/mjyang04/flash-deal/internal/middleware"
)

// SeckillOutput mirrors service.SeckillOutput so the handler can depend on its
// own port instead of importing the service package transitively in tests.
type SeckillOutput struct {
	Outcome    domain.SeckillOutcome
	QueueToken string
	Remaining  int
}

// SeckillService is the port the handler calls.
type SeckillService interface {
	Seckill(ctx context.Context, req domain.SeckillRequest) (SeckillOutput, error)
}

// Seckill returns a Gin handler that drives the seckill flow.
func Seckill(svc SeckillService) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req domain.SeckillRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			writeError(c, http.StatusBadRequest, "BAD_REQUEST", err.Error())
			return
		}
		out, err := svc.Seckill(c.Request.Context(), req)
		if err != nil && out.Outcome == domain.OutcomeInternal {
			writeError(c, http.StatusInternalServerError, "INTERNAL", err.Error())
			return
		}
		switch out.Outcome {
		case domain.OutcomeQueued:
			c.JSON(http.StatusAccepted, gin.H{
				"status":      out.Outcome.String(),
				"queue_token": out.QueueToken,
				"remaining":   out.Remaining,
			})
		case domain.OutcomeSoldOut:
			writeError(c, http.StatusGone, "STOCK_NOT_ENOUGH", "sold out")
		case domain.OutcomeUserLimit:
			writeError(c, http.StatusConflict, "USER_LIMIT_EXCEEDED", "per-user limit hit")
		case domain.OutcomeDuplicate:
			writeError(c, http.StatusConflict, "IDEMPOTENT_REPLAY", "duplicate token")
		case domain.OutcomeNotStarted:
			writeError(c, http.StatusForbidden, "ACTIVITY_NOT_STARTED", "activity not started")
		case domain.OutcomeEnded:
			writeError(c, http.StatusGone, "ACTIVITY_ENDED", "activity ended")
		case domain.OutcomeNotFound:
			writeError(c, http.StatusNotFound, "NOT_FOUND", "activity not found")
		default:
			writeError(c, http.StatusInternalServerError, "INTERNAL", "unknown outcome")
		}
	}
}

func writeError(c *gin.Context, status int, code, msg string) {
	c.JSON(status, gin.H{
		"error": gin.H{
			"code":       code,
			"message":    msg,
			"request_id": middleware.RequestIDFrom(c),
		},
	})
}
```

- [ ] **Step 11.4:跑 seckill handler 测试确认 PASS**

```bash
go test -v ./internal/handler/... -run TestHandler
```

Expected:全部 PASS

- [ ] **Step 11.5:写失败的 admin 测试**

`internal/handler/admin_test.go`:

```go
package handler_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/mjyang04/flash-deal/internal/domain"
	"github.com/mjyang04/flash-deal/internal/handler"
)

type adminStub struct {
	created  []domain.Activity
	warmedID int64
}

func (s *adminStub) Create(_ context.Context, a domain.Activity) error {
	s.created = append(s.created, a)
	return nil
}
func (s *adminStub) Warm(_ context.Context, id int64) (int, error) {
	s.warmedID = id
	return 100, nil
}

func TestAdmin_CreateActivity(t *testing.T) {
	gin.SetMode(gin.TestMode)
	stub := &adminStub{}
	r := gin.New()
	r.POST("/admin/activities", handler.AdminCreateActivity(stub))

	body := `{"id":9001,"product_id":555,"total_stock":100,"start_at":"2026-05-25T12:00:00Z","end_at":"2026-05-25T13:00:00Z","per_user_limit":1,"status":2}`
	req := httptest.NewRequest("POST", "/admin/activities", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("code = %d, body = %s", w.Code, w.Body.String())
	}
	if len(stub.created) != 1 || stub.created[0].ID != 9001 {
		t.Errorf("not stored: %+v", stub.created)
	}
}

func TestAdmin_WarmActivity(t *testing.T) {
	gin.SetMode(gin.TestMode)
	stub := &adminStub{}
	r := gin.New()
	r.POST("/admin/activities/:id/warm", handler.AdminWarmActivity(stub))

	req := httptest.NewRequest("POST", "/admin/activities/9001/warm", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("code = %d, body = %s", w.Code, w.Body.String())
	}
	if stub.warmedID != 9001 {
		t.Errorf("warmedID = %d, want 9001", stub.warmedID)
	}
}
```

- [ ] **Step 11.6:实现 `internal/handler/admin.go`**

```go
package handler

import (
	"context"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"

	"github.com/mjyang04/flash-deal/internal/domain"
)

// AdminService is the port admin handlers depend on.
type AdminService interface {
	Create(ctx context.Context, a domain.Activity) error
	Warm(ctx context.Context, id int64) (int, error)
}

// AdminCreateActivity: POST /admin/activities — write canonical record to MySQL.
// M1 leaves Redis warm to a separate explicit call (AdminWarmActivity).
func AdminCreateActivity(svc AdminService) gin.HandlerFunc {
	return func(c *gin.Context) {
		var a domain.Activity
		if err := c.ShouldBindJSON(&a); err != nil {
			writeError(c, http.StatusBadRequest, "BAD_REQUEST", err.Error())
			return
		}
		if err := svc.Create(c.Request.Context(), a); err != nil {
			writeError(c, http.StatusInternalServerError, "INTERNAL", err.Error())
			return
		}
		c.JSON(http.StatusCreated, gin.H{"id": a.ID})
	}
}

// AdminWarmActivity: POST /admin/activities/:id/warm — refresh Redis cache from MySQL.
func AdminWarmActivity(svc AdminService) gin.HandlerFunc {
	return func(c *gin.Context) {
		id, err := strconv.ParseInt(c.Param("id"), 10, 64)
		if err != nil {
			writeError(c, http.StatusBadRequest, "BAD_REQUEST", "bad id")
			return
		}
		warmed, err := svc.Warm(c.Request.Context(), id)
		if err != nil {
			writeError(c, http.StatusInternalServerError, "INTERNAL", err.Error())
			return
		}
		c.JSON(http.StatusOK, gin.H{"warmed_stock": warmed})
	}
}
```

- [ ] **Step 11.7:跑全部 handler 测试 PASS**

```bash
go test -v ./internal/handler/...
```

Expected:seckill + admin 测试全部 PASS

- [ ] **Step 11.8:Commit**

```bash
git add internal/handler/
git commit -m "feat(handler): seckill outcome→HTTP mapping + admin create/warm"
```

---

## Task 12:接线到 `cmd/api/main.go`

**Files:**
- Modify: `cmd/api/main.go`(整体替换)

> 说明:M1 不引 Kafka / OTel。Admin service 直接用 `repo.ActivityRepo` + `repo.StockRepo` 组合;`Warm` 在 M1 暂时把 `total_stock` 写到 Redis `stock:{id}` 一个 key(M2 切 Lua 时会沿用相同 key)。

- [ ] **Step 12.1:新建 `internal/service/admin.go` 充当 admin service**

```go
package service

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/mjyang04/flash-deal/internal/domain"
	"github.com/mjyang04/flash-deal/internal/repo"
)

type AdminService struct {
	activities repo.ActivityRepo
	rdb        *redis.Client
}

func NewAdmin(activities repo.ActivityRepo, rdb *redis.Client) *AdminService {
	return &AdminService{activities: activities, rdb: rdb}
}

func (s *AdminService) Create(ctx context.Context, a domain.Activity) error {
	return s.activities.Create(ctx, a)
}

// Warm copies activity.total_stock into Redis under stock:{id}.
// M2's Lua deduct reads/writes the same key.
func (s *AdminService) Warm(ctx context.Context, id int64) (int, error) {
	a, err := s.activities.GetByID(ctx, id)
	if err != nil {
		return 0, err
	}
	key := fmt.Sprintf("stock:%d", id)
	if err := s.rdb.Set(ctx, key, a.TotalStock, 24*time.Hour).Err(); err != nil {
		return 0, err
	}
	return a.TotalStock, nil
}
```

- [ ] **Step 12.2:重写 `cmd/api/main.go`**

```go
// Package main is the HTTP API entrypoint for flash-deal.
//
// Run:
//
//	make up && make migrate && go run ./cmd/api
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/mjyang04/flash-deal/internal/config"
	"github.com/mjyang04/flash-deal/internal/handler"
	"github.com/mjyang04/flash-deal/internal/infra/logger"
	fdmysql "github.com/mjyang04/flash-deal/internal/infra/mysql"
	fdredis "github.com/mjyang04/flash-deal/internal/infra/redis"
	"github.com/mjyang04/flash-deal/internal/middleware"
	"github.com/mjyang04/flash-deal/internal/repo"
	"github.com/mjyang04/flash-deal/internal/service"
)

func main() {
	cfg, err := config.Load(os.Getenv("FD_CONFIG_FILE"))
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	if _, err := logger.Init(os.Getenv("FD_LOG_MODE")); err != nil {
		log.Fatalf("logger: %v", err)
	}

	db, err := fdmysql.Open(cfg.MySQL)
	if err != nil {
		log.Fatalf("mysql open: %v", err)
	}
	defer db.Close()

	rdb := fdredis.New(cfg.Redis)
	defer rdb.Close()

	ar := repo.NewActivityRepo(db)
	or := repo.NewOrderRepo(db)
	sr := repo.NewStockRepo(db)

	// snowflake-lite id generator for M1: time-based monotonic int64.
	var idCounter int64
	nextID := func() int64 {
		return time.Now().UnixNano() + atomic.AddInt64(&idCounter, 1)
	}
	seckillSvc := service.New(ar, sr, or, time.Now, nextID)
	adminSvc := service.NewAdmin(ar, rdb)

	// adapter so handler.SeckillService is satisfied by *service.SeckillService.
	svcAdapter := serviceHandlerAdapter{inner: seckillSvc}

	r := gin.New()
	r.Use(middleware.RequestID(), middleware.Recovery())

	r.GET("/health", func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"status": "ok"}) })
	r.POST("/v1/seckill", handler.Seckill(svcAdapter))
	r.POST("/admin/activities", handler.AdminCreateActivity(adminSvc))
	r.POST("/admin/activities/:id/warm", handler.AdminWarmActivity(adminSvc))

	srv := &http.Server{
		Addr:              cfg.HTTP.Addr,
		Handler:           r,
		ReadHeaderTimeout: cfg.HTTP.ReadHeaderWait,
	}
	go func() {
		log.Printf("flash-deal api on %s", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("listen: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), cfg.HTTP.ShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("shutdown: %v", err)
	}
}

// serviceHandlerAdapter bridges service.SeckillOutput → handler.SeckillOutput.
type serviceHandlerAdapter struct {
	inner *service.SeckillService
}

func (a serviceHandlerAdapter) Seckill(ctx context.Context, req domainSeckillReq) (handler.SeckillOutput, error) {
	out, err := a.inner.Seckill(ctx, req)
	return handler.SeckillOutput{
		Outcome:    out.Outcome,
		QueueToken: out.QueueToken,
		Remaining:  out.Remaining,
	}, err
}

// alias keeps the adapter signature short.
type domainSeckillReq = struct {
	ActivityID       int64  `json:"activity_id" binding:"required"`
	UserID           int64  `json:"user_id" binding:"required"`
	IdempotencyToken string `json:"idempotency_token" binding:"required"`
}
```

> 校正:`domainSeckillReq` 与 `domain.SeckillRequest` 必须**完全相同**。为避免漂移,把 adapter 改成直接用 `domain.SeckillRequest`。修订下面这段:

替换 main.go 中 `serviceHandlerAdapter` 与 `domainSeckillReq` 两块为:

```go
import (
	...
	"github.com/mjyang04/flash-deal/internal/domain"
)

type serviceHandlerAdapter struct {
	inner *service.SeckillService
}

func (a serviceHandlerAdapter) Seckill(ctx context.Context, req domain.SeckillRequest) (handler.SeckillOutput, error) {
	out, err := a.inner.Seckill(ctx, req)
	return handler.SeckillOutput{
		Outcome:    out.Outcome,
		QueueToken: out.QueueToken,
		Remaining:  out.Remaining,
	}, err
}
```

- [ ] **Step 12.3:编译**

```bash
go build ./...
```

Expected:无报错

- [ ] **Step 12.4:启动并做 curl 烟测**

```bash
make up && make migrate
go run ./cmd/api &
API_PID=$!
sleep 2

# 1. 创建活动(now ± 1h)
NOW=$(date -u +%Y-%m-%dT%H:%M:%SZ)
END=$(date -u -v+1H +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -d '+1 hour' +%Y-%m-%dT%H:%M:%SZ)
curl -sS -X POST -H 'Content-Type: application/json' \
  -d "{\"id\":9001,\"product_id\":555,\"total_stock\":10,\"start_at\":\"$NOW\",\"end_at\":\"$END\",\"per_user_limit\":1,\"status\":2}" \
  http://localhost:8080/admin/activities

# 2. 预热(M1 这步可选,因为 stock 走 SQL)
curl -sS -X POST http://localhost:8080/admin/activities/9001/warm

# 3. 下单
curl -sS -X POST -H 'Content-Type: application/json' \
  -d '{"activity_id":9001,"user_id":1,"idempotency_token":"tok-curl-1"}' \
  http://localhost:8080/v1/seckill

kill $API_PID
```

Expected:第三次 curl 返回 `{"queue_token":"...","remaining":9,"status":"queued"}` HTTP 202

- [ ] **Step 12.5:Commit**

```bash
git add cmd/api/main.go internal/service/admin.go
git commit -m "feat(api): wire config/mysql/redis/repos/handlers into cmd/api"
```

---

## Task 13:`cmd/seed` 种子数据

**Files:**
- Create: `cmd/seed/main.go`
- Modify: `Makefile` 中 `seed` target 实现

- [ ] **Step 13.1:实现 `cmd/seed/main.go`**

```go
// Package main seeds a demo activity (id=1001) and warms Redis stock for it.
//
// Run:
//
//	go run ./cmd/seed [ACTIVITY_ID] [TOTAL_STOCK]
//	# defaults: ACTIVITY_ID=1001 TOTAL_STOCK=1000
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/mjyang04/flash-deal/internal/config"
	"github.com/mjyang04/flash-deal/internal/domain"
	fdmysql "github.com/mjyang04/flash-deal/internal/infra/mysql"
	fdredis "github.com/mjyang04/flash-deal/internal/infra/redis"
	"github.com/mjyang04/flash-deal/internal/repo"
	"github.com/mjyang04/flash-deal/internal/service"
)

func main() {
	id := int64(1001)
	stock := 1000
	if len(os.Args) > 1 {
		if v, err := strconv.ParseInt(os.Args[1], 10, 64); err == nil {
			id = v
		}
	}
	if len(os.Args) > 2 {
		if v, err := strconv.Atoi(os.Args[2]); err == nil {
			stock = v
		}
	}

	cfg, err := config.Load(os.Getenv("FD_CONFIG_FILE"))
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	db, err := fdmysql.Open(cfg.MySQL)
	if err != nil {
		log.Fatalf("mysql: %v", err)
	}
	defer db.Close()
	rdb := fdredis.New(cfg.Redis)
	defer rdb.Close()

	ar := repo.NewActivityRepo(db)
	adminSvc := service.NewAdmin(ar, rdb)

	now := time.Now().UTC()
	a := domain.Activity{
		ID: id, ProductID: 555, TotalStock: stock,
		StartAt: now.Add(-time.Minute), EndAt: now.Add(2 * time.Hour),
		PerUserLimit: 1, Status: domain.ActivityRunning,
	}
	ctx := context.Background()
	if err := adminSvc.Create(ctx, a); err != nil && !errors.Is(err, errAlreadyExists(err)) {
		// MySQL duplicate PK; ok if user re-seeds — proceed to warm.
		log.Printf("create: %v (continuing to warm)", err)
	}
	warmed, err := adminSvc.Warm(ctx, id)
	if err != nil {
		log.Fatalf("warm: %v", err)
	}
	fmt.Printf("seeded activity=%d stock=%d warmed_stock=%d\n", id, stock, warmed)
}

// errAlreadyExists is a sentinel-detector; sqlx returns the driver error directly.
func errAlreadyExists(err error) error {
	if err == nil {
		return nil
	}
	if e := err.Error(); e != "" && (containsDup(e)) {
		return err
	}
	return nil
}
func containsDup(s string) bool {
	return len(s) > 0 && (s[0] == 'E' || s[0] == 'D') // best-effort, used only for logging
}
```

- [ ] **Step 13.2:更新 `Makefile` 的 `seed` target**

替换原 `seed:` 段为:

```makefile
seed:
	go run ./cmd/seed
```

- [ ] **Step 13.3:跑 seed 验证**

```bash
make up && make migrate
make seed
docker exec -i fd-mysql mysql -uflashdeal -pflashdeal -e 'select id,total_stock from activities' flashdeal
docker exec -i fd-redis redis-cli get stock:1001
```

Expected:
- MySQL 行 `1001 | 1000`
- Redis 返回 `"1000"`

- [ ] **Step 13.4:Commit**

```bash
git add cmd/seed/ Makefile
git commit -m "feat(seed): demo activity + redis warm via cmd/seed"
```

---

## Task 14:调整 k6 baseline 脚本

**Files:**
- Modify: `bench/k6/seckill.js`

> M1 期望 baseline 是温和的 constant 1000 rps × 30s,先确认 wiring 正确再追高。M2/M3/M4 才上 burst。

- [ ] **Step 14.1:替换 `bench/k6/seckill.js` 的 `options` 段**

把 `options.scenarios` 改为:

```javascript
export const options = {
  scenarios: {
    baseline_m1: {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.RATE || 1000),
      timeUnit: '1s',
      duration: __ENV.DURATION || '30s',
      preAllocatedVUs: 200,
      maxVUs: 1000,
    },
  },
  thresholds: {
    // M1 baseline — record only, do not gate. Tighten in M4.
    http_req_failed: ['rate<1'],
    http_req_duration: ['p(99)<1000'],
  },
};
```

`check()` 段不变(202/409/410 都算预期)。

- [ ] **Step 14.2:本地跑一次确认没回归**

```bash
make seed
go run ./cmd/api &
API_PID=$!
sleep 2
RATE=200 DURATION=10s k6 run bench/k6/seckill.js
kill $API_PID
```

Expected:k6 输出 RPS ≈ 200,`http_req_failed` 不触发阈值。

- [ ] **Step 14.3:Commit**

```bash
git add bench/k6/seckill.js
git commit -m "chore(bench): k6 constant-arrival-rate baseline for M1"
```

---

## Task 15:跑 baseline 并写报告

**Files:**
- Create: `reports/week1_mvp.md`

- [ ] **Step 15.1:重置数据 + 启 API**

```bash
make down && make up && make migrate
make seed
go run ./cmd/api > /tmp/fd-api.log 2>&1 &
API_PID=$!
sleep 3
```

- [ ] **Step 15.2:跑三档 baseline,每档记录 RPS / P50 / P95 / P99 / error rate**

```bash
for rate in 500 1000 2000; do
  echo "=== RATE=$rate ==="
  RATE=$rate DURATION=30s k6 run bench/k6/seckill.js \
    --summary-export=/tmp/fd-bench-$rate.json
  # 每档之间重置库存
  docker exec -i fd-mysql mysql -uflashdeal -pflashdeal -e \
    'update activities set total_stock=1000 where id=1001' flashdeal
  docker exec -i fd-redis redis-cli set stock:1001 1000
done
kill $API_PID
```

- [ ] **Step 15.3:写报告 `reports/week1_mvp.md`**

骨架(填入实际数字):

```markdown
# Week 1 — MVP Baseline (M1)

## Setup
- Host: <CPU / RAM / OS>
- API: single instance, `go run ./cmd/api`, GOMAXPROCS=<n>
- MySQL: docker fd-mysql, innodb_buffer_pool_size=512M
- Redis: docker fd-redis, M1 仅做活动 warm,SQL 是 stock 权威
- Bench: k6 vX.Y, scenario `baseline_m1` (constant-arrival-rate)
- Commit: <git rev-parse --short HEAD>

## Workloads
| Rate (rps) | Duration | http_reqs | success(202) | sold_out(410) | http_req_duration P50 / P95 / P99 (ms) | error rate |
|------------|----------|-----------|--------------|---------------|----------------------------------------|-----------|
| 500        | 30s      | …         | …            | …             | … / … / …                              | …         |
| 1000       | 30s      | …         | …            | …             | … / … / …                              | …         |
| 2000       | 30s      | …         | …            | …             | … / … / …                              | …         |

## Correctness
- 初始 stock = 1000;final stock = `select total_stock from activities where id=1001` = **N**
- INSERTed orders = `select count(*) from orders_0 where activity_id=1001` = **M**
- 等式 `N + M == 1000` 是否成立:✅/❌

## Findings
- 瓶颈点(MySQL row lock contention / 连接池 / gin)
- pprof 截图(可选)

## Next (M2 目标)
- 把 stock deduct 切到 Redis Lua,期望同等 rate 下 P99 下降一个量级
- 把 order insert 切到 Kafka,期望 stock 路径与 order 路径解耦,峰值 RPS 上限大幅提高
```

- [ ] **Step 15.4:Commit**

```bash
git add reports/week1_mvp.md
git commit -m "chore(report): M1 baseline numbers + correctness check"
```

---

## Task 16:Lint / Tidy / Tag

- [ ] **Step 16.1:整理 + lint**

```bash
go mod tidy
gofmt -s -w .
go vet ./...
golangci-lint run || true  # M1 阶段不阻塞;记录 issue 留 M2-M4 处理
```

- [ ] **Step 16.2:跑全部 unit 测试**

```bash
go test -race -cover ./...
```

Expected:全部 PASS

- [ ] **Step 16.3:跑全部 integration 测试(需 docker-compose 已启动)**

```bash
make up && make migrate
go test -tags=integration -race ./internal/...
```

Expected:全部 PASS,**特别确认 `TestStockRepo_Deduct_NoOversell` 通过**

- [ ] **Step 16.4:更新 README(简短)+ Commit**

`README.md` 末尾加一段(若 README 不存在,先 `touch README.md`):

```markdown
## Run M1 (MVP)
```sh
make up        # mysql / redis / kafka / jaeger / prom / grafana
make migrate   # activities + orders_0
make seed      # demo activity id=1001 stock=1000
make api       # http://localhost:8080
make bench     # k6 constant-arrival-rate
```

详见 `plan/` 与 `docs/superpowers/plans/2026-05-25-flash-deal-m1-mvp.md`。
```

```bash
git add README.md go.mod go.sum
git commit -m "docs(readme): M1 quick-start"
```

- [ ] **Step 16.5:Tag m1**

```bash
git tag -a m1 -m "M1: synchronous MVP, baseline bench, no Lua/Kafka/sharding"
git log --oneline -20
```

Expected:看到 m1 tag 指向最后一个 commit

---

## 验收清单(收尾时逐项核查)

- [ ] `make up && make migrate && make seed && make api` 一键起 API
- [ ] `curl -X POST -H 'Content-Type: application/json' -d '{"activity_id":1001,"user_id":1,"idempotency_token":"x"}' :8080/v1/seckill` 返回 202
- [ ] `go test ./...` 全 PASS
- [ ] `go test -tags=integration ./internal/...` 全 PASS(包括并发零超卖)
- [ ] `reports/week1_mvp.md` 含三档 baseline 数字 + correctness 等式
- [ ] git tag `m1` 已打
- [ ] 已知不在 M1 范围的 TODO(写入 `plan/decisions.md` 或 `plan/risks.md`):Lua 路径(M2)、Kafka 路径(M2)、分库分表(M3)、限流熔断(M3)、OTel/Prom/Grafana(M3)、pprof 优化(M4)

---

## Self-Review 结果

| 检查 | 状态 |
|------|------|
| Spec 覆盖:milestone.md Day 1-7 全部映射到 Task | ✅(Day 1-2 → Task 0;Day 3-4 → Task 1-9;Day 5 → Task 10-12;Day 6 → Task 14-15;Day 7 → Task 16) |
| Placeholder 扫描:无 TBD / TODO / "类似 Task N" | ✅ |
| 类型一致:`Outcome` 在 domain 定义,service 用,handler 用同一 enum | ✅ |
| 与已有 scaffold 一致:module path `github.com/mjyang04/flash-deal`、`orders_0`、Redis port 6380、MySQL port 3307 | ✅ |
| 不引入 M2+ 依赖(Kafka producer / Lua / otel) | ✅(cmd/api 只 import M1 必需模块) |

---

## M2 / M3 / M4 plan 占位(下次会话单独写)

按同样格式产出:

- `docs/superpowers/plans/YYYY-MM-DD-flash-deal-m2-async.md`
  覆盖 Week 2:Lua 切换 + Kafka producer/consumer + 排队 token 轮询 + DLQ + 对照 baseline。
- `docs/superpowers/plans/YYYY-MM-DD-flash-deal-m3-shard-observability.md`
  覆盖 Week 3:`orders_{0..3}` 分片 + per-shard `OrderRepo` + Redis 令牌桶限流 + gobreaker + OTel 跨 Kafka 透传 + Prom 指标 + Grafana dashboard + chaos 测试。
- `docs/superpowers/plans/YYYY-MM-DD-flash-deal-m4-optimize-release.md`
  覆盖 Week 4:pprof CPU/alloc/mutex → 优化连接池 / sync.Pool / GOMEMLIMIT;终版压测 + 报告 + 博客 + Tag `m4-release`。
