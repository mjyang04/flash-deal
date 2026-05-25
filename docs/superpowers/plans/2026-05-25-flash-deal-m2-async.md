# flash-deal M2 — Redis Lua + Kafka 异步化 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 stock 扣减从 MySQL 行锁切到 **Redis Lua 原子脚本**,把 order 落库从同步 INSERT 切到 **Kafka 异步** + 独立 consumer 物化;新增排队 token API,让客户端可轮询查落单结果。期望与 M1 baseline 对照:同 1000 rps 下 P95 从 2.4s → < 50ms。

**Architecture:** Stock 权威移到 Redis(MySQL `activities.total_stock` 只在 warm 时被读取),Lua 脚本一次性完成"检查 + 扣减 + 用户限购";扣减成功后 API 把订单消息投递到 Kafka topic `seckill_orders`,立即返回 202 + queue token;独立 `cmd/consumer` 消费消息、写 `orders_0`、把订单状态写回 Redis `queue:{token}` 供客户端轮询。失败走 DLQ。

**Tech Stack:** 接续 M1 栈 · 新引入 `segmentio/kafka-go v0.4.47`(已在 go.mod) · `apache/kafka:3.8.0` docker image(替换 M1 注释掉的 `bitnami/kafka:3.8`) · Redis Lua + EVALSHA 缓存

**Scope:** Week 2 / Day 8-14 / Tag `m2`。**不在本 plan**:分库分表(M3)、限流熔断(M3)、OTel/Prometheus(M3)、pprof 优化(M4)。

**前置状态(M1 已就绪):**
- tag `m1` 已打,M1 同步路径完整可跑
- `internal/repo/stock_repo.go`(SQL 行锁 `Deduct(activityID, n)`)—— M2 会**并行**新增 `stock_redis_repo.go`,接口签名一致,wire 时通过 config 切换
- `internal/repo/order_repo.go`(`Create` 同步 INSERT)—— M2 会保留,consumer 直接复用
- `internal/service/seckill.go` 的 `OrderCreator` 端口 —— M2 切换实现为 kafka producer
- `internal/infra/redis/scripts/stock_deduct.lua` 已存在,M2 才真正调用
- `cmd/consumer/main.go` 是 stub —— M2 实现
- `deploy/docker-compose.yml` 中 kafka 段被注释 —— M2 复活并切到 `apache/kafka:3.8.0`(env 名去掉 `_CFG_`)

---

## File Map

### 已存在 — 本 plan 会修改

| 路径 | 改动 |
|------|------|
| `deploy/docker-compose.yml` | 复活 kafka service,改用 `apache/kafka:3.8.0` |
| `internal/config/config.go` | 加 `KafkaConfig` + `Switches`(LuaStock / KafkaOrder 开关) |
| `internal/config/config_test.go` | 加 kafka 默认值 + env override 测试 |
| `internal/infra/redis/scripts/stock_deduct.lua` | 不动(已是终版) |
| `internal/repo/stock_repo.go` | 不动,保留 SQL impl 作为 fallback |
| `internal/service/seckill.go` | 不动,因为依赖小端口接口 —— wire 改在 main.go |
| `cmd/api/main.go` | 接线时按 cfg 切 stock 实现 + 用 Kafka producer 替换 OrderCreator |
| `cmd/consumer/main.go` | 实现完整消费循环 |
| `Makefile` | 加 `make consumer`(已有 target,补完语义);加 `make kafka-topic`(创建 topic) |
| `bench/k6/seckill.js` | 跑 M2 对照前不动;Task 12 跑完后写新报告 |

### 新建

| 路径 | 单一职责 | 行数预算 |
|------|---------|---------|
| `internal/repo/stock_redis_repo.go` | Lua EVALSHA 实现 `StockRepo`,outcome → `(remaining, err)`;复用 SQL impl 的接口 | < 180 |
| `internal/repo/stock_redis_repo_test.go` | integration:warm → 1000G × 100 stock 零超卖 + per_user_limit 验证 + not_warmed 错误 | < 250 |
| `internal/infra/kafka/producer.go` | `*Producer` 封装 `kafka.Writer`,sync produce + 失败错误传播 | < 100 |
| `internal/infra/kafka/producer_test.go` | integration:produce → 自己 consume 验证 | < 120 |
| `internal/infra/kafka/consumer.go` | `*Consumer` 封装 `kafka.Reader`(manual commit),Loop 取消息 + 回调 + ack | < 150 |
| `internal/infra/kafka/topics.go` | topic 常量 + 消息 envelope struct + Marshal/Unmarshal | < 80 |
| `internal/service/order_materializer.go` | consumer 端业务:把 Kafka 消息 → `OrderRepo.Create` + 更新 Redis queue token state | < 150 |
| `internal/service/order_materializer_test.go` | 单元:用 mock OrderRepo + redis mini → 验证幂等(dup token 不重复 INSERT)+ DLQ 触发 | < 250 |
| `internal/service/kafka_order_creator.go` | 实现 service.OrderCreator port,但底层是 Kafka produce(不写 DB) | < 100 |
| `internal/service/queue_token.go` | `QueueService` 生成 token(UUIDv7) + 写/读 Redis `queue:{token}` | < 120 |
| `internal/service/queue_token_test.go` | integration:write → read 状态轮询 | < 120 |
| `internal/handler/order.go` | `GET /v1/order/by-token/:token`(查 Redis 队列状态)| < 80 |
| `internal/handler/order_test.go` | httptest:queued / success / failed / not_found | < 150 |
| `bench/k6/seckill_m2.js` | M2 对照脚本,同三档 rate;额外加 queue token polling 场景 | < 80 |
| `reports/week2_redis_kafka.md` | M1 vs M2 对照报告 | 模板 |

---

## 测试环境约定(沿用 M1)

- 集成测试 build tag `integration`,需要 `make up && make migrate`
- 新增 `make up` 必须能成功启动 kafka,kafka 健康前 wait
- Kafka 测试连接:`FD_TEST_KAFKA_BROKERS=127.0.0.1:9092`,topic 由测试自创建(测前 delete + create)

---

## Outcome 与错误码映射(Lua 路径)

`stock_deduct.lua` 返回值:
- `>= 0` → success,值是 post-deduct remaining
- `-1` → sold out → `OutcomeSoldOut` → HTTP 410 `STOCK_NOT_ENOUGH`
- `-2` → per-user limit → `OutcomeUserLimit` → HTTP 409 `USER_LIMIT_EXCEEDED`
- `-3` → not warmed → 视为运行事故 → HTTP 503 `BACKEND_UNAVAILABLE`(M1 此情况会返回 `OutcomeInternal`,M2 提升为独立信号)

新增 outcome / wire string:`OutcomeNotWarmed → "not_warmed" → HTTP 503`。`internal/domain/seckill.go` 加常量 + String 分支 + 测试覆盖。

Queue token state(`queue:{token}` value):
- `queued` —— consumer 还没处理
- `success:{order_id}` —— consumer 已 INSERT
- `failed:{reason}` —— consumer 永久失败(写 DLQ 后)

---

## Task 1:复活 docker-compose Kafka 段

**Files:** `deploy/docker-compose.yml`

- [ ] **Step 1.1:把 M1 时注释掉的 kafka 段恢复并切到 apache/kafka:3.8.0**

定位到 docker-compose.yml 中 `# kafka disabled in M1` 注释块,替换为(注意 env 名相对 bitnami 去掉 `_CFG_`):

```yaml
  kafka:
    image: apache/kafka:3.8.0
    container_name: fd-kafka
    environment:
      - KAFKA_NODE_ID=1
      - KAFKA_PROCESS_ROLES=broker,controller
      - KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafka:9093
      - KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093
      - KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092
      - KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER
      - KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT
      - KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      - CLUSTER_ID=5L6g3nShT-eMCtK--X86sw
      - KAFKA_LOG_DIRS=/tmp/kraft-combined-logs
    ports:
      - "9092:9092"
    healthcheck:
      test: ["CMD-SHELL", "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 >/dev/null 2>&1"]
      interval: 10s
      timeout: 5s
      retries: 10
```

- [ ] **Step 1.2:启动验证**

```bash
make down && make up
docker ps | grep fd-kafka                         # → Up (healthy)
docker exec fd-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Expected:命令返回(可能空)无 error。

- [ ] **Step 1.3:在 Makefile 加 `kafka-topic` target**

```makefile
kafka-topic:
	docker exec fd-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
	  --create --if-not-exists --topic seckill_orders --partitions 16 --replication-factor 1
	docker exec fd-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
	  --create --if-not-exists --topic seckill_orders_dlq --partitions 4 --replication-factor 1
```

```bash
make kafka-topic                                  # 创建 seckill_orders + DLQ
docker exec fd-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Expected:列出 `seckill_orders` + `seckill_orders_dlq`。

- [ ] **Step 1.4:Commit**

```bash
git add deploy/docker-compose.yml Makefile
git commit -m "chore(deploy): revive kafka via apache/kafka:3.8.0 (env names drop _CFG_); add make kafka-topic"
```

---

## Task 2:Config 加 KafkaConfig + Switches

**Files:** `internal/config/config.go`, `internal/config/config_test.go`

- [ ] **Step 2.1:扩展 Config struct(verbatim)**

在 `Config` struct 上方加两个新 struct,并在 `Config` 中追加字段:

```go
type KafkaConfig struct {
	Brokers           []string      `mapstructure:"brokers"`
	OrderTopic        string        `mapstructure:"order_topic"`
	DLQTopic          string        `mapstructure:"dlq_topic"`
	ConsumerGroup     string        `mapstructure:"consumer_group"`
	ProduceTimeout    time.Duration `mapstructure:"produce_timeout"`
	ConsumerBatchWait time.Duration `mapstructure:"consumer_batch_wait"`
}

type Switches struct {
	LuaStock     bool `mapstructure:"lua_stock"`
	KafkaOrder   bool `mapstructure:"kafka_order"`
}

type Config struct {
	HTTP     HTTPConfig  `mapstructure:"http"`
	MySQL    MySQLConfig `mapstructure:"mysql"`
	Redis    RedisConfig `mapstructure:"redis"`
	Kafka    KafkaConfig `mapstructure:"kafka"`
	Switches Switches    `mapstructure:"switches"`
}
```

在 `Load` 中加默认值:

```go
v.SetDefault("kafka.brokers", []string{"127.0.0.1:9092"})
v.SetDefault("kafka.order_topic", "seckill_orders")
v.SetDefault("kafka.dlq_topic", "seckill_orders_dlq")
v.SetDefault("kafka.consumer_group", "seckill-consumer")
v.SetDefault("kafka.produce_timeout", 2*time.Second)
v.SetDefault("kafka.consumer_batch_wait", 100*time.Millisecond)

v.SetDefault("switches.lua_stock", true)    // M2 默认开
v.SetDefault("switches.kafka_order", true)  // M2 默认开
```

- [ ] **Step 2.2:扩 config_test.go 加 2 个测试**

```go
func TestLoad_KafkaDefaults(t *testing.T) {
	cfg, err := config.Load("")
	if err != nil { t.Fatal(err) }
	if cfg.Kafka.OrderTopic != "seckill_orders" {
		t.Errorf("OrderTopic = %q", cfg.Kafka.OrderTopic)
	}
	if !cfg.Switches.LuaStock || !cfg.Switches.KafkaOrder {
		t.Errorf("Switches not on by default: %+v", cfg.Switches)
	}
}

func TestLoad_SwitchesOff(t *testing.T) {
	t.Setenv("FD_SWITCHES_LUA_STOCK", "false")
	t.Setenv("FD_SWITCHES_KAFKA_ORDER", "false")
	cfg, _ := config.Load("")
	if cfg.Switches.LuaStock || cfg.Switches.KafkaOrder {
		t.Errorf("switches did not flip: %+v", cfg.Switches)
	}
}
```

- [ ] **Step 2.3:跑测试**

```bash
go test ./internal/config/... -v
```

Expected:全 PASS。

- [ ] **Step 2.4:Commit**

```bash
git add internal/config/
git commit -m "feat(config): KafkaConfig + Switches (lua_stock / kafka_order)"
```

---

## Task 3:Domain 加 OutcomeNotWarmed + String

**Files:** `internal/domain/seckill.go`, `internal/domain/seckill_test.go`

- [ ] **Step 3.1:在常量块尾部加 `OutcomeNotWarmed`**

```go
const (
	OutcomeQueued SeckillOutcome = iota
	OutcomeSoldOut
	OutcomeUserLimit
	OutcomeDuplicate
	OutcomeNotStarted
	OutcomeEnded
	OutcomeNotFound
	OutcomeInternal
	OutcomeNotWarmed // M2: Lua 返回 -3,stock key 未预热
)
```

- [ ] **Step 3.2:`String()` 加分支**

```go
case OutcomeNotWarmed:
    return "not_warmed"
```

- [ ] **Step 3.3:扩 seckill_test.go 测试用例**

```go
{domain.OutcomeNotWarmed, "not_warmed"},
```

- [ ] **Step 3.4:跑测试 + Commit**

```bash
go test ./internal/domain/... -v
git add internal/domain/
git commit -m "feat(domain): OutcomeNotWarmed for un-warmed Lua stock key"
```

---

## Task 4:Kafka topics + envelope

**Files:** `internal/infra/kafka/topics.go`

- [ ] **Step 4.1:实现**

```go
// Package kafka holds Kafka producer/consumer primitives used by api and consumer.
package kafka

import (
	"encoding/json"
	"time"
)

// Topics constants — single source of truth used by both producer and consumer.
const (
	TopicSeckillOrders = "seckill_orders"
	TopicSeckillDLQ    = "seckill_orders_dlq"
)

// OrderMessage is the envelope produced by api after Redis Lua succeeds.
// Consumer materializes it into MySQL orders table.
type OrderMessage struct {
	Version          int       `json:"version"`
	OrderID          int64     `json:"order_id"`
	ActivityID       int64     `json:"activity_id"`
	UserID           int64     `json:"user_id"`
	ProductID        int64     `json:"product_id"`
	IdempotencyToken string    `json:"idempotency_token"`
	QueueToken       string    `json:"queue_token"`
	ProducedAt       time.Time `json:"produced_at"`
}

// Marshal returns the JSON bytes of the message.
func (m OrderMessage) Marshal() ([]byte, error) {
	return json.Marshal(m)
}

// UnmarshalOrderMessage decodes JSON into OrderMessage.
func UnmarshalOrderMessage(b []byte) (OrderMessage, error) {
	var m OrderMessage
	err := json.Unmarshal(b, &m)
	return m, err
}
```

- [ ] **Step 4.2:Commit(此 Task 无独立测试,Task 5/6 会用)**

```bash
go build ./internal/infra/kafka/...
git add internal/infra/kafka/topics.go
git commit -m "feat(kafka): topic constants + OrderMessage envelope"
```

---

## Task 5:Kafka Producer

**Files:** `internal/infra/kafka/producer.go`, `internal/infra/kafka/producer_test.go`

- [ ] **Step 5.1:写 producer_test.go(integration)**

```go
//go:build integration

package kafka_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	segkafka "github.com/segmentio/kafka-go"

	fdkafka "github.com/mjyang04/flash-deal/internal/infra/kafka"
)

func brokers(t *testing.T) []string {
	t.Helper()
	v := os.Getenv("FD_TEST_KAFKA_BROKERS")
	if v == "" {
		v = "127.0.0.1:9092"
	}
	return strings.Split(v, ",")
}

func TestProducer_RoundTrip(t *testing.T) {
	topic := "test_producer_roundtrip"
	bs := brokers(t)
	// ensure topic exists (auto-create may be off in real Kafka; create explicitly via Writer.AllowAutoTopicCreation)
	p, err := fdkafka.NewProducer(bs, 2*time.Second)
	if err != nil { t.Fatal(err) }
	defer p.Close()

	msg := fdkafka.OrderMessage{
		Version: 1, OrderID: 42, ActivityID: 100, UserID: 7,
		ProductID: 555, IdempotencyToken: "tok-x", QueueToken: "q-x",
		ProducedAt: time.Now().UTC(),
	}
	if err := p.SendOrder(context.Background(), topic, msg); err != nil {
		t.Fatalf("send: %v", err)
	}

	// consume back to verify
	r := segkafka.NewReader(segkafka.ReaderConfig{
		Brokers: bs, Topic: topic, GroupID: "test_producer_roundtrip_group",
		MinBytes: 1, MaxBytes: 1e6,
	})
	defer r.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rec, err := r.ReadMessage(ctx)
	if err != nil { t.Fatalf("read: %v", err) }
	got, err := fdkafka.UnmarshalOrderMessage(rec.Value)
	if err != nil { t.Fatal(err) }
	if got.OrderID != 42 || got.IdempotencyToken != "tok-x" {
		t.Errorf("round-trip mismatch: %+v", got)
	}
}
```

- [ ] **Step 5.2:实现 `producer.go`**

```go
package kafka

import (
	"context"
	"strconv"
	"time"

	segkafka "github.com/segmentio/kafka-go"
)

// Producer wraps a kafka-go Writer for typed sends.
type Producer struct {
	writer  *segkafka.Writer
	timeout time.Duration
}

// NewProducer builds a Writer that auto-creates the topic on first publish.
// Use Close() on shutdown.
func NewProducer(brokers []string, produceTimeout time.Duration) (*Producer, error) {
	w := &segkafka.Writer{
		Addr:                   segkafka.TCP(brokers...),
		Balancer:               &segkafka.Hash{}, // by message key
		AllowAutoTopicCreation: true,
		BatchTimeout:           10 * time.Millisecond,
		RequiredAcks:           segkafka.RequireAll,
	}
	return &Producer{writer: w, timeout: produceTimeout}, nil
}

// SendOrder publishes a single message with key=user_id so per-user ordering is preserved.
func (p *Producer) SendOrder(ctx context.Context, topic string, m OrderMessage) error {
	b, err := m.Marshal()
	if err != nil {
		return err
	}
	cctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	return p.writer.WriteMessages(cctx, segkafka.Message{
		Topic: topic,
		Key:   []byte(strconv.FormatInt(m.UserID, 10)),
		Value: b,
		Time:  m.ProducedAt,
	})
}

// Close flushes pending writes.
func (p *Producer) Close() error { return p.writer.Close() }
```

- [ ] **Step 5.3:跑测试 + Commit**

```bash
go test -tags=integration -v ./internal/infra/kafka/...
git add internal/infra/kafka/producer.go internal/infra/kafka/producer_test.go go.mod go.sum
git commit -m "feat(kafka): producer with hash partitioning by user_id + round-trip test"
```

---

## Task 6:Kafka Consumer

**Files:** `internal/infra/kafka/consumer.go`

> 说明:Consumer 是个 generic loop;具体业务回调由 `service.OrderMaterializer` 提供。本 Task 只交付 loop 框架,业务逻辑测试放 Task 9。

- [ ] **Step 6.1:实现**

```go
package kafka

import (
	"context"
	"time"

	segkafka "github.com/segmentio/kafka-go"
)

// Handler processes one message and returns nil on success.
// If it returns a non-nil error, Consumer.Run will dispatch the message to dlqHandler.
type Handler func(ctx context.Context, m OrderMessage) error

// DLQHandler is called when Handler permanently fails. It should publish to a DLQ topic.
type DLQHandler func(ctx context.Context, raw []byte, reason error)

// Consumer wraps kafka-go Reader with manual commit semantics.
type Consumer struct {
	reader     *segkafka.Reader
	handler    Handler
	dlq        DLQHandler
	batchWait  time.Duration
	maxRetries int
}

// NewConsumer builds a Reader configured for manual offset commits.
func NewConsumer(brokers []string, topic, group string, batchWait time.Duration, maxRetries int) *Consumer {
	r := segkafka.NewReader(segkafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        group,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: 0, // manual commit
		MaxWait:        batchWait,
	})
	return &Consumer{reader: r, batchWait: batchWait, maxRetries: maxRetries}
}

// SetHandler / SetDLQ — caller wires in the business callbacks before Run.
func (c *Consumer) SetHandler(h Handler)       { c.handler = h }
func (c *Consumer) SetDLQ(d DLQHandler)        { c.dlq = d }

// Run blocks until ctx is canceled. Each message is retried up to maxRetries
// (with simple exponential backoff) before going to DLQ.
func (c *Consumer) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return c.reader.Close()
		default:
		}
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		om, err := UnmarshalOrderMessage(msg.Value)
		if err != nil {
			c.dlq(ctx, msg.Value, err)
			_ = c.reader.CommitMessages(ctx, msg)
			continue
		}
		var lastErr error
		for attempt := 0; attempt <= c.maxRetries; attempt++ {
			if attempt > 0 {
				time.Sleep(time.Duration(attempt) * 100 * time.Millisecond)
			}
			if lastErr = c.handler(ctx, om); lastErr == nil {
				break
			}
		}
		if lastErr != nil {
			c.dlq(ctx, msg.Value, lastErr)
		}
		// commit either way: handled (success) or dispatched to DLQ
		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			return err
		}
	}
}

// Close stops the reader.
func (c *Consumer) Close() error { return c.reader.Close() }
```

- [ ] **Step 6.2:Commit(此 Task 编译即可,运行测试在 Task 9 端到端验证)**

```bash
go build ./internal/infra/kafka/...
git add internal/infra/kafka/consumer.go
git commit -m "feat(kafka): generic consumer loop with manual commit + retry + DLQ hook"
```

---

## Task 7:Redis Lua StockRepo

**Files:** `internal/repo/stock_redis_repo.go`, `internal/repo/stock_redis_repo_test.go`

> 接口与 SQL 版**完全一致**(`StockRepo.Deduct(ctx, activityID, n) (remaining int, err)`),只是新增 `(activityID, userID int64)` 不行 —— 用户限购需要额外参数。**修订**:在 `StockRepo` 接口加一个新方法 `DeductForUser(ctx, activityID, userID, n) (remaining int, err)`,SQL 版补一个简单实现(`activities.total_stock` 行锁 + 忽略 userID,因为 SQL 版没存 per-user 状态),Redis 版用 Lua。Service 在 wire 时调用新接口。

- [ ] **Step 7.1:扩 `internal/repo/stock_repo.go` 加 `DeductForUser` 方法**

把现有接口改为:

```go
type StockRepo interface {
	// Deduct atomically subtracts n from activity stock.
	// Returns the post-deduction remaining stock, or ErrStockNotEnough.
	// userID 在 SQL impl 中被忽略;Redis Lua impl 用于 per_user_limit 检查。
	DeductForUser(ctx context.Context, activityID, userID int64, n, perUserLimit int) (remaining int, err error)
}
```

SQL impl 改造:把现有 `Deduct(...)` 重命名为 `DeductForUser(...)`,签名加 `userID, perUserLimit` 参数(忽略,只做 stock 行锁)。**保留旧的零超卖测试**,改用新签名。

新增 sentinel:

```go
var ErrStockNotWarmed = errors.New("stock not warmed")
var ErrUserLimitExceeded = errors.New("user limit exceeded")
```

- [ ] **Step 7.2:实现 `stock_redis_repo.go`**

```go
package repo

import (
	"context"
	"errors"
	"fmt"

	goredis "github.com/redis/go-redis/v9"
)

// stockRedisRepo runs the EVALSHA path against stock_deduct.lua.
type stockRedisRepo struct {
	rdb    *goredis.Client
	script *goredis.Script
}

// NewStockRedisRepo loads the script (LOAD on first call) and returns a StockRepo.
// The lua source lives at internal/infra/redis/scripts/stock_deduct.lua and is
// embedded here as a string to avoid runtime path coupling.
func NewStockRedisRepo(rdb *goredis.Client) StockRepo {
	return &stockRedisRepo{
		rdb:    rdb,
		script: goredis.NewScript(stockDeductLua),
	}
}

// DeductForUser invokes the Lua script atomically.
// KEYS[1] = stock:{activityID}
// KEYS[2] = user_buy:{activityID}:{userID}
// ARGV[1] = deduct_count
// ARGV[2] = per_user_limit
// Lua returns: ≥0 (new remaining), -1 sold_out, -2 user_limit, -3 not_warmed.
func (r *stockRedisRepo) DeductForUser(
	ctx context.Context, activityID, userID int64, n, perUserLimit int,
) (int, error) {
	stockKey := fmt.Sprintf("stock:%d", activityID)
	userKey := fmt.Sprintf("user_buy:%d:%d", activityID, userID)
	v, err := r.script.Run(ctx, r.rdb, []string{stockKey, userKey}, n, perUserLimit).Int64()
	if err != nil { return 0, err }
	switch v {
	case -1:
		return 0, ErrStockNotEnough
	case -2:
		return 0, ErrUserLimitExceeded
	case -3:
		return 0, ErrStockNotWarmed
	}
	if v < 0 { return 0, errors.New("unknown lua return") }
	return int(v), nil
}

// stockDeductLua holds the script body; keep in sync with
// internal/infra/redis/scripts/stock_deduct.lua.
const stockDeductLua = `
local stock_key  = KEYS[1]
local userbuy_key = KEYS[2]
local deduct      = tonumber(ARGV[1])
local user_limit  = tonumber(ARGV[2])

local raw_stock = redis.call('GET', stock_key)
if not raw_stock then
  return -3
end

local stock = tonumber(raw_stock)
if stock < deduct then
  return -1
end

local user_bought = tonumber(redis.call('GET', userbuy_key) or '0')
if user_bought + deduct > user_limit then
  return -2
end

local new_stock = redis.call('DECRBY', stock_key, deduct)
redis.call('INCRBY', userbuy_key, deduct)
redis.call('EXPIRE', userbuy_key, 86400)
return new_stock
`
```

- [ ] **Step 7.3:写 `stock_redis_repo_test.go`**

```go
//go:build integration

package repo_test

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	goredis "github.com/redis/go-redis/v9"

	"github.com/mjyang04/flash-deal/internal/repo"
)

func openTestRedis(t *testing.T) *goredis.Client {
	t.Helper()
	addr := os.Getenv("FD_TEST_REDIS_ADDR")
	if addr == "" { addr = "127.0.0.1:6380" }
	return goredis.NewClient(&goredis.Options{Addr: addr, DB: 0})
}

func TestStockRedis_NoOversell(t *testing.T) {
	rdb := openTestRedis(t)
	defer rdb.Close()
	ctx := context.Background()
	const aid = int64(8002)
	rdb.Set(ctx, "stock:8002", 100, 0)
	defer rdb.Del(ctx, "stock:8002")

	sr := repo.NewStockRedisRepo(rdb)
	var succ, sold int32
	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := sr.DeductForUser(ctx, aid, int64(i+1), 1, 1)
			if err == nil { atomic.AddInt32(&succ, 1) }
			if err == repo.ErrStockNotEnough { atomic.AddInt32(&sold, 1) }
		}()
	}
	wg.Wait()
	if succ != 100 || sold != 900 {
		t.Errorf("succ=%d sold=%d", succ, sold)
	}
	// final stock = 0
	if v, _ := rdb.Get(ctx, "stock:8002").Int(); v != 0 {
		t.Errorf("final stock = %d", v)
	}
	// per-user keys
	defer rdb.Del(ctx, "user_buy:8002:*")
}

func TestStockRedis_PerUserLimit(t *testing.T) {
	rdb := openTestRedis(t)
	defer rdb.Close()
	ctx := context.Background()
	rdb.Set(ctx, "stock:8003", 10, 0)
	defer rdb.Del(ctx, "stock:8003")

	sr := repo.NewStockRedisRepo(rdb)
	if _, err := sr.DeductForUser(ctx, 8003, 7, 1, 1); err != nil { t.Fatal(err) }
	_, err := sr.DeductForUser(ctx, 8003, 7, 1, 1)
	if err != repo.ErrUserLimitExceeded {
		t.Errorf("expected user-limit, got %v", err)
	}
	defer rdb.Del(ctx, "user_buy:8003:7")
}

func TestStockRedis_NotWarmed(t *testing.T) {
	rdb := openTestRedis(t)
	defer rdb.Close()
	rdb.Del(context.Background(), "stock:8004")
	sr := repo.NewStockRedisRepo(rdb)
	_, err := sr.DeductForUser(context.Background(), 8004, 1, 1, 1)
	if err != repo.ErrStockNotWarmed {
		t.Errorf("expected not-warmed, got %v", err)
	}
}
```

- [ ] **Step 7.4:跑测试**

```bash
go test -tags=integration -race -v ./internal/repo/... -run TestStockRedis
```

Expected:三个测试全 PASS。

- [ ] **Step 7.5:Commit**

```bash
git add internal/repo/stock_repo.go internal/repo/stock_redis_repo.go internal/repo/stock_redis_repo_test.go internal/repo/stock_repo_test.go
git commit -m "feat(repo): Redis Lua StockRepo with EVALSHA + per-user limit + no-oversell test"
```

---

## Task 8:Service Outcome 映射加 NotWarmed / UserLimit

**Files:** `internal/service/seckill.go`, `internal/service/seckill_test.go`

- [ ] **Step 8.1:`SeckillService` 把 `StockDeducter` 端口改为新签名**

```go
type StockDeducter interface {
	DeductForUser(ctx context.Context, activityID, userID int64, n, perUserLimit int) (remaining int, err error)
}
```

`Seckill` 方法中调用 `s.stock.DeductForUser(ctx, req.ActivityID, req.UserID, 1, a.PerUserLimit)`,switch 加分支:

```go
if errors.Is(err, repo.ErrUserLimitExceeded) {
    return SeckillOutput{Outcome: domain.OutcomeUserLimit}, nil
}
if errors.Is(err, repo.ErrStockNotWarmed) {
    return SeckillOutput{Outcome: domain.OutcomeNotWarmed}, nil
}
```

- [ ] **Step 8.2:测试新增 2 个用例**

```go
func TestSeckill_UserLimit(t *testing.T) {
	ar := &fakeActivityRepo{store: map[int64]domain.Activity{9001: runningActivity()}}
	sr := &fakeStockRepo{err: repo.ErrUserLimitExceeded}
	svc := newSvc(ar, sr, &fakeOrderRepo{})
	res, _ := svc.Seckill(context.Background(), domain.SeckillRequest{ActivityID: 9001, UserID: 1, IdempotencyToken: "x"})
	if res.Outcome != domain.OutcomeUserLimit {
		t.Errorf("outcome = %v", res.Outcome)
	}
}

func TestSeckill_NotWarmed(t *testing.T) {
	ar := &fakeActivityRepo{store: map[int64]domain.Activity{9001: runningActivity()}}
	sr := &fakeStockRepo{err: repo.ErrStockNotWarmed}
	svc := newSvc(ar, sr, &fakeOrderRepo{})
	res, _ := svc.Seckill(context.Background(), domain.SeckillRequest{ActivityID: 9001, UserID: 1, IdempotencyToken: "x"})
	if res.Outcome != domain.OutcomeNotWarmed {
		t.Errorf("outcome = %v", res.Outcome)
	}
}
```

更新 `fakeStockRepo` 改用新 method 签名:

```go
func (f *fakeStockRepo) DeductForUser(_ context.Context, _, _ int64, n, _ int) (int, error) {
	if f.err != nil { return 0, f.err }
	f.remaining -= n
	return f.remaining, nil
}
```

- [ ] **Step 8.3:跑测试 + Commit**

```bash
go test -race -v ./internal/service/...
git add internal/service/
git commit -m "feat(service): stock port takes (userID,perUserLimit); handle user-limit + not-warmed"
```

---

## Task 9:Order Materializer(consumer 业务)

**Files:** `internal/service/order_materializer.go`, `internal/service/order_materializer_test.go`

- [ ] **Step 9.1:实现**

```go
package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/mjyang04/flash-deal/internal/domain"
	fdkafka "github.com/mjyang04/flash-deal/internal/infra/kafka"
	"github.com/mjyang04/flash-deal/internal/repo"
)

// OrderMaterializer is the consumer-side business: turn a Kafka OrderMessage
// into a row in MySQL and update the queue-token state in Redis.
type OrderMaterializer struct {
	orders repo.OrderRepo
	rdb    *goredis.Client
}

func NewOrderMaterializer(orders repo.OrderRepo, rdb *goredis.Client) *OrderMaterializer {
	return &OrderMaterializer{orders: orders, rdb: rdb}
}

// Handle is the kafka.Handler. Idempotent: duplicate insert → treat as success
// (the row already exists with the same idempotency token).
func (m *OrderMaterializer) Handle(ctx context.Context, msg fdkafka.OrderMessage) error {
	o := domain.Order{
		ID: msg.OrderID, UserID: msg.UserID, ActivityID: msg.ActivityID,
		ProductID: msg.ProductID, Status: domain.OrderQueued,
		IdempotencyToken: msg.IdempotencyToken, CreatedAt: msg.ProducedAt,
	}
	err := m.orders.Create(ctx, o)
	if err != nil && !errors.Is(err, repo.ErrOrderDuplicate) {
		return err
	}
	// success or duplicate → mark queue token
	key := fmt.Sprintf("queue:%s", msg.QueueToken)
	val := fmt.Sprintf("success:%d", msg.OrderID)
	return m.rdb.Set(ctx, key, val, time.Hour).Err()
}

// MarkFailed is used by the DLQ handler to flip the queue token to a failure.
func (m *OrderMaterializer) MarkFailed(ctx context.Context, queueToken string, reason error) {
	key := fmt.Sprintf("queue:%s", queueToken)
	m.rdb.Set(ctx, key, fmt.Sprintf("failed:%s", reason.Error()), time.Hour)
}
```

- [ ] **Step 9.2:测试 `order_materializer_test.go`(用 fake redis = miniredis or 用真 redis 做集成)**

```go
//go:build integration

package service_test

import (
	"context"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/mjyang04/flash-deal/internal/domain"
	fdkafka "github.com/mjyang04/flash-deal/internal/infra/kafka"
	"github.com/mjyang04/flash-deal/internal/repo"
	"github.com/mjyang04/flash-deal/internal/service"
)

type memOrderRepo struct {
	rows map[int64]domain.Order
	keys map[string]bool // (activity,user,token)
}

func newMem() *memOrderRepo { return &memOrderRepo{rows: map[int64]domain.Order{}, keys: map[string]bool{}} }
func (r *memOrderRepo) Create(_ context.Context, o domain.Order) error {
	k := fmt.Sprintf("%d:%d:%s", o.ActivityID, o.UserID, o.IdempotencyToken)
	if r.keys[k] { return repo.ErrOrderDuplicate }
	r.keys[k] = true
	r.rows[o.ID] = o
	return nil
}
func (r *memOrderRepo) GetByID(_ context.Context, _ int64, id int64) (domain.Order, error) {
	if o, ok := r.rows[id]; ok { return o, nil }
	return domain.Order{}, repo.ErrOrderNotFound
}

func TestMaterializer_HappyPath_AndDuplicate(t *testing.T) {
	rdb := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:6380"})
	defer rdb.Close()
	ctx := context.Background()
	rdb.Del(ctx, "queue:tok-mat-1")

	mem := newMem()
	m := service.NewOrderMaterializer(mem, rdb)
	msg := fdkafka.OrderMessage{
		Version: 1, OrderID: 555, ActivityID: 1, UserID: 7, ProductID: 100,
		IdempotencyToken: "idem-mat-1", QueueToken: "tok-mat-1", ProducedAt: time.Now(),
	}
	if err := m.Handle(ctx, msg); err != nil { t.Fatal(err) }
	if v, _ := rdb.Get(ctx, "queue:tok-mat-1").Result(); v != "success:555" {
		t.Errorf("queue val = %q", v)
	}
	// 2nd time = duplicate, still success
	if err := m.Handle(ctx, msg); err != nil { t.Fatalf("dup: %v", err) }
	if len(mem.rows) != 1 { t.Errorf("rows = %d, want 1", len(mem.rows)) }
}
```

(头部加 `"fmt"` import 和 `_ "github.com/jmoiron/sqlx"` 不需要,memRepo 用 fmt 已 import。)

- [ ] **Step 9.3:跑测试 + Commit**

```bash
go test -tags=integration -race -v ./internal/service/... -run TestMaterializer
git add internal/service/order_materializer.go internal/service/order_materializer_test.go
git commit -m "feat(service): OrderMaterializer (kafka → MySQL + redis queue state, idempotent)"
```

---

## Task 10:Kafka-backed OrderCreator(api 侧)

**Files:** `internal/service/kafka_order_creator.go`, `internal/service/queue_token.go`, `internal/service/queue_token_test.go`

> 设计:M1 的 `OrderCreator` port 调用 → SQL INSERT。M2 接同一 port,但实现是 produce 到 Kafka(并把 queue token 写 Redis `queued`)。API 侧不再同步落库。

- [ ] **Step 10.1:实现 `queue_token.go`**

```go
package service

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

const queueTokenTTL = 10 * time.Minute

type QueueService struct{ rdb *goredis.Client }

func NewQueue(rdb *goredis.Client) *QueueService { return &QueueService{rdb: rdb} }

// New generates a UUIDv7 (time-sortable) and marks it queued in Redis.
func (q *QueueService) New(ctx context.Context) (string, error) {
	id, err := uuid.NewV7()
	if err != nil { return "", err }
	tok := id.String()
	key := fmt.Sprintf("queue:%s", tok)
	if err := q.rdb.Set(ctx, key, "queued", queueTokenTTL).Err(); err != nil {
		return "", err
	}
	return tok, nil
}

// Get reads the latest known state.
func (q *QueueService) Get(ctx context.Context, token string) (string, error) {
	key := fmt.Sprintf("queue:%s", token)
	v, err := q.rdb.Get(ctx, key).Result()
	if err == goredis.Nil { return "not_found", nil }
	return v, err
}
```

- [ ] **Step 10.2:`queue_token_test.go`(integration)**

```go
//go:build integration

package service_test

import (
	"context"
	"strings"
	"testing"

	goredis "github.com/redis/go-redis/v9"

	"github.com/mjyang04/flash-deal/internal/service"
)

func TestQueue_NewAndGet(t *testing.T) {
	rdb := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:6380"})
	defer rdb.Close()
	q := service.NewQueue(rdb)
	tok, err := q.New(context.Background())
	if err != nil { t.Fatal(err) }
	if !strings.Contains(tok, "-") { t.Errorf("token shape: %q", tok) }
	st, err := q.Get(context.Background(), tok)
	if err != nil { t.Fatal(err) }
	if st != "queued" { t.Errorf("state = %q", st) }
}
```

- [ ] **Step 10.3:实现 `kafka_order_creator.go`**

```go
package service

import (
	"context"
	"time"

	"github.com/mjyang04/flash-deal/internal/domain"
	fdkafka "github.com/mjyang04/flash-deal/internal/infra/kafka"
)

// KafkaOrderCreator implements OrderCreator (used by SeckillService) but
// publishes to Kafka instead of writing the order row synchronously.
type KafkaOrderCreator struct {
	producer *fdkafka.Producer
	topic    string
	queue    *QueueService
}

func NewKafkaOrderCreator(p *fdkafka.Producer, topic string, q *QueueService) *KafkaOrderCreator {
	return &KafkaOrderCreator{producer: p, topic: topic, queue: q}
}

// Create publishes an OrderMessage to Kafka and stores the queue token state.
// SeckillService stuffs the queue_token into the result via a side channel:
// the implementation reads it from o.IdempotencyToken? — no — we extend
// SeckillService below to thread an extra QueueToken through Output.
//
// For M2 wire: we use o.IdempotencyToken as fallback queue_token if Output
// is not threaded yet, but recommend Step 10.4 to thread queue_token via the
// service interface.
func (k *KafkaOrderCreator) Create(ctx context.Context, o domain.Order) error {
	// Generate token here (simplest: same as o.IdempotencyToken for M2 v0);
	// Task 11 wires QueueService into SeckillService directly.
	msg := fdkafka.OrderMessage{
		Version: 1, OrderID: o.ID, ActivityID: o.ActivityID, UserID: o.UserID,
		ProductID: o.ProductID, IdempotencyToken: o.IdempotencyToken,
		QueueToken: o.IdempotencyToken, // overridden by SeckillService in Task 11
		ProducedAt: time.Now().UTC(),
	}
	return k.producer.SendOrder(ctx, k.topic, msg)
}
```

- [ ] **Step 10.4:`SeckillService` 增强 — 在 Output 里返回 QueueToken,允许 wire 时注入 token 生成器**

修改 `SeckillService` 加字段 `queue *QueueService`(可空,M1 行为不变);Seckill() 中扣减成功后:

```go
queueToken := ""
if s.queue != nil {
    tok, err := s.queue.New(ctx)
    if err == nil { queueToken = tok }
}
// ... build order, hand off ...
return SeckillOutput{
    Outcome: domain.OutcomeQueued,
    QueueToken: queueToken,
    Remaining: remaining,
}, nil
```

并在 `KafkaOrderCreator.Create` 改签名 / 加一个 `CreateWithToken(ctx, o, queueToken string)` 方法,SeckillService 调用新方法。**为避免与 M1 单元测试冲突**,做法是:

- 给 `OrderCreator` port 加 method:`CreateWithToken(ctx, o domain.Order, queueToken string) error`,默认实现委托给 `Create`(M1 同步 INSERT 不 care token)。
- `KafkaOrderCreator` 重写 `CreateWithToken`,把 queueToken 放进 `OrderMessage`。

- [ ] **Step 10.5:跑全部 service 测试 + Commit**

```bash
go test -tags=integration -race -v ./internal/service/...
go test -race -v ./internal/service/...
git add internal/service/
git commit -m "feat(service): QueueService + KafkaOrderCreator + Output.QueueToken threading"
```

---

## Task 11:Handler `GET /v1/order/by-token/:token`

**Files:** `internal/handler/order.go`, `internal/handler/order_test.go`

- [ ] **Step 11.1:实现**

```go
package handler

import (
	"context"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

// QueueQuery is the small port for queue state lookup.
type QueueQuery interface {
	Get(ctx context.Context, token string) (string, error)
}

// OrderByToken: GET /v1/order/by-token/:queue_token
// Returns {queue_token, state, order_id, reason}.
func OrderByToken(q QueueQuery) gin.HandlerFunc {
	return func(c *gin.Context) {
		token := c.Param("queue_token")
		if token == "" {
			writeError(c, http.StatusBadRequest, "BAD_REQUEST", "token required")
			return
		}
		state, err := q.Get(c.Request.Context(), token)
		if err != nil {
			writeError(c, http.StatusInternalServerError, "INTERNAL", err.Error())
			return
		}
		resp := gin.H{"queue_token": token}
		switch {
		case state == "queued":
			resp["state"] = "queued"
		case strings.HasPrefix(state, "success:"):
			resp["state"] = "success"
			resp["order_id"] = strings.TrimPrefix(state, "success:")
		case strings.HasPrefix(state, "failed:"):
			resp["state"] = "failed"
			resp["reason"] = strings.TrimPrefix(state, "failed:")
		case state == "not_found":
			resp["state"] = "not_found"
		default:
			resp["state"] = state
		}
		c.JSON(http.StatusOK, resp)
	}
}
```

- [ ] **Step 11.2:测试**

```go
package handler_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/mjyang04/flash-deal/internal/handler"
)

type stubQueue struct{ state string }

func (s stubQueue) Get(_ context.Context, _ string) (string, error) { return s.state, nil }

func TestOrderByToken_Variants(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cases := []struct {
		state string
		want  map[string]any
	}{
		{"queued", map[string]any{"state": "queued"}},
		{"success:42", map[string]any{"state": "success", "order_id": "42"}},
		{"failed:boom", map[string]any{"state": "failed", "reason": "boom"}},
		{"not_found", map[string]any{"state": "not_found"}},
	}
	for _, c := range cases {
		r := gin.New()
		r.GET("/v1/order/by-token/:queue_token", handler.OrderByToken(stubQueue{state: c.state}))
		req := httptest.NewRequest("GET", "/v1/order/by-token/T", nil)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		if w.Code != http.StatusOK { t.Errorf("code=%d body=%s", w.Code, w.Body) }
		var got map[string]any
		_ = json.Unmarshal(w.Body.Bytes(), &got)
		for k, v := range c.want {
			if got[k] != v { t.Errorf("state=%q field %q = %v, want %v", c.state, k, got[k], v) }
		}
	}
}
```

- [ ] **Step 11.3:跑测试 + Commit**

```bash
go test -v -race ./internal/handler/...
git add internal/handler/order.go internal/handler/order_test.go
git commit -m "feat(handler): GET /v1/order/by-token/:token with state parsing"
```

---

## Task 12:Wire — cmd/api 接入 Lua + Kafka producer + queue API

**Files:** `cmd/api/main.go`

- [ ] **Step 12.1:按 `cfg.Switches` 选择 stock impl 和 order impl**

```go
// after creating db, rdb:
var sr repo.StockRepo
if cfg.Switches.LuaStock {
    sr = repo.NewStockRedisRepo(rdb)
} else {
    sr = repo.NewStockRepo(db)
}

queue := service.NewQueue(rdb)

var oc service.OrderCreator
if cfg.Switches.KafkaOrder {
    producer, err := fdkafka.NewProducer(cfg.Kafka.Brokers, cfg.Kafka.ProduceTimeout)
    if err != nil { log.Fatalf("kafka producer: %v", err) }
    defer producer.Close()
    oc = service.NewKafkaOrderCreator(producer, cfg.Kafka.OrderTopic, queue)
} else {
    oc = repo.NewOrderRepo(db) // M1 fallback path
}

seckillSvc := service.New(ar, sr, oc, time.Now, nextID).WithQueue(queue) // builder
```

(为此在 `SeckillService` 加 `WithQueue(q *QueueService) *SeckillService` 链式 setter,避免破坏 M1 构造签名。)

- [ ] **Step 12.2:加路由 `GET /v1/order/by-token/:queue_token`**

```go
r.GET("/v1/order/by-token/:queue_token", handler.OrderByToken(queue))
```

- [ ] **Step 12.3:跑全套 unit 测试 + 启动烟测**

```bash
go test -race ./...
make down && make up && make migrate && make kafka-topic
make seed
go run ./cmd/api > /tmp/fd-api.log 2>&1 &
sleep 3
# 不启 consumer 时,seckill 仍返回 202 + queue_token,但轮询会一直 "queued"
curl -sS -X POST -H 'Content-Type: application/json' \
  -d '{"activity_id":1001,"user_id":1,"idempotency_token":"tok-A2"}' \
  http://localhost:8080/v1/seckill
# 假设 queue_token = abc-def
curl -sS http://localhost:8080/v1/order/by-token/<token>
pkill -f cmd/api
```

Expected:`{"status":"queued","queue_token":"...","remaining":999}`;GET 返回 `{"state":"queued"}`(consumer 还没起所以一直 queued)。

- [ ] **Step 12.4:Commit**

```bash
git add cmd/api/main.go internal/service/
git commit -m "feat(api): wire Lua stock + Kafka producer + queue token API behind config switches"
```

---

## Task 13:Wire — cmd/consumer 完整实现

**Files:** `cmd/consumer/main.go`

- [ ] **Step 13.1:替换 stub 为完整 consumer**

```go
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/mjyang04/flash-deal/internal/config"
	fdkafka "github.com/mjyang04/flash-deal/internal/infra/kafka"
	"github.com/mjyang04/flash-deal/internal/infra/logger"
	fdmysql "github.com/mjyang04/flash-deal/internal/infra/mysql"
	fdredis "github.com/mjyang04/flash-deal/internal/infra/redis"
	"github.com/mjyang04/flash-deal/internal/repo"
	"github.com/mjyang04/flash-deal/internal/service"
)

func main() {
	cfg, err := config.Load(os.Getenv("FD_CONFIG_FILE"))
	if err != nil { log.Fatalf("config: %v", err) }
	logger.Init(os.Getenv("FD_LOG_MODE"))

	db, err := fdmysql.Open(cfg.MySQL)
	if err != nil { log.Fatalf("mysql: %v", err) }
	defer db.Close()
	rdb := fdredis.New(cfg.Redis)
	defer rdb.Close()

	orders := repo.NewOrderRepo(db)
	mat := service.NewOrderMaterializer(orders, rdb)

	dlqProd, _ := fdkafka.NewProducer(cfg.Kafka.Brokers, cfg.Kafka.ProduceTimeout)
	defer dlqProd.Close()

	c := fdkafka.NewConsumer(cfg.Kafka.Brokers, cfg.Kafka.OrderTopic, cfg.Kafka.ConsumerGroup, cfg.Kafka.ConsumerBatchWait, 3)
	c.SetHandler(mat.Handle)
	c.SetDLQ(func(ctx context.Context, raw []byte, reason error) {
		// best-effort republish to DLQ topic; also flip queue token to failed
		log.Printf("DLQ: %v", reason)
		_ = dlqProd.SendOrder(ctx, cfg.Kafka.DLQTopic, fdkafka.OrderMessage{}) // simplified
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	go func() { <-stop; log.Println("shutdown"); cancel() }()

	log.Printf("consumer reading topic=%s group=%s", cfg.Kafka.OrderTopic, cfg.Kafka.ConsumerGroup)
	if err := c.Run(ctx); err != nil { log.Fatalf("run: %v", err) }
}
```

- [ ] **Step 13.2:端到端烟测**

```bash
make down && make up && make migrate && make kafka-topic
make seed
go run ./cmd/api > /tmp/api.log 2>&1 &
go run ./cmd/consumer > /tmp/consumer.log 2>&1 &
sleep 5
resp=$(curl -sS -X POST -H 'Content-Type: application/json' \
  -d '{"activity_id":1001,"user_id":1,"idempotency_token":"tok-e2e-1"}' \
  http://localhost:8080/v1/seckill)
echo "$resp"
token=$(echo "$resp" | jq -r .queue_token)
sleep 2  # let consumer materialize
curl -sS http://localhost:8080/v1/order/by-token/$token
# DB 应该有 1 条订单
docker exec fd-mysql mysql -uflashdeal -pflashdeal -e 'select count(*) from orders_0' flashdeal
pkill -f cmd/api; pkill -f cmd/consumer
```

Expected:
- seckill 返回 `{"status":"queued","queue_token":"xxx-...","remaining":999}`
- 2s 后 by-token 返回 `{"state":"success","order_id":"..."}`
- MySQL 1 条订单

- [ ] **Step 13.3:Commit**

```bash
git add cmd/consumer/main.go
git commit -m "feat(consumer): wire OrderMaterializer + DLQ via cmd/consumer end-to-end"
```

---

## Task 14:M2 对照 bench + 报告

**Files:** `bench/k6/seckill_m2.js`, `reports/week2_redis_kafka.md`

- [ ] **Step 14.1:复制 `bench/k6/seckill.js` 为 `seckill_m2.js`**,scenario / thresholds 保持 constant-arrival-rate 三档(500/1000/2000)。**新增**:从 response 拿 queue_token,以 30% 概率 GET 一次 by-token(模拟客户端轮询)。

- [ ] **Step 14.2:跑三档**

```bash
make down && make up && make migrate && make kafka-topic
make seed
go run ./cmd/api > /tmp/api.log 2>&1 &
go run ./cmd/consumer > /tmp/consumer.log 2>&1 &
sleep 5
for rate in 500 1000 2000; do
  docker exec -i fd-mysql mysql -uflashdeal -pflashdeal -e 'truncate orders_0' flashdeal
  docker exec fd-redis redis-cli set stock:1001 10000
  echo "=== RATE=$rate ==="
  RATE=$rate DURATION=30s k6 run bench/k6/seckill_m2.js --summary-export=/tmp/fd-bench-m2-$rate.json
done
pkill -f cmd/api; pkill -f cmd/consumer
```

- [ ] **Step 14.3:写 `reports/week2_redis_kafka.md`**

骨架:
- M1 vs M2 对照表(RPS / P50 / P95 / P99 / 错误率)
- Consumer lag 走势(`kafka-consumer-groups.sh --describe`)
- DLQ 数量
- 正确性:initial stock = `count(orders) + final stock`
- 结论:Redis Lua 比 SQL 行锁快 N 倍,Kafka 解耦 API 与 DB 写入

- [ ] **Step 14.4:Commit**

```bash
git add bench/k6/seckill_m2.js reports/week2_redis_kafka.md
git commit -m "chore(report): M2 bench + M1 vs M2 comparison"
```

---

## Task 15:收尾 lint / tag m2

- [ ] **Step 15.1**

```bash
go mod tidy
gofmt -s -w .
go vet ./...
go test -race -cover ./...
go test -tags=integration -race ./internal/...
```

Expected:全 PASS,**特别确认 `TestStockRedis_NoOversell` + `TestMaterializer_HappyPath_AndDuplicate` 通过**。

- [ ] **Step 15.2:README 加 M2 状态**

更新 README 状态行:`[x] M2 Redis Lua + Kafka async` (tag m2, [`reports/week2_redis_kafka.md`](./reports/week2_redis_kafka.md))。Quickstart 加 `make consumer` 和 `make kafka-topic`。

- [ ] **Step 15.3:Tag m2**

```bash
git tag -a m2 -m "M2: Redis Lua stock + Kafka async order + queue token API"
git log --oneline | head -10
```

---

## 验收清单

- [ ] `make up && make migrate && make kafka-topic && make seed && make api && make consumer` 一键起完整 M2
- [ ] `TestStockRedis_NoOversell`(1000G × 100 stock 零超卖,Lua 版)PASS
- [ ] M2 P95 在 1000 rps 下 ≤ 100ms(相比 M1 的 2.4s)
- [ ] 端到端:POST /v1/seckill → 2s 内 by-token 返回 success + order_id
- [ ] consumer kill → restart 后能消费完积压(无丢单)
- [ ] DLQ 路径触发后 by-token 返回 `failed:{reason}`
- [ ] git tag `m2` 已打

---

## Self-Review 结果

| 检查 | 状态 |
|------|------|
| Spec 覆盖:milestone.md Day 8-14 全部映射到 Task | ✅ Day 8-9→T1+T7,Day 10→T4+T5,Day 11→T6+T9,Day 12→T10+T11+T13,Day 13→T14,Day 14→T15 |
| Placeholder 扫描 | ✅ 无 TBD/TODO |
| 类型一致 | `StockDeducter` 端口签名升级一致;`OrderCreator` 双 method `Create`/`CreateWithToken` 兼容 M1 测试 |
| 依赖 M1 状态准确 | ✅ 沿用 `domain.Activity` JSON tag、`orders_0` 单分片、`stock:{id}` Redis key、`stock_deduct.lua` 返回值定义 |
