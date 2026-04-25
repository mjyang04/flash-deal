# flash-deal — 技术设计

## 1. 架构总览

```
                    ┌────────────┐
        client ────▶│  Nginx LB  │
                    └─────┬──────┘
                          │
        ┌─────────────────┼─────────────────┐
        ▼                 ▼                 ▼
  ┌──────────┐      ┌──────────┐      ┌──────────┐
  │  api-1   │      │  api-2   │      │  api-3   │  Gin + middleware
  └────┬─────┘      └────┬─────┘      └────┬─────┘
       │                 │                 │
       └─────────────────┼─────────────────┘
                         │
        ┌────────────────┼────────────────┐
        ▼                ▼                ▼
   ┌─────────┐     ┌──────────┐    ┌──────────┐
   │  Redis  │     │  Kafka   │    │  MySQL   │
   │ Cluster │     │ (3 brks) │    │ (4 shrd) │
   └─────────┘     └────┬─────┘    └──────────┘
   stock + lua          │                ▲
                        ▼                │
                  ┌──────────┐           │
                  │ consumer │───────────┘
                  └──────────┘  写订单
```

## 2. 核心链路:下单

```
客户端
  │ POST /v1/seckill {activity_id, user_id, idempotency_token}
  ▼
[middleware]
  ├─ trace 注入
  ├─ rate-limit 用户 + 全局
  ├─ auth(JWT)
  └─ idempotency 检查(Redis SETNX)
  │
  ▼
[handler]
  │
  ▼
[service.Seckill]
  ├─ 1. 校验活动状态(本地缓存,TTL 1s)
  ├─ 2. 调用 Redis Lua 扣库存
  │     ├─ 成功 → 进入第 3 步
  │     └─ 失败 → 返回 "sold_out" 或 "not_started"
  ├─ 3. 投递 Kafka(主题: seckill_orders)
  ├─ 4. 写排队状态到 Redis(token → "queued")
  └─ 5. 返回 {status:"queued", token:"..."}
  │
  ▼
异步:[consumer]
  ├─ 拉取 Kafka 消息
  ├─ 写订单(分库分表路由)
  ├─ 更新 Redis 排队状态(token → "success" / "failed")
  └─ ACK
```

## 3. 关键技术点

### 3.1 Redis Lua 库存扣减
```lua
-- KEYS[1]: stock_key  ARGV[1]: deduct_count  ARGV[2]: max_per_user
-- KEYS[2]: user_buy_key  user 已买记录
local stock = tonumber(redis.call('GET', KEYS[1]))
if not stock or stock < tonumber(ARGV[1]) then
  return -1   -- sold out
end

local user_bought = tonumber(redis.call('GET', KEYS[2]) or '0')
if user_bought >= tonumber(ARGV[2]) then
  return -2   -- exceed limit
end

local new_stock = redis.call('DECRBY', KEYS[1], ARGV[1])
redis.call('INCRBY', KEYS[2], ARGV[1])
redis.call('EXPIRE', KEYS[2], 86400)
return new_stock
```
**关键点**:
- 单次 Lua 是原子的,不会出现并发竞争
- 返回值 -1/-2 区分错误类型
- user_buy_key 双重保险 + 限购

### 3.2 幂等设计
两层防护:
1. **请求层**:`idempotency_token` 走 `SETNX token "processing" EX 60`,占位失败说明已处理
2. **数据库层**:`orders` 表 `(activity_id, user_id, idempotency_token)` 唯一索引

### 3.3 分库分表
- 4 个 MySQL 实例(本地 docker)或同实例 4 库
- 路由:`db_idx = user_id % 4`
- 表内分表(可选):`tb_idx = (user_id / 4) % 16`
- 跨片查询走 ES / Hbase(本项目用 ES 简化)

```go
// pkg/shardkey
func ShardDBIndex(userID int64, dbCount int) int {
    return int(userID % int64(dbCount))
}
```

### 3.4 限流
- **本地令牌桶**:`golang.org/x/time/rate`(全局 / 实例级)
- **分布式令牌桶**:Redis + Lua(用户级跨实例)
- **熔断**:`sony/gobreaker`,阈值:错误率 50% / 60s 窗口

### 3.5 Kafka 配置要点
- partition 数 = 消费者并发数 × N
- producer:`acks=all`、`max.in.flight=1`(保证有序)
- consumer:`enable.auto.commit=false`(手动 ACK)
- 死信 topic:`seckill_orders_dlq`

### 3.6 缓存策略
| 数据 | 缓存策略 | TTL |
|------|---------|-----|
| 活动元信息 | 本地 + Redis | 5s 本地 / 60s Redis |
| 库存 | Redis(权威) | 活动结束清理 |
| 用户限购 | Redis | 24h |
| 订单详情 | Redis(写穿透) | 1h |

## 4. 数据库设计

### activities
```sql
CREATE TABLE activities (
  id BIGINT PRIMARY KEY,
  product_id BIGINT NOT NULL,
  total_stock INT NOT NULL,
  start_at DATETIME NOT NULL,
  end_at DATETIME NOT NULL,
  per_user_limit INT DEFAULT 1,
  status TINYINT DEFAULT 0,
  created_at DATETIME,
  updated_at DATETIME
);
```

### orders(分片表)
```sql
CREATE TABLE orders_{N} (
  id BIGINT PRIMARY KEY,
  user_id BIGINT NOT NULL,
  activity_id BIGINT NOT NULL,
  product_id BIGINT NOT NULL,
  status TINYINT DEFAULT 0,
  idempotency_token VARCHAR(64) NOT NULL,
  created_at DATETIME,
  UNIQUE KEY uk_idem (activity_id, user_id, idempotency_token),
  KEY idx_user (user_id, created_at)
);
```

### ID 生成
- snowflake(自实现或 sony/sonyflake)
- workerID 从 hostname 派生

## 5. 监控指标

### 业务指标
- `seckill_request_total{activity_id, status}`
- `seckill_stock_remain{activity_id}`
- `seckill_order_created_total`

### 系统指标
- `http_request_duration_seconds`
- `redis_command_duration_seconds`
- `mysql_query_duration_seconds`
- `kafka_produce_duration_seconds`

### Grafana dashboard
- 实时 QPS / 成功率 / P99
- 库存剩余曲线
- 各中间件健康度

## 6. 测试策略

### 单元测试
- domain 层:100% 覆盖
- service 层:核心路径 + 边界 case

### 集成测试
- testcontainers-go 起 Redis / MySQL
- 端到端下单流程

### 并发正确性测试
```go
// 1000 协程抢 100 库存,验证最终库存 = 0,订单数 = 100
func TestSeckill_NoOversell(t *testing.T) { ... }
```

### 压测
- k6 脚本:`bench/k6/seckill.js`
- 多机分布式压测(若资源允许)

## 7. 部署

### Docker Compose(开发)
- mysql × 4(分片)
- redis × 1(开发)/ cluster(生产模拟)
- kafka × 3 + zookeeper / kraft
- jaeger + prometheus + grafana

### K8s(Stretch)
- Deployment for api / consumer
- HPA based on CPU + custom metric(QPS)
- Service + Ingress

## 8. 风险与应对

| 风险 | 应对 |
|------|------|
| Redis 单点 | sentinel / cluster |
| Kafka 堆积 | 监控 lag,自动扩容 consumer |
| MySQL 写入瓶颈 | 分库分表 + 异步 + 批量 |
| 热点 key | 本地缓存 + key 拆分 |
| GC 抖动 | GOMEMLIMIT + sync.Pool |

## 9. 不做什么(明确边界)

- 不做完整电商前端(只给最小管理 UI)
- 不做支付真实集成(用 mock,3s 后随机成功/失败)
- 不做营销 / 优惠券 / 积分
- 不做物流 / 售后
- 不重复造轮子(能用成熟库就用,精力放在架构与 trade-off)
