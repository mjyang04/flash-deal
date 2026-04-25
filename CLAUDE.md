# flash-deal

## 项目目标
实现一个**生产级 Go 高并发秒杀系统**,覆盖大厂后端面试核心知识点:Redis 原子操作、Kafka 异步削峰、分库分表、限流熔断、幂等设计、压测验证。

## 核心定位
- **目标岗位**:互联网大厂后端(阿里 / 字节 / 美团 / 腾讯 / 拼多多)
- **简历区分点**:不是"做过电商 demo",而是"独立设计高并发秒杀,有压测数据,QPS 10W+"
- **补 FoodOrderSystem 短板**:那个是 C++ 后端(国内招聘窄),这个是 Go(主流)

## 技术栈
- **语言**:Go 1.22+
- **Web 框架**:Gin
- **缓存**:Redis 7(+ Lua 脚本原子化)
- **消息队列**:Kafka(削峰异步下单)
- **数据库**:MySQL 8.0(分库分表 via sharding-jdbc 思想 / 自实现路由)
- **限流**:sentinel-golang / 自研 token bucket
- **熔断**:hystrix-go / sony/gobreaker
- **链路追踪**:OpenTelemetry + Jaeger
- **监控**:Prometheus + Grafana
- **容器化**:Docker + docker-compose,可选 K8s
- **压测**:k6 / wrk / vegeta

## 目录结构
```
flash-deal/
├── CLAUDE.md
├── plan/
│   ├── PRD.md
│   ├── milestone.md
│   └── tech_design.md
├── cmd/
│   ├── api/                # HTTP API 入口
│   ├── consumer/           # Kafka 消费者
│   └── worker/             # 异步任务 worker
├── internal/
│   ├── domain/             # 领域模型(User, Product, Order, Stock)
│   ├── service/            # 业务逻辑
│   ├── repo/               # 数据访问层(MySQL + Redis)
│   ├── handler/            # HTTP handler
│   ├── middleware/         # 限流 / 鉴权 / trace
│   └── infra/              # 基础设施(redis client, kafka, db, otel)
├── pkg/
│   ├── shardkey/           # 分库分表路由
│   └── ratelimit/
├── deploy/
│   ├── docker-compose.yml  # 一键起 mysql/redis/kafka/jaeger/prom/grafana
│   └── k8s/                # K8s manifests(Stretch goal)
├── bench/
│   ├── k6/                 # k6 压测脚本
│   └── reports/            # 压测报告
├── migrations/             # SQL 迁移
└── go.mod
```

## 核心约束
1. **正确性 > 性能**:不能超卖、不能少卖、不能重复下单
2. **幂等**:用户点 N 次只生成 1 个订单(token + 唯一索引双保险)
3. **可压测**:有完整 k6 脚本 + 真实压测数据 + Grafana 截图
4. **遵循 Go 习惯**:接口隔离、依赖注入、context 传递、错误显式处理

## 关键场景
- **下单接口** `POST /v1/seckill`:
  1. 限流(用户级 + 全局)
  2. 校验秒杀活动状态
  3. Redis Lua 原子扣减库存
  4. 发送 Kafka 异步落单消息
  5. 返回排队 token
- **查询订单状态** `GET /v1/order/:id`(结果一致性)
- **管理后台**:活动配置、库存预热

## 关键里程碑
- M1(Week 1):MVP 单机版下单流程跑通
- M2(Week 2):Redis Lua + Kafka 异步化
- M3(Week 3):分库分表 + 限流熔断 + 监控
- M4(Week 4):压测 + 优化 + 报告 + 博客

## 产出物
- [ ] GitHub repo with `make up` 一键起整个系统
- [ ] 压测报告:单机 QPS、集群 QPS、P99 延迟、库存正确性验证
- [ ] 架构图(mermaid + 真实 trace 截图)
- [ ] Grafana dashboard
- [ ] 技术博客:《从零到 10W QPS:Go 秒杀系统全链路拆解》

## 与其他项目的关系
- **完全独立于 FoodOrderSystem 和 LLM 项目**,补后端短板
- 复用 OTel + Prometheus + Grafana 监控栈

## 运行约定
- 启动全栈:`make up`(docker-compose up -d)
- 启动 API:`go run ./cmd/api`
- 启动 Consumer:`go run ./cmd/consumer`
- 跑压测:`k6 run bench/k6/seckill.js`
- 数据迁移:`make migrate-up`
