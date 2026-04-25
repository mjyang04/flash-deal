# flash-deal — 4 周里程碑

## Week 1:MVP 单机版

### Day 1-2:工程脚手架
- [ ] `go mod init github.com/yourname/flash-deal`
- [ ] 目录结构按 CLAUDE.md 创建
- [ ] `deploy/docker-compose.yml`:MySQL + Redis + Kafka + Jaeger + Prometheus + Grafana
- [ ] `Makefile`:up / down / migrate / test / bench
- [ ] `.golangci.yml` + `pre-commit` 配置

### Day 3-4:领域模型 + 数据层
- [ ] domain:User、Activity、Product、Order、Stock
- [ ] migrations:`activities`, `orders` 表(orders 先单库)
- [ ] repo 接口 + sqlx/sqlc 实现
- [ ] Redis client wrapper

### Day 5-7:下单接口 v0.1
- [ ] `POST /v1/seckill`:同步扣库存 + 同步落单(纯 SQL,先不上 Lua/Kafka)
- [ ] 单元测试:正常下单、库存不足、活动未开始、幂等
- [ ] `bench/k6/seckill_v0.js` baseline 压测
- [ ] **里程碑产出**:`reports/week1_mvp.md`(baseline QPS 数据)

## Week 2:Redis Lua + Kafka 异步化

### Day 8-9:Redis Lua 库存
- [ ] 编写 `stock_deduct.lua`(原子检查 + 扣减 + 返回剩余)
- [ ] 库存预热脚本(活动开始前同步 MySQL → Redis)
- [ ] 并发测试:1000 协程抢 100 库存,验证零超卖

### Day 10-12:Kafka 异步落单
- [ ] producer:扣库存成功后投递 Kafka
- [ ] `cmd/consumer`:消费 + 写订单 + ACK
- [ ] 死信队列 + 重试策略
- [ ] 排队 token 设计(uuid + Redis 状态)

### Day 13-14:压测与对比
- [ ] 重跑 k6 压测
- [ ] 对比 v0.1 vs v0.2 QPS / P99
- [ ] **里程碑产出**:`reports/week2_redis_kafka.md` + 对比图

## Week 3:分库分表 + 限流熔断 + 监控

### Day 15-16:分库分表
- [ ] 订单表按 `user_id % N` 路由(N=4)
- [ ] `pkg/shardkey` 路由层
- [ ] 跨分片查询封装
- [ ] migration 拆分 + 测试

### Day 17-18:限流熔断 + 幂等
- [ ] 用户级令牌桶(Redis-based)
- [ ] 全局限流(本地令牌桶)
- [ ] gobreaker 熔断(下游 MySQL 异常时切流)
- [ ] 幂等中间件:idempotency_token + Redis SETNX

### Day 19-21:监控全链路
- [ ] OTel 接入:gateway → service → repo 全链路 trace
- [ ] Prometheus metrics:QPS、延迟分布、错误率、库存命中率
- [ ] Grafana dashboard:业务大盘 + 中间件大盘
- [ ] 告警规则(P99 > 100ms、错误率 > 1%)
- [ ] **里程碑产出**:`reports/week3_observability.md`

## Week 4:压测、优化、博客

### Day 22-24:压测大战
- [ ] k6 多机压测(本地 + 1-2 台云主机做 worker)
- [ ] 4 类场景:
  - 正常秒杀(库存充足)
  - 库存不足(99% 失败)
  - 突发流量(0 → 5w qps 1s 内)
  - 长时间稳定(1w qps × 30 min)
- [ ] 库存正确性验证脚本
- [ ] 性能问题定位(pprof CPU / mem / mutex profile)

### Day 25-26:优化迭代
- [ ] 根据 profile 优化热点(常见:JSON 序列化、log、锁竞争)
- [ ] 连接池调优(MySQL / Redis / Kafka)
- [ ] GC 调优(GOGC、GOMEMLIMIT)
- [ ] 优化前后对比报告

### Day 27-28:发布
- [ ] GitHub README:架构图、quick start、压测结果摘要
- [ ] 技术博客《从零到 10W QPS:Go 秒杀系统全链路拆解》
  - 发布:个人博客 → 知乎 → 掘金 → InfoQ
- [ ] 5 分钟 demo 视频
- [ ] LinkedIn / 简历更新

## 验收标准

- [ ] `make up && make migrate && make seed && make bench` 一键复现压测
- [ ] 单机 QPS ≥ 30k,集群 ≥ 100k
- [ ] 库存正确性测试 100% 通过(无超卖、无少卖)
- [ ] 幂等测试通过(重复请求结果一致)
- [ ] Grafana dashboard 截图 + 真实压测数据
- [ ] 简历可写:"独立设计 Go 秒杀系统(Redis Lua + Kafka + 分库分表 + 熔断限流),单机 30k+ QPS / P99 < 50ms,通过 Lua 原子操作实现零超卖,完整压测与监控体系"

## Stretch Goals(若有时间)
- K8s 部署 + HPA 自动扩缩
- 灰度发布(配置中心 + feature flag)
- 风控:设备指纹、行为分析(简化版)
- 接入支付 mock + TCC 事务
