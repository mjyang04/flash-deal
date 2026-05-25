# Code Review — M1→M5 stretch

Run on `feat/stretch-goals` (after M4 release + OTel kafka header + reconcile sweeper + chaos PASS).

Reviewer: ecc:code-reviewer (Opus). Audit scope: correctness, leaks, concurrency, idioms, test quality.

## CRITICAL (blocks merge)

1. **`internal/middleware/idempotency.go:65`** — 409 stickiness: SetNX=false branch hits 24h-stale `processing` keys, locking users out for 24h on a Redis blip.
2. **`internal/middleware/idempotency.go:73`** — Cached response write uses request ctx, cancelled on client disconnect → key stays `processing` for 24h.
3. **`internal/service/order_materializer.go:60`** — MySQL insert OK + Redis Set fails → DLQ marks `failed` for an order that actually exists.
4. **`internal/infra/kafka/consumer.go:99-101`** — DLQ-publish error swallowed; offset committed anyway → permanent message loss.

## HIGH

- `internal/middleware/idempotency.go:38` — unbounded body read OOM
- `internal/middleware/ratelimit.go:42-44` — INCR + EXPIRE race (crash between → key without TTL)
- `internal/service/seckill.go:107` & `cmd/api/main.go:131-132` — "snowflake-lite" ID = `time.Now().UnixNano() + atomic` → multi-host collision
- `internal/service/reconcile.go:113` — dead `_ = shardkey.DBIndex`
- `cmd/api/main.go:140-145` — reconcile goroutine uses `context.Background()`,never cancelled
- `internal/infra/kafka/consumer.go:89` — `time.Sleep` ignores ctx during shutdown
- `internal/middleware/idempotency.go:84` — `bodyWriter.buf` unbounded
- `internal/service/queue_token.go:34-40` — `err == goredis.Nil` should be `errors.Is`
- `internal/infra/logger/logger.go:17-26` — `sync.Once` swallows 2nd Init error

## MEDIUM

- `internal/repo/order_repo.go:89-92` — substring match on MySQL 1062 (use `*mysql.MySQLError`)
- `internal/repo/stock_redis_repo.go:57-82` — Lua duplicated in `.go` + `.lua` (use `//go:embed`)
- `cmd/consumer/main.go:117-127` — poison-pill (unmarshal fail) loses raw bytes
- `internal/service/seckill.go:113-117` — queue token error silently fallback to orderID
- `internal/middleware/idempotency.go:67` — writer replacement not restored
- `internal/infra/breaker/breaker.go:31-35` — magic `>=5` conflicts with `MaxRequests` config
- `cmd/api/main.go:81-93` — `log.Fatalf` skips `defer` on shards
- `internal/service/reconcile.go:82` — `redis.Nil` produces noisy WARN
- `cmd/api/main.go:208-219` — `serviceHandlerAdapter` mirrors identical struct

## LOW

- `internal/infra/kafka/consumer.go:84` — `c.reader.Config()` allocates per message
- `internal/repo/stock_redis_repo_test.go:96-117` — hand-rolled itoa
- `internal/middleware/recovery.go:28` — panic content leaked to client (overlap with security HIGH)
- `internal/repo/sharded_order_repo.go:24` — hardcoded `orders_0`
- `internal/service/seckill.go:89` — `!now.Before(EndAt)` style

## Strengths

- Port-driven design (`OrderCreator`/`OrderCreatorWithToken`/`StockDeducter`) really paid off in M2 swap.
- `TestStockRedis_NoOversell` is a genuine invariant check, not theater.

## Verdict

Has blocking issues. Fix the 4 CRITICAL items before next tag.
