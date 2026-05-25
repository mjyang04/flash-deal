# Security Review — M1→M5 stretch

Reviewer: ecc:security-reviewer (Opus). Scope: OWASP Top 10 + supply chain + secret exposure.

## CRITICAL (exploitable today)

1. **`internal/middleware/idempotency.go:24`** — unbounded `io.ReadAll` → single 50 GB body OOMs server.
2. **`internal/middleware/ratelimit.go:30-52`** — per-user limit keyed on `X-User-Id` header but business uses body `user_id`. Omit header → bypass. Need auth-bound identity for both.
3. **`deploy/docker-compose.yml:88`** — Grafana `admin/admin` + `0.0.0.0:3001`. Public host = instant pwn → MySQL creds via datasource.
4. **`cmd/api/main.go:54-59`** & **`cmd/consumer/main.go:47-52`** — pprof on `:6060`/`:6061` listens all interfaces unauthenticated → heap dump leaks tokens/DSN.

## HIGH

- `internal/handler/admin.go:21-51` — admin endpoints unauthenticated (ADR-007 promised JWT,not done) → anyone can create activities / warm Redis.
- `internal/middleware/recovery.go:25-31` — `fmt.Sprintf("internal: %v", r)` leaks panic content (incl. driver state) to client.
- `internal/handler/{seckill,order,admin}.go` — handlers echo internal `err.Error()` to clients.
- `internal/handler/order.go:18-46` — `GET /v1/order/by-token/:token` returns order_id without verifying ownership.
- `internal/middleware/ratelimit.go:36-41` — Redis error → fail-open silently.
- `cmd/api/main.go:147-153` — `gin.New()` trusts all proxies by default → `X-Forwarded-For` spoofing.
- `internal/repo/order_repo.go:89-92` — substring match for MySQL 1062.

## MEDIUM

- `cmd/api/main.go:131-132` — "snowflake-lite" predictable IDs → IDOR risk if future handler omits user_id.
- `internal/service/queue_token.go:20-31` — UUIDv7 leaks creation wall-clock time.
- `internal/middleware/idempotency.go:43-65` — cache key uses body-supplied user_id (not auth identity) → cross-user replay possible post-auth.
- `migrations/002_orders_shards.up.sql:7-11` — `GRANT ALL` on app user (should be DML only).
- `internal/infra/otel/tracer.go:18-21` — OTLP `WithInsecure()` → cleartext PII attributes.
- `deploy/docker-compose.yml:14, 28` — MySQL / Redis bind 0.0.0.0,no Redis password.

## LOW

- pprof start log advertises endpoint
- `domain.Activity` `ShouldBindJSON` accepts `Status` from client
- `s.rdb.Set(..., 24h)` hardcoded TTL
- `X-User-Id` header unsanitized (newline in Redis key)
- `internal/handler/admin.go:32` echoes server-assigned id

## Verdict

Not production-shippable. Localhost demo OK. Fix the 4 CRITICAL items before any external deployment.
