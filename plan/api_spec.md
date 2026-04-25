# flash-deal — API Spec

All requests/responses are `application/json` unless noted.

## Common

### Headers
- `Authorization: Bearer <jwt>` — required when `auth_enabled=true`
- `X-Request-Id: <uuid>` — optional; gateway will generate if absent
- `Content-Type: application/json` — for POST/PUT

### Error envelope
```json
{
  "error": {
    "code": "STOCK_NOT_ENOUGH",
    "message": "stock not enough for activity 1001",
    "request_id": "..."
  }
}
```

### Standard error codes
| Code | HTTP | Meaning |
|------|------|---------|
| `BAD_REQUEST` | 400 | malformed body / validation failure |
| `UNAUTHENTICATED` | 401 | missing or invalid token |
| `FORBIDDEN` | 403 | user not allowed |
| `NOT_FOUND` | 404 | activity / order missing |
| `IDEMPOTENT_REPLAY` | 409 | duplicate idempotency_token, returns prior result |
| `USER_LIMIT_EXCEEDED` | 409 | per-user limit hit |
| `STOCK_NOT_ENOUGH` | 410 | sold out (Gone semantics) |
| `RATE_LIMITED` | 429 | per-user or global rate limit |
| `INTERNAL` | 500 | unexpected |
| `BACKEND_UNAVAILABLE` | 503 | downstream broken / circuit open |

## Endpoints

### `POST /v1/seckill`

Reserve stock; place into async order queue.

**Request body**
```json
{
  "activity_id": 1001,
  "user_id": 88888,
  "idempotency_token": "8b1c8e92-..."
}
```

**Response 202 Accepted (queued)**
```json
{
  "status": "queued",
  "queue_token": "01J...",
  "remaining": 9876
}
```

**Response 410 Gone (sold out)**
```json
{ "error": { "code": "STOCK_NOT_ENOUGH", "message": "..." } }
```

**Response 409 (per-user limit)**
```json
{ "error": { "code": "USER_LIMIT_EXCEEDED", "message": "..." } }
```

**Response 409 (idempotent replay)**
Returns the originally-stored response (could be 202 or 410 depending on first call's outcome).

---

### `GET /v1/order/by-token/:queue_token`

Poll for async order materialization status.

**Response 200**
```json
{
  "queue_token": "01J...",
  "state": "queued | success | failed | not_found",
  "order_id": 12345678,        // present iff state=success
  "reason": "..."              // present iff state=failed
}
```

---

### `GET /v1/order/:order_id`

Look up a materialized order. Routes to the correct shard via `shardkey`.

**Response 200**
```json
{
  "id": 12345678,
  "user_id": 88888,
  "activity_id": 1001,
  "product_id": 555,
  "status": "queued | paid | canceled | refunded",
  "created_at": "2026-04-25T...Z"
}
```

---

### `GET /v1/activities/:id`

Public view of an activity (cacheable).

**Response 200**
```json
{
  "id": 1001,
  "product_id": 555,
  "total_stock": 10000,
  "remaining_stock": 9876,
  "start_at": "...",
  "end_at": "...",
  "per_user_limit": 1,
  "status": "running"
}
```

---

### Admin (auth required, role=admin)

#### `POST /admin/activities`
Create activity (`total_stock`, window, etc.).

#### `POST /admin/activities/:id/warm`
Warm Redis stock from MySQL canonical value.

```json
{ "warmed_stock": 10000 }
```

#### `POST /admin/activities/:id/freeze`
Stop accepting new requests (set status to canceled).

---

### `GET /health`
```json
{ "status": "ok" }
```

### `GET /ready`
```json
{
  "status": "ready",
  "checks": {
    "redis": "ok",
    "mysql_shard_0": "ok",
    "mysql_shard_1": "ok",
    "mysql_shard_2": "ok",
    "mysql_shard_3": "ok",
    "kafka": "ok"
  }
}
```

### `GET /metrics`
Prometheus exposition format.

## Rate limits (default)

| Scope | Bucket | Limit |
|-------|--------|-------|
| per-user | requests/minute | 5 |
| per-IP | requests/minute | 30 |
| global | requests/second | 100,000 |

## Idempotency rules

- Token must be unique within (activity_id, user_id) for **24h**
- `SETNX idem:{activity_id}:{user_id}:{token}` controls in-flight detection
- After service call returns, success/error response cached at the same key with `EXPIRE 86400`
- Replays within 24h return the cached body and original status code
