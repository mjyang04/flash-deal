package middleware

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	goredis "github.com/redis/go-redis/v9"

	"github.com/mjyang04/flash-deal/internal/infra/metrics"
)

const (
	idemTTL         = 24 * time.Hour
	maxBodyBytes    = 4 * 1024  // seckill body is tiny; reject larger to prevent OOM
	maxCachedBody   = 16 * 1024 // cap response cache to avoid unbounded buf growth
	idemWriteCtxTTL = 2 * time.Second
)

// Idempotency: reads idem token from JSON body, SETNX "processing", on first
// completion stores cached (status + body). Replays within 24h return cached.
// Concurrent in-flight requests (token already "processing") return 409.
//
// Hardening (M5):
//   - request body capped at maxBodyBytes (4 KiB) before reading → DoS protection
//   - cached-response write uses a detached context with 2s TTL → client disconnect
//     can no longer leave the key stuck on "processing" for 24h
//   - response cache buffer is capped at maxCachedBody (16 KiB)
//   - non-Nil Redis errors on Get are surfaced as 500, not silent 409
func Idempotency(rdb *goredis.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxBodyBytes)
		body, err := io.ReadAll(c.Request.Body)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusRequestEntityTooLarge, gin.H{
				"error": gin.H{
					"code":       "BODY_TOO_LARGE",
					"message":    fmt.Sprintf("body exceeds %d bytes", maxBodyBytes),
					"request_id": RequestIDFrom(c),
				},
			})
			return
		}
		c.Request.Body = io.NopCloser(bytes.NewBuffer(body))
		var probe struct {
			ActivityID       int64  `json:"activity_id"`
			UserID           int64  `json:"user_id"`
			IdempotencyToken string `json:"idempotency_token"`
		}
		if jerr := json.Unmarshal(body, &probe); jerr != nil || probe.IdempotencyToken == "" {
			c.Next()
			return
		}
		key := fmt.Sprintf("idem:%d:%d:%s", probe.ActivityID, probe.UserID, probe.IdempotencyToken)
		ctx := c.Request.Context()
		ok, serr := rdb.SetNX(ctx, key, "processing", idemTTL).Result()
		if serr != nil {
			// Fail-open: don't block legitimate traffic on Redis blip.
			c.Next()
			return
		}
		if !ok {
			v, gerr := rdb.Get(ctx, key).Result()
			switch {
			case errors.Is(gerr, goredis.Nil):
				// key just expired between SetNX and Get; pass through
				c.Next()
				return
			case gerr != nil:
				c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
					"error": gin.H{"code": "INTERNAL", "message": "internal error", "request_id": RequestIDFrom(c)},
				})
				return
			case v == "processing":
				// in-flight elsewhere; tell client to retry shortly
				c.Writer.Header().Set("Retry-After", "1")
				c.AbortWithStatusJSON(http.StatusConflict, gin.H{
					"error": gin.H{"code": "IDEMPOTENT_REPLAY", "message": "still processing", "request_id": RequestIDFrom(c)},
				})
				return
			}
			var cached struct {
				Status int             `json:"status"`
				Body   json.RawMessage `json:"body"`
			}
			if jerr := json.Unmarshal([]byte(v), &cached); jerr != nil {
				c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
					"error": gin.H{"code": "INTERNAL", "message": "internal error", "request_id": RequestIDFrom(c)},
				})
				return
			}
			metrics.IdempotentReplayTotal.Inc()
			c.Data(cached.Status, "application/json", cached.Body)
			c.Abort()
			return
		}
		bw := &bodyWriter{ResponseWriter: c.Writer, buf: &bytes.Buffer{}, max: maxCachedBody}
		c.Writer = bw
		c.Next()
		// Use a detached context so a client disconnect can't strand the key
		// on "processing" for 24h. Cap write to 2s so a Redis hiccup at finish
		// time also doesn't strand it.
		writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), idemWriteCtxTTL)
		defer cancel()
		cached, _ := json.Marshal(map[string]any{"status": bw.Status(), "body": json.RawMessage(bw.buf.Bytes())})
		_ = rdb.Set(writeCtx, key, string(cached), idemTTL).Err()
	}
}

type bodyWriter struct {
	gin.ResponseWriter
	buf      *bytes.Buffer
	max      int
	overflow bool
}

func (b *bodyWriter) Write(p []byte) (int, error) {
	if !b.overflow {
		if b.buf.Len()+len(p) > b.max {
			b.overflow = true
			b.buf.Reset() // give up caching; client still gets response
		} else {
			b.buf.Write(p)
		}
	}
	return b.ResponseWriter.Write(p)
}
