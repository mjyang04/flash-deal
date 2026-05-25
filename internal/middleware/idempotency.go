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

	"github.com/mjyangnb/flash-deal/internal/infra/metrics"
)

const idemTTL = 24 * time.Hour

// Idempotency: reads idem token from JSON body, SETNX "processing", on first
// completion stores cached (status + body). Replays within 24h return cached.
// Concurrent in-flight requests (token already "processing") return 409.
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
		if probe.IdempotencyToken == "" {
			c.Next()
			return
		}
		key := fmt.Sprintf("idem:%d:%d:%s", probe.ActivityID, probe.UserID, probe.IdempotencyToken)
		ctx := c.Request.Context()
		ok, err := rdb.SetNX(ctx, key, "processing", idemTTL).Result()
		if err != nil {
			c.Next()
			return
		}
		if !ok {
			v, gerr := rdb.Get(ctx, key).Result()
			if gerr == nil && v != "processing" {
				var cached struct {
					Status int             `json:"status"`
					Body   json.RawMessage `json:"body"`
				}
				if jerr := json.Unmarshal([]byte(v), &cached); jerr == nil {
					metrics.IdempotentReplayTotal.Inc()
					c.Data(cached.Status, "application/json", cached.Body)
					c.Abort()
					return
				}
			}
			c.AbortWithStatusJSON(http.StatusConflict, gin.H{
				"error": gin.H{
					"code":       "IDEMPOTENT_REPLAY",
					"message":    "still processing",
					"request_id": RequestIDFrom(c),
				},
			})
			return
		}
		bw := &bodyWriter{ResponseWriter: c.Writer, buf: &bytes.Buffer{}}
		c.Writer = bw
		c.Next()
		cached, _ := json.Marshal(map[string]any{
			"status": bw.Status(),
			"body":   json.RawMessage(bw.buf.Bytes()),
		})
		rdb.Set(ctx, key, string(cached), idemTTL)
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
