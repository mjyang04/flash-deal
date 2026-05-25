package middleware

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	goredis "github.com/redis/go-redis/v9"
	"golang.org/x/time/rate"

	"github.com/mjyang04/flash-deal/internal/infra/metrics"
)

// RateLimit returns Gin middleware that enforces:
//   - global token bucket: globalQPS req/s with globalBurst burst
//   - per-user redis sliding window: perUserPerMin requests/min, keyed by X-User-Id header
//
// X-User-Id is opportunistic — if absent the per-user check is skipped (e.g. for
// /health and /admin paths). Redis errors fail-open.
func RateLimit(rdb *goredis.Client, perUserPerMin, globalQPS, globalBurst int) gin.HandlerFunc {
	global := rate.NewLimiter(rate.Limit(globalQPS), globalBurst)
	return func(c *gin.Context) {
		if !global.Allow() {
			metrics.RateLimitTotal.WithLabelValues("global").Inc()
			writeRateLimit(c, "global")
			return
		}
		userID := c.GetHeader("X-User-Id")
		if userID == "" {
			c.Next()
			return
		}
		key := fmt.Sprintf("ratelimit:user:%s", userID)
		ctx := c.Request.Context()
		cur, err := rdb.Incr(ctx, key).Result()
		if err != nil {
			c.Next()
			return
		}
		if cur == 1 {
			rdb.Expire(ctx, key, time.Minute)
		}
		if int(cur) > perUserPerMin {
			metrics.RateLimitTotal.WithLabelValues("per_user").Inc()
			c.Writer.Header().Set("Retry-After", strconv.Itoa(60))
			writeRateLimit(c, "per_user")
			return
		}
		c.Next()
	}
}

func writeRateLimit(c *gin.Context, scope string) {
	c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
		"error": gin.H{
			"code":       "RATE_LIMITED",
			"message":    "rate limited at " + scope,
			"request_id": RequestIDFrom(c),
		},
	})
}
