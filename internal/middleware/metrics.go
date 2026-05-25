package middleware

import (
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/mjyangnb/flash-deal/internal/infra/metrics"
)

// Metrics records http_request_duration_seconds per route + status.
func Metrics() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		elapsed := time.Since(start).Seconds()
		route := c.FullPath()
		if route == "" {
			route = "unknown"
		}
		metrics.HTTPDuration.WithLabelValues(route, strconv.Itoa(c.Writer.Status())).Observe(elapsed)
	}
}
