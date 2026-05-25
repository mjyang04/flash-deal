package middleware

import (
	"net/http"
	"runtime/debug"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/mjyang04/flash-deal/internal/infra/logger"
)

// Recovery converts panics into 500. Panic content + stack go to server logs
// only — the client response is a fixed generic message to avoid leaking
// internal state (driver errors, struct fields, paths, embedded credentials).
func Recovery() gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if r := recover(); r != nil {
				logger.L().Error("panic",
					zap.String("request_id", RequestIDFrom(c)),
					zap.String("path", c.FullPath()),
					zap.Any("panic", r),
					zap.String("stack", string(debug.Stack())),
				)
				c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
					"error": gin.H{
						"code":       "INTERNAL",
						"message":    "internal error",
						"request_id": RequestIDFrom(c),
					},
				})
			}
		}()
		c.Next()
	}
}
