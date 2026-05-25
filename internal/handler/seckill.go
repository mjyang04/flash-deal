// Package handler exposes HTTP handlers for the seckill API.
package handler

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/mjyangnb/flash-deal/internal/domain"
	"github.com/mjyangnb/flash-deal/internal/middleware"
)

// SeckillOutput mirrors service.SeckillOutput so the handler can depend on its
// own port instead of importing the service package transitively in tests.
type SeckillOutput struct {
	Outcome    domain.SeckillOutcome
	QueueToken string
	Remaining  int
}

// SeckillService is the port the handler calls.
type SeckillService interface {
	Seckill(ctx context.Context, req domain.SeckillRequest) (SeckillOutput, error)
}

// Seckill returns a Gin handler that drives the seckill flow.
func Seckill(svc SeckillService) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req domain.SeckillRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			writeError(c, http.StatusBadRequest, "BAD_REQUEST", err.Error())
			return
		}
		out, err := svc.Seckill(c.Request.Context(), req)
		if err != nil && out.Outcome == domain.OutcomeInternal {
			writeError(c, http.StatusInternalServerError, "INTERNAL", err.Error())
			return
		}
		switch out.Outcome {
		case domain.OutcomeQueued:
			c.JSON(http.StatusAccepted, gin.H{
				"status":      out.Outcome.String(),
				"queue_token": out.QueueToken,
				"remaining":   out.Remaining,
			})
		case domain.OutcomeSoldOut:
			writeError(c, http.StatusGone, "STOCK_NOT_ENOUGH", "sold out")
		case domain.OutcomeUserLimit:
			writeError(c, http.StatusConflict, "USER_LIMIT_EXCEEDED", "per-user limit hit")
		case domain.OutcomeDuplicate:
			writeError(c, http.StatusConflict, "IDEMPOTENT_REPLAY", "duplicate token")
		case domain.OutcomeNotStarted:
			writeError(c, http.StatusForbidden, "ACTIVITY_NOT_STARTED", "activity not started")
		case domain.OutcomeEnded:
			writeError(c, http.StatusGone, "ACTIVITY_ENDED", "activity ended")
		case domain.OutcomeNotFound:
			writeError(c, http.StatusNotFound, "NOT_FOUND", "activity not found")
		default:
			writeError(c, http.StatusInternalServerError, "INTERNAL", "unknown outcome")
		}
	}
}

func writeError(c *gin.Context, status int, code, msg string) {
	c.JSON(status, gin.H{
		"error": gin.H{
			"code":       code,
			"message":    msg,
			"request_id": middleware.RequestIDFrom(c),
		},
	})
}
