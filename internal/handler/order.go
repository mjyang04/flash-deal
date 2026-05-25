package handler

import (
	"context"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

// QueueQuery is the port for queue state lookup.
type QueueQuery interface {
	Get(ctx context.Context, token string) (string, error)
}

// OrderByToken: GET /v1/order/by-token/:queue_token
// Returns {queue_token, state, order_id?, reason?}.
func OrderByToken(q QueueQuery) gin.HandlerFunc {
	return func(c *gin.Context) {
		token := c.Param("queue_token")
		if token == "" {
			writeError(c, http.StatusBadRequest, "BAD_REQUEST", "token required")
			return
		}
		state, err := q.Get(c.Request.Context(), token)
		if err != nil {
			writeError(c, http.StatusInternalServerError, "INTERNAL", err.Error())
			return
		}
		resp := gin.H{"queue_token": token}
		switch {
		case state == "queued":
			resp["state"] = "queued"
		case strings.HasPrefix(state, "success:"):
			resp["state"] = "success"
			resp["order_id"] = strings.TrimPrefix(state, "success:")
		case strings.HasPrefix(state, "failed:"):
			resp["state"] = "failed"
			resp["reason"] = strings.TrimPrefix(state, "failed:")
		case state == "not_found":
			resp["state"] = "not_found"
		default:
			resp["state"] = state
		}
		c.JSON(http.StatusOK, resp)
	}
}
