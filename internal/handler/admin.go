package handler

import (
	"context"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"

	"github.com/mjyang04/flash-deal/internal/domain"
)

// AdminService is the port admin handlers depend on.
type AdminService interface {
	Create(ctx context.Context, a domain.Activity) error
	Warm(ctx context.Context, id int64) (int, error)
}

// AdminCreateActivity: POST /admin/activities — write canonical record to MySQL.
// M1 leaves Redis warm to a separate explicit call (AdminWarmActivity).
func AdminCreateActivity(svc AdminService) gin.HandlerFunc {
	return func(c *gin.Context) {
		var a domain.Activity
		if err := c.ShouldBindJSON(&a); err != nil {
			writeError(c, http.StatusBadRequest, "BAD_REQUEST", err.Error())
			return
		}
		if err := svc.Create(c.Request.Context(), a); err != nil {
			writeError(c, http.StatusInternalServerError, "INTERNAL", err.Error())
			return
		}
		c.JSON(http.StatusCreated, gin.H{"id": a.ID})
	}
}

// AdminWarmActivity: POST /admin/activities/:id/warm — refresh Redis cache from MySQL.
func AdminWarmActivity(svc AdminService) gin.HandlerFunc {
	return func(c *gin.Context) {
		id, err := strconv.ParseInt(c.Param("id"), 10, 64)
		if err != nil {
			writeError(c, http.StatusBadRequest, "BAD_REQUEST", "bad id")
			return
		}
		warmed, err := svc.Warm(c.Request.Context(), id)
		if err != nil {
			writeError(c, http.StatusInternalServerError, "INTERNAL", err.Error())
			return
		}
		c.JSON(http.StatusOK, gin.H{"warmed_stock": warmed})
	}
}
