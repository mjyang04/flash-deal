//go:build integration

package middleware_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	goredis "github.com/redis/go-redis/v9"

	"github.com/mjyangnb/flash-deal/internal/middleware"
)

func TestRateLimit_PerUser(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rdb := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:6380"})
	defer rdb.Close()
	rdb.Del(context.Background(), "ratelimit:user:U1")

	r := gin.New()
	r.Use(middleware.RateLimit(rdb, 3, 100000, 1000))
	r.GET("/x", func(c *gin.Context) { c.Status(http.StatusOK) })

	send := func() int {
		req := httptest.NewRequest("GET", "/x", nil)
		req.Header.Set("X-User-Id", "U1")
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		return w.Code
	}
	for i := 0; i < 3; i++ {
		if c := send(); c != http.StatusOK {
			t.Fatalf("req %d: code=%d, want 200", i+1, c)
		}
	}
	if c := send(); c != http.StatusTooManyRequests {
		t.Errorf("4th req: code=%d, want 429", c)
	}
}

func TestRateLimit_GlobalBurst(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rdb := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:6380"})
	defer rdb.Close()

	// global QPS=1, burst=1 — second immediate req should be rejected
	r := gin.New()
	r.Use(middleware.RateLimit(rdb, 1000, 1, 1))
	r.GET("/y", func(c *gin.Context) { c.Status(http.StatusOK) })

	req := httptest.NewRequest("GET", "/y", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("first: %d", w.Code)
	}
	w2 := httptest.NewRecorder()
	r.ServeHTTP(w2, req)
	if w2.Code != http.StatusTooManyRequests {
		t.Errorf("second: %d", w2.Code)
	}
}
