//go:build integration

package middleware_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/gin-gonic/gin"
	goredis "github.com/redis/go-redis/v9"

	"github.com/mjyang04/flash-deal/internal/middleware"
)

func TestIdempotency_CacheReplay(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rdb := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:6380"})
	defer rdb.Close()
	rdb.Del(context.Background(), "idem:1:2:tok-cache")

	var calls int32
	r := gin.New()
	r.Use(middleware.Idempotency(rdb))
	r.POST("/v1/seckill", func(c *gin.Context) {
		atomic.AddInt32(&calls, 1)
		c.JSON(http.StatusAccepted, gin.H{"status": "queued", "n": calls})
	})

	body := `{"activity_id":1,"user_id":2,"idempotency_token":"tok-cache"}`
	mk := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest("POST", "/v1/seckill", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		return w
	}
	w1 := mk()
	if w1.Code != http.StatusAccepted {
		t.Fatalf("first code: %d", w1.Code)
	}
	w2 := mk()
	if w2.Code != http.StatusAccepted {
		t.Errorf("replay code: %d, want 202", w2.Code)
	}
	if w1.Body.String() != w2.Body.String() {
		t.Errorf("replay body differs: %q vs %q", w1.Body.String(), w2.Body.String())
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Errorf("handler invoked %d times, want 1", calls)
	}
}
