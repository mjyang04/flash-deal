package handler_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/mjyangnb/flash-deal/internal/handler"
)

type stubQueue struct{ state string }

func (s stubQueue) Get(_ context.Context, _ string) (string, error) { return s.state, nil }

func TestOrderByToken_Variants(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cases := []struct {
		state string
		want  map[string]any
	}{
		{"queued", map[string]any{"state": "queued"}},
		{"success:42", map[string]any{"state": "success", "order_id": "42"}},
		{"failed:boom", map[string]any{"state": "failed", "reason": "boom"}},
		{"not_found", map[string]any{"state": "not_found"}},
	}
	for _, c := range cases {
		r := gin.New()
		r.GET("/v1/order/by-token/:queue_token", handler.OrderByToken(stubQueue{state: c.state}))
		req := httptest.NewRequest("GET", "/v1/order/by-token/T", nil)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Errorf("code=%d body=%s", w.Code, w.Body)
		}
		var got map[string]any
		_ = json.Unmarshal(w.Body.Bytes(), &got)
		for k, v := range c.want {
			if got[k] != v {
				t.Errorf("state=%q field %q = %v, want %v", c.state, k, got[k], v)
			}
		}
	}
}
