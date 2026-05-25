package handler_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/mjyangnb/flash-deal/internal/domain"
	"github.com/mjyangnb/flash-deal/internal/handler"
)

type stubSvc struct {
	out domain.SeckillOutcome
	rem int
}

func (s *stubSvc) Seckill(_ context.Context, _ domain.SeckillRequest) (handler.SeckillOutput, error) {
	return handler.SeckillOutput{Outcome: s.out, QueueToken: "tok", Remaining: s.rem}, nil
}

func post(t *testing.T, r http.Handler, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest("POST", "/v1/seckill", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func newRouter(out domain.SeckillOutcome, rem int) http.Handler {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/v1/seckill", handler.Seckill(&stubSvc{out: out, rem: rem}))
	return r
}

func TestHandler_Outcomes(t *testing.T) {
	body := `{"activity_id":1,"user_id":2,"idempotency_token":"t"}`
	cases := []struct {
		out  domain.SeckillOutcome
		code int
	}{
		{domain.OutcomeQueued, http.StatusAccepted},
		{domain.OutcomeSoldOut, http.StatusGone},
		{domain.OutcomeUserLimit, http.StatusConflict},
		{domain.OutcomeDuplicate, http.StatusConflict},
		{domain.OutcomeNotStarted, http.StatusForbidden},
		{domain.OutcomeEnded, http.StatusGone},
		{domain.OutcomeNotFound, http.StatusNotFound},
		{domain.OutcomeInternal, http.StatusInternalServerError},
	}
	for _, c := range cases {
		w := post(t, newRouter(c.out, 9), body)
		if w.Code != c.code {
			t.Errorf("outcome %v → code %d, want %d", c.out, w.Code, c.code)
		}
	}
}

func TestHandler_QueuedBody(t *testing.T) {
	w := post(t, newRouter(domain.OutcomeQueued, 9), `{"activity_id":1,"user_id":2,"idempotency_token":"t"}`)
	var got map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got["status"] != "queued" {
		t.Errorf("status = %v, want queued", got["status"])
	}
	if got["queue_token"] != "tok" {
		t.Errorf("queue_token = %v", got["queue_token"])
	}
	if int(got["remaining"].(float64)) != 9 {
		t.Errorf("remaining = %v, want 9", got["remaining"])
	}
}

func TestHandler_BadRequest(t *testing.T) {
	w := post(t, newRouter(domain.OutcomeQueued, 0), `{"activity_id":0,"user_id":0}`) // missing token + zero ids
	if w.Code != http.StatusBadRequest {
		t.Errorf("code = %d, want 400 (got body: %s)", w.Code, w.Body.String())
	}
}
