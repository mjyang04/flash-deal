package handler_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/mjyang04/flash-deal/internal/domain"
	"github.com/mjyang04/flash-deal/internal/handler"
)

type adminStub struct {
	created  []domain.Activity
	warmedID int64
}

func (s *adminStub) Create(_ context.Context, a domain.Activity) error {
	s.created = append(s.created, a)
	return nil
}
func (s *adminStub) Warm(_ context.Context, id int64) (int, error) {
	s.warmedID = id
	return 100, nil
}

func TestAdmin_CreateActivity(t *testing.T) {
	gin.SetMode(gin.TestMode)
	stub := &adminStub{}
	r := gin.New()
	r.POST("/admin/activities", handler.AdminCreateActivity(stub))

	body := `{"id":9001,"product_id":555,"total_stock":100,"start_at":"2026-05-25T12:00:00Z","end_at":"2026-05-25T13:00:00Z","per_user_limit":1,"status":2}`
	req := httptest.NewRequest("POST", "/admin/activities", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("code = %d, body = %s", w.Code, w.Body.String())
	}
	if len(stub.created) != 1 || stub.created[0].ID != 9001 {
		t.Errorf("not stored: %+v", stub.created)
	}
	// regression: snake_case json fields must populate PascalCase struct fields.
	a := stub.created[0]
	if a.ProductID != 555 || a.TotalStock != 100 || a.PerUserLimit != 1 {
		t.Errorf("scalar fields mismatch: %+v", a)
	}
	if a.StartAt.IsZero() || a.EndAt.IsZero() {
		t.Errorf("datetime fields unset: start=%v end=%v", a.StartAt, a.EndAt)
	}
}

func TestAdmin_WarmActivity(t *testing.T) {
	gin.SetMode(gin.TestMode)
	stub := &adminStub{}
	r := gin.New()
	r.POST("/admin/activities/:id/warm", handler.AdminWarmActivity(stub))

	req := httptest.NewRequest("POST", "/admin/activities/9001/warm", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("code = %d, body = %s", w.Code, w.Body.String())
	}
	if stub.warmedID != 9001 {
		t.Errorf("warmedID = %d, want 9001", stub.warmedID)
	}
}
