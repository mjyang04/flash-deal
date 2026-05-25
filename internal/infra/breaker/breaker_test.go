package breaker_test

import (
	"errors"
	"testing"
	"time"

	"github.com/sony/gobreaker"

	"github.com/mjyangnb/flash-deal/internal/infra/breaker"
)

func TestBreaker_TripsOnHighFailure(t *testing.T) {
	cb := breaker.New(breaker.Config{
		Name: "t", MaxRequests: 1, Interval: time.Minute, Timeout: time.Second, FailureRatio: 0.5,
	})
	boom := errors.New("boom")
	// 5 failures → ratio 1.0 → trip
	for i := 0; i < 5; i++ {
		if err := cb.Do(func() error { return boom }); err == nil {
			t.Fatalf("expected boom on %d", i)
		}
	}
	// next call should be blocked open
	err := cb.Do(func() error { return nil })
	if !errors.Is(err, gobreaker.ErrOpenState) {
		t.Errorf("expected ErrOpenState, got %v", err)
	}
}
