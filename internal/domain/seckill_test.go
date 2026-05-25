package domain_test

import (
	"testing"

	"github.com/mjyangnb/flash-deal/internal/domain"
)

func TestSeckillOutcome_String(t *testing.T) {
	cases := []struct {
		o    domain.SeckillOutcome
		want string
	}{
		{domain.OutcomeQueued, "queued"},
		{domain.OutcomeSoldOut, "sold_out"},
		{domain.OutcomeUserLimit, "user_limit"},
		{domain.OutcomeDuplicate, "duplicate"},
		{domain.OutcomeNotStarted, "not_started"},
		{domain.OutcomeEnded, "ended"},
		{domain.OutcomeNotFound, "not_found"},
		{domain.OutcomeInternal, "internal"},
		{domain.OutcomeNotWarmed, "not_warmed"},
	}
	for _, c := range cases {
		if got := c.o.String(); got != c.want {
			t.Errorf("%d.String() = %q, want %q", c.o, got, c.want)
		}
	}
}
