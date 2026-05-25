// Reconcile sweeper: periodically verify that for each active activity,
//
//	initial_stock == redis_remaining + orders_count_across_shards
//
// Discrepancies surface as a Prometheus counter + zap warning. The sweeper
// only *observes* — it does not silently mutate stock. Operator can decide
// to top-up Redis from MySQL canonical (`adminSvc.Warm`) after investigation.
package service

import (
	"context"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	goredis "github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/mjyang04/flash-deal/internal/infra/logger"
	"github.com/mjyang04/flash-deal/internal/infra/metrics"
	"github.com/mjyang04/flash-deal/internal/repo"
	"github.com/mjyang04/flash-deal/pkg/shardkey"
)

// ReconcileService walks active activities every Interval and emits findings.
type ReconcileService struct {
	activities repo.ActivityRepo
	rdb        *goredis.Client
	shards     []*sqlx.DB
	db         *sqlx.DB // fallback when shards == nil
	interval   time.Duration
	clock      func() time.Time
}

// NewReconcile builds a sweeper. If `shards` is empty it falls back to `db` (M1/M2 layout).
func NewReconcile(activities repo.ActivityRepo, rdb *goredis.Client, shards []*sqlx.DB, db *sqlx.DB, interval time.Duration) *ReconcileService {
	return &ReconcileService{
		activities: activities, rdb: rdb, shards: shards, db: db,
		interval: interval, clock: time.Now,
	}
}

// Run blocks until ctx done. Each tick:
//  1. read activity from MySQL
//  2. read stock:{id} from Redis
//  3. SUM count(*) from orders_0 across all shards filtered by activity_id
//  4. expected: total_stock == redis_remaining + orders_count
//  5. on mismatch: increment counter + log
//
// Activities checked are pinned by caller: pass activity IDs via channel-fed
// future enhancement; for now check a single bootstrap list provided to RunFor.
func (s *ReconcileService) Run(ctx context.Context, activityIDs []int64) error {
	t := time.NewTicker(s.interval)
	defer t.Stop()
	// run once immediately
	s.tick(ctx, activityIDs)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			s.tick(ctx, activityIDs)
		}
	}
}

// Finding describes one activity's correctness state at a point in time.
type Finding struct {
	ActivityID  int64
	InitStock   int
	Redis       int
	OrdersCount int
	Drift       int // InitStock - (Redis + OrdersCount); 0 means consistent
}

// Check returns the per-activity finding without logging — useful for tests.
func (s *ReconcileService) Check(ctx context.Context, activityID int64) (Finding, error) {
	a, err := s.activities.GetByID(ctx, activityID)
	if err != nil {
		return Finding{}, err
	}
	rem, err := s.rdb.Get(ctx, fmt.Sprintf("stock:%d", activityID)).Int()
	if err != nil {
		return Finding{}, fmt.Errorf("redis get stock:%d: %w", activityID, err)
	}
	cnt, err := s.countOrders(ctx, activityID)
	if err != nil {
		return Finding{}, err
	}
	return Finding{
		ActivityID:  activityID,
		InitStock:   a.TotalStock,
		Redis:       rem,
		OrdersCount: cnt,
		Drift:       a.TotalStock - (rem + cnt),
	}, nil
}

func (s *ReconcileService) countOrders(ctx context.Context, activityID int64) (int, error) {
	dbs := s.shards
	if len(dbs) == 0 {
		dbs = []*sqlx.DB{s.db}
	}
	const q = `SELECT COUNT(*) FROM orders_0 WHERE activity_id = ?`
	total := 0
	for i, d := range dbs {
		var n int
		if err := d.GetContext(ctx, &n, q, activityID); err != nil {
			return 0, fmt.Errorf("shard %d count: %w", i, err)
		}
		total += n
	}
	_ = shardkey.DBIndex // referenced to keep import meaningful for future per-shard queries
	return total, nil
}

func (s *ReconcileService) tick(ctx context.Context, ids []int64) {
	for _, id := range ids {
		f, err := s.Check(ctx, id)
		if err != nil {
			logger.L().Warn("reconcile check failed", zap.Int64("activity_id", id), zap.Error(err))
			continue
		}
		if f.Drift != 0 {
			metrics.DLQTotal.WithLabelValues(fmt.Sprintf("stock_drift_%d", f.ActivityID)).Inc()
			logger.L().Warn("stock drift detected",
				zap.Int64("activity_id", f.ActivityID),
				zap.Int("init_stock", f.InitStock),
				zap.Int("redis_remaining", f.Redis),
				zap.Int("orders_count", f.OrdersCount),
				zap.Int("drift", f.Drift),
			)
		}
	}
}
