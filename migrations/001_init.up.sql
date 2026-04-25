-- Activities (single DB)
CREATE TABLE IF NOT EXISTS activities (
  id BIGINT UNSIGNED NOT NULL PRIMARY KEY,
  product_id BIGINT UNSIGNED NOT NULL,
  total_stock INT NOT NULL,
  start_at DATETIME NOT NULL,
  end_at DATETIME NOT NULL,
  per_user_limit INT NOT NULL DEFAULT 1,
  status TINYINT NOT NULL DEFAULT 0,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_status_window (status, start_at, end_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Orders shard 0 (in production: replicate this DDL across shards 1..N)
CREATE TABLE IF NOT EXISTS orders_0 (
  id BIGINT UNSIGNED NOT NULL PRIMARY KEY,
  user_id BIGINT UNSIGNED NOT NULL,
  activity_id BIGINT UNSIGNED NOT NULL,
  product_id BIGINT UNSIGNED NOT NULL,
  status TINYINT NOT NULL DEFAULT 0,
  idempotency_token VARCHAR(64) NOT NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uk_idem (activity_id, user_id, idempotency_token),
  KEY idx_user_created (user_id, created_at),
  KEY idx_activity (activity_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
