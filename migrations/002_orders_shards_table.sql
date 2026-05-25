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
