// Package shardkey computes shard routing for sharded MySQL writes/reads.
package shardkey

// DBIndex returns which logical DB the user_id belongs to.
// Use the same function on every read & write path to keep routing stable.
func DBIndex(userID int64, dbCount int) int {
	if dbCount <= 0 {
		return 0
	}
	idx := userID % int64(dbCount)
	if idx < 0 {
		idx = -idx
	}
	return int(idx)
}

// TableIndex returns which physical table inside a DB the user_id belongs to.
// dbCount must match DBIndex's argument; tableCount is per-DB table count.
func TableIndex(userID int64, dbCount, tableCount int) int {
	if dbCount <= 0 || tableCount <= 0 {
		return 0
	}
	bucket := userID / int64(dbCount)
	if bucket < 0 {
		bucket = -bucket
	}
	return int(bucket % int64(tableCount))
}
