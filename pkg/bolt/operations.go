// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package bolt

import (
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// WriteOperation represents a single write operation to be queued in the buffer.
// Users explicitly provide all necessary information: bucket path, key, value, and optional stats update.
type WriteOperation struct {
	// Where to write
	BucketPath []string
	Key        []byte
	Value      []byte

	// Optional stats update (aggregated by buffer before applying)
	StatsUpdate *StatsUpdate
}

// StatsUpdate represents a stats bucket update that will be aggregated by the buffer.
type StatsUpdate struct {
	// StatsBucketPath is the path to the stats bucket to update
	// Example: []string{"SRC", "nodes"} or []string{"SRC", "levels", "00000001", "pending"}
	StatsBucketPath []string
	// Delta is the amount to add/subtract (+1, -1, etc.)
	Delta int64
}

// Execute performs the write operation within a transaction.
// This writes the key/value pair to the specified bucket path.
// Stats updates are NOT executed here - they are collected by the buffer and aggregated.
func (op *WriteOperation) Execute(tx *bolt.Tx) error {
	if len(op.BucketPath) == 0 {
		return fmt.Errorf("bucket path cannot be empty")
	}

	bucket, err := getOrCreateBucket(tx, op.BucketPath)
	if err != nil {
		return fmt.Errorf("failed to get or create bucket at path %v: %w", op.BucketPath, err)
	}

	return bucket.Put(op.Key, op.Value)
}
