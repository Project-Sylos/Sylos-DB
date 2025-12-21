// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package bolt

import (
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// IteratorOptions configures how items are iterated.
type IteratorOptions struct {
	// Limit is the maximum number of items to return (0 = no limit)
	Limit int
}

// IterateStatusBucket iterates over all ULIDs in a status bucket.
// The callback receives the ULID (nodeID) for each item in the status bucket.
func (db *DB) IterateStatusBucket(queueType string, level int, status string, opts IteratorOptions, fn func(nodeID []byte) error) error {
	return db.View(func(tx *bolt.Tx) error {
		bucket := getBucket(tx, GetStatusBucketPath(queueType, level, status))
		if bucket == nil {
			// Bucket doesn't exist yet, that's okay (no items)
			return nil
		}

		cursor := bucket.Cursor()
		count := 0

		for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
			if opts.Limit > 0 && count >= opts.Limit {
				break
			}

			// Make a copy of the key to pass to callback
			keyCopy := make([]byte, len(k))
			copy(keyCopy, k)

			if err := fn(keyCopy); err != nil {
				return err
			}

			count++
		}

		return nil
	})
}

// IterateNodeStates iterates over all nodes in the nodes bucket.
// The callback receives the ULID (nodeID) and NodeState for each node.
func (db *DB) IterateNodeStates(queueType string, opts IteratorOptions, fn func(nodeID []byte, state *NodeState) error) error {
	return db.View(func(tx *bolt.Tx) error {
		bucket := getBucket(tx, GetNodesBucketPath(queueType))
		if bucket == nil {
			return fmt.Errorf("nodes bucket not found for %s", queueType)
		}

		cursor := bucket.Cursor()
		count := 0

		for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
			if opts.Limit > 0 && count >= opts.Limit {
				break
			}

			// Deserialize node state
			ns, err := DeserializeNodeState(v)
			if err != nil {
				return fmt.Errorf("failed to deserialize node state: %w", err)
			}

			// Make a copy of the key
			keyCopy := make([]byte, len(k))
			copy(keyCopy, k)

			if err := fn(keyCopy, ns); err != nil {
				return err
			}

			count++
		}

		return nil
	})
}

// IterateLevel iterates over all status buckets at a specific level.
// For each status bucket, it calls the callback with the status name and bucket.
func (db *DB) IterateLevel(queueType string, level int, fn func(status string, bucket *bolt.Bucket) error) error {
	return db.View(func(tx *bolt.Tx) error {
		levelBucket := getBucket(tx, GetLevelBucketPath(queueType, level))
		if levelBucket == nil {
			// Level doesn't exist yet, that's okay
			return nil
		}

		cursor := levelBucket.Cursor()

		for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
			statusBucket := levelBucket.Bucket(k)
			if statusBucket == nil {
				continue // Not a bucket, skip
			}

			status := string(k)
			if err := fn(status, statusBucket); err != nil {
				return err
			}
		}

		return nil
	})
}

// HasItems checks if a bucket (specified by bucket path) has any items.
// Returns true if the bucket exists and has at least one item, false otherwise.
// This is O(1) - it only checks for the first key without counting all items.
func (db *DB) HasItems(bucketPath []string) (bool, error) {
	hasItems := false

	err := db.View(func(tx *bolt.Tx) error {
		bucket := getBucket(tx, bucketPath)
		if bucket == nil {
			// Bucket doesn't exist, no items
			return nil
		}

		// Just check if there's at least one key - O(1) operation
		cursor := bucket.Cursor()
		key, _ := cursor.First()
		hasItems = (key != nil)

		return nil
	})

	return hasItems, err
}

// HasStatusBucketItems checks if a status bucket has any items.
// Returns true if the bucket exists and has at least one item, false otherwise.
// This is O(1) - it only checks for the first key without counting all items.
func (db *DB) HasStatusBucketItems(queueType string, level int, status string) (bool, error) {
	bucketPath := GetStatusBucketPath(queueType, level, status)
	return db.HasItems(bucketPath)
}

// CountStatusBucket returns the number of items in a status bucket.
// Uses stats bucket for O(1) lookup. Falls back to cursor scan if stats unavailable.
func (db *DB) CountStatusBucket(queueType string, level int, status string) (int, error) {
	// Try stats first (fast path)
	bucketPath := GetStatusBucketPath(queueType, level, status)
	count, err := db.GetBucketCount(bucketPath)
	if err == nil {
		// Stats available - return count
		return int(count), nil
	}

	// Fallback to cursor scan (slow path, but safe)
	return db.countStatusBucketSlow(queueType, level, status)
}

// countStatusBucketSlow performs a full cursor scan of a status bucket.
// This is the fallback method when stats are unavailable.
func (db *DB) countStatusBucketSlow(queueType string, level int, status string) (int, error) {
	count := 0

	err := db.View(func(tx *bolt.Tx) error {
		bucket := getBucket(tx, GetStatusBucketPath(queueType, level, status))
		if bucket == nil {
			return nil // Bucket doesn't exist, count is 0
		}

		cursor := bucket.Cursor()
		for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
			count++
		}

		return nil
	})

	return count, err
}

// CountNodes returns the total number of nodes in the nodes bucket.
// Uses stats bucket for O(1) lookup. Falls back to cursor scan if stats unavailable.
func (db *DB) CountNodes(queueType string) (int, error) {
	// Try stats first (fast path)
	bucketPath := GetNodesBucketPath(queueType)
	count, err := db.GetBucketCount(bucketPath)
	if err == nil {
		// Stats available - return count
		return int(count), nil
	}

	// Fallback to cursor scan (slow path, but safe)
	return db.countNodesSlow(queueType)
}

// countNodesSlow performs a full cursor scan of the nodes bucket.
// This is the fallback method when stats are unavailable.
func (db *DB) countNodesSlow(queueType string) (int, error) {
	count := 0

	err := db.View(func(tx *bolt.Tx) error {
		bucket := getBucket(tx, GetNodesBucketPath(queueType))
		if bucket == nil {
			return nil
		}

		cursor := bucket.Cursor()
		for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
			count++
		}

		return nil
	})

	return count, err
}

// GetAllLevels returns all level numbers that exist for a queue type.
func (db *DB) GetAllLevels(queueType string) ([]int, error) {
	var levels []int

	err := db.View(func(tx *bolt.Tx) error {
		levelsBucket := getBucket(tx, []string{TraversalDataBucket, queueType, SubBucketLevels})
		if levelsBucket == nil {
			return nil // No levels yet
		}

		cursor := levelsBucket.Cursor()
		for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
			levelBucket := levelsBucket.Bucket(k)
			if levelBucket == nil {
				continue // Not a bucket
			}

			levelNum, err := ParseLevel(string(k))
			if err != nil {
				continue // Skip invalid level names
			}

			levels = append(levels, levelNum)
		}

		return nil
	})

	return levels, err
}

// FindMinPendingLevel finds the minimum level that has pending items.
// Returns -1 if no pending items exist.
func (db *DB) FindMinPendingLevel(queueType string) (int, error) {
	levels, err := db.GetAllLevels(queueType)
	if err != nil {
		return -1, err
	}

	minLevel := -1

	for _, level := range levels {
		count, err := db.CountStatusBucket(queueType, level, StatusPending)
		if err != nil {
			continue
		}

		if count > 0 {
			if minLevel == -1 || level < minLevel {
				minLevel = level
			}
		}
	}

	return minLevel, nil
}

// LeaseTasksFromStatus atomically leases tasks by moving them from the source status
// to "in-progress" status. This prevents race conditions where multiple workers
// could lease the same task. Similar to MongoDB's findAndModify operation.
//
// This function:
// 1. Finds up to limit tasks in the source status
// 2. Atomically moves each from source status -> "in-progress"
// 3. Returns the leased node IDs
//
// If sourceStatus is already "in-progress", this will just return tasks that are
// already in-progress (useful for recovery scenarios).
func (db *DB) LeaseTasksFromStatus(queueType string, level int, sourceStatus string, limit int) ([][]byte, error) {
	var nodeIDs [][]byte

	// Use Update (write transaction) to atomically move tasks
	err := db.Update(func(tx *bolt.Tx) error {
		sourceBucket := getBucket(tx, GetStatusBucketPath(queueType, level, sourceStatus))
		if sourceBucket == nil {
			return nil // No items in source status
		}

		// Get or create in-progress bucket
		inProgressBucket, err := GetOrCreateStatusBucket(tx, queueType, level, StatusInProgress)
		if err != nil {
			return fmt.Errorf("failed to get in-progress bucket: %w", err)
		}

		// Get status-lookup bucket for verification and updates
		statusLookupBucket, err := GetOrCreateStatusLookupBucket(tx, queueType, level)
		if err != nil {
			return fmt.Errorf("failed to get status-lookup bucket: %w", err)
		}

		cursor := sourceBucket.Cursor()
		count := 0

		for k, _ := cursor.First(); k != nil && count < limit; k, _ = cursor.Next() {
			// Verify via status-lookup that this task is still in source status
			// This prevents leasing tasks that have been transitioned but not yet flushed
			currentStatusBytes := statusLookupBucket.Get(k)
			if currentStatusBytes == nil {
				// No status-lookup entry - this shouldn't happen, but skip to be safe
				continue
			}

			currentStatus := string(currentStatusBytes)
			if currentStatus != sourceStatus {
				// Status has changed - skip this task (it's been transitioned by another worker)
				continue
			}

			// Atomically move from source status to in-progress
			// 1. Delete from source status bucket
			if err := sourceBucket.Delete(k); err != nil {
				return fmt.Errorf("failed to delete from source status bucket: %w", err)
			}

			// 2. Add to in-progress bucket
			if err := inProgressBucket.Put(k, []byte{}); err != nil {
				return fmt.Errorf("failed to add to in-progress bucket: %w", err)
			}

			// 3. Update status-lookup to reflect in-progress status
			if err := UpdateStatusLookup(tx, queueType, level, k, StatusInProgress); err != nil {
				return fmt.Errorf("failed to update status-lookup: %w", err)
			}

			// 4. Update node's TraversalStatus in nodes bucket
			nodesBucket := getBucket(tx, GetNodesBucketPath(queueType))
			if nodesBucket != nil {
				nodeData := nodesBucket.Get(k)
				if nodeData != nil {
					ns, err := DeserializeNodeState(nodeData)
					if err == nil {
						ns.TraversalStatus = StatusInProgress
						updatedData, err := ns.Serialize()
						if err == nil {
							nodesBucket.Put(k, updatedData)
						}
					}
				}
			}

			// Task successfully leased - add to return list
			keyCopy := make([]byte, len(k))
			copy(keyCopy, k)
			nodeIDs = append(nodeIDs, keyCopy)
			count++
		}

		return nil
	})

	return nodeIDs, err
}

// BatchFetchWithKeys fetches up to limit NodeStates from a status bucket with their keys.
// This is used for task leasing in the queue system.
type FetchResult struct {
	Key   string // ULID for deduplication tracking
	State *NodeState
}

func BatchFetchWithKeys(db *DB, queueType string, level int, status string, limit int) ([]FetchResult, error) {
	var results []FetchResult

	err := db.View(func(tx *bolt.Tx) error {
		statusBucket := getBucket(tx, GetStatusBucketPath(queueType, level, status))
		if statusBucket == nil {
			return nil // No items in this status
		}

		nodesBucket := getBucket(tx, GetNodesBucketPath(queueType))
		if nodesBucket == nil {
			return fmt.Errorf("nodes bucket not found for %s", queueType)
		}

		cursor := statusBucket.Cursor()
		count := 0

		for nodeIDBytes, _ := cursor.First(); nodeIDBytes != nil && count < limit; nodeIDBytes, _ = cursor.Next() {
			// Get the node state from nodes bucket using ULID
			nodeData := nodesBucket.Get(nodeIDBytes)
			if nodeData == nil {
				continue // Node was deleted
			}

			state, err := DeserializeNodeState(nodeData)
			if err != nil {
				continue // Skip invalid entries
			}

			// Use ULID as key for deduplication
			keyStr := string(nodeIDBytes)

			results = append(results, FetchResult{
				Key:   keyStr,
				State: state,
			})
			count++
		}

		return nil
	})

	return results, err
}

// CountByPrefix counts nodes at a specific level and status.
// This is a compatibility wrapper for the old prefix-based counting.
func (db *DB) CountByPrefix(queueType string, level int, status string) (int, error) {
	return db.CountStatusBucket(queueType, level, status)
}
