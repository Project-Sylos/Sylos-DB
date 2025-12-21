// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package store

import (
	"github.com/Project-Sylos/Sylos-DB/pkg/bolt"
	bbolt "go.etcd.io/bbolt"
)

// GetLevelProgress retrieves the count of nodes in each status at a specific level.
// Returns counts for pending, completed (successful), and failed nodes.
func (s *Store) GetLevelProgress(queueType string, level int) (pending, completed, failed int, err error) {
	topic := getTopicForQueueType(queueType)

	// Check conflicts: reads from stats bucket
	bucketsToRead := [][]string{
		{"Traversal-Data", "STATS"}, // Stats bucket
	}
	if err := s.checkConflict(topic, bucketsToRead); err != nil {
		return 0, 0, 0, err
	}

	// Get counts from stats bucket
	pendingCount, err := s.db.GetBucketCount(bolt.GetStatusBucketPath(queueType, level, bolt.StatusPending))
	if err != nil {
		return 0, 0, 0, err
	}

	completedCount, err := s.db.GetBucketCount(bolt.GetStatusBucketPath(queueType, level, bolt.StatusSuccessful))
	if err != nil {
		return 0, 0, 0, err
	}

	failedCount, err := s.db.GetBucketCount(bolt.GetStatusBucketPath(queueType, level, bolt.StatusFailed))
	if err != nil {
		return 0, 0, 0, err
	}

	return int(pendingCount), int(completedCount), int(failedCount), nil
}

// GetQueueDepth returns the total number of pending nodes across all levels.
func (s *Store) GetQueueDepth(queueType string) (int, error) {
	topic := getTopicForQueueType(queueType)

	// Check conflicts: reads from stats bucket
	bucketsToRead := [][]string{
		{"Traversal-Data", "STATS"}, // Stats bucket
	}
	if err := s.checkConflict(topic, bucketsToRead); err != nil {
		return 0, err
	}

	// Get all levels
	levels, err := s.db.GetAllLevels(queueType)
	if err != nil {
		return 0, err
	}

	// Sum pending counts across all levels
	total := 0
	for _, level := range levels {
		count, err := s.db.GetBucketCount(bolt.GetStatusBucketPath(queueType, level, bolt.StatusPending))
		if err != nil {
			return 0, err
		}
		total += int(count)
	}

	return total, nil
}

// CountStatusAtLevel counts nodes at a specific level with a specific status.
// This is a general-purpose count method that works for any status (not just pending/successful/failed).
func (s *Store) CountStatusAtLevel(queueType string, level int, status string) (int, error) {
	topic := getTopicForQueueType(queueType)

	// Check conflicts: reads from stats bucket
	bucketsToRead := [][]string{
		{"Traversal-Data", "STATS"}, // Stats bucket
	}
	if err := s.checkConflict(topic, bucketsToRead); err != nil {
		return 0, err
	}

	// Use CountByPrefix from bolt package (which internally uses CountStatusBucket)
	return s.db.CountByPrefix(queueType, level, status)
}

// CountNodes returns the total number of nodes in the nodes bucket for a queue type.
func (s *Store) CountNodes(queueType string) (int, error) {
	topic := getTopicForQueueType(queueType)

	// Check conflicts: reads from stats bucket
	bucketsToRead := [][]string{
		{"Traversal-Data", "STATS"}, // Stats bucket
	}
	if err := s.checkConflict(topic, bucketsToRead); err != nil {
		return 0, err
	}

	return s.db.CountNodes(queueType)
}

// SetQueueStats writes queue statistics to the queue-stats bucket.
// queueKey is the key for the stats (e.g., "src-traversal", "dst-traversal").
// statsJSON is the JSON-encoded stats data to store.
func (s *Store) SetQueueStats(queueKey string, statsJSON []byte) error {
	// Queue stats bucket is shared - use "src" topic as default
	bucketsToWrite := [][]string{
		bolt.GetQueueStatsBucketPath(),
	}
	return s.queueWrite("src", func(tx *bbolt.Tx) error {
		return bolt.SetQueueStatsInTx(tx, queueKey, statsJSON)
	}, bucketsToWrite, nil)
}

// SetQueueStatsBatch writes multiple queue statistics in a single transaction.
// statsMap is a map of queue key -> JSON-encoded stats.
// This is used by QueueObserver to publish metrics for multiple queues at once.
func (s *Store) SetQueueStatsBatch(statsMap map[string][]byte) error {
	// Queue stats bucket is shared - use "src" topic as default
	bucketsToWrite := [][]string{
		bolt.GetQueueStatsBucketPath(),
	}
	return s.queueWrite("src", func(tx *bbolt.Tx) error {
		return bolt.SetQueueStatsBatchInTx(tx, statsMap)
	}, bucketsToWrite, nil)
}

// GetQueueStats retrieves queue statistics from the queue-stats bucket.
// Returns the JSON-encoded stats for the specified queue key (e.g., "src-traversal", "dst-traversal").
// Returns nil if the stats don't exist.
//
// NOTE: Queue stats are eventually consistent - this method does NOT flush the buffer.
// Queue stats are used for API polling (200ms interval) and don't require immediate consistency.
func (s *Store) GetQueueStats(queueKey string) ([]byte, error) {
	// Queue stats are eventually consistent - no conflict check needed
	// API polling can tolerate slight delays (buffer flushes every 2 seconds by default)

	return s.db.GetQueueStats(queueKey)
}

// GetAllQueueStats retrieves all queue statistics from the queue-stats bucket.
// Returns a map of queue key -> JSON-encoded stats.
//
// NOTE: Queue stats are eventually consistent - this method does NOT flush the buffer.
// Queue stats are used for API polling (200ms interval) and don't require immediate consistency.
func (s *Store) GetAllQueueStats() (map[string][]byte, error) {
	// Queue stats are eventually consistent - no conflict check needed
	// API polling can tolerate slight delays (buffer flushes every 2 seconds by default)

	return s.db.GetAllQueueStats()
}
