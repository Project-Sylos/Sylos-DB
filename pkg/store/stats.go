// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package store

import (
	"github.com/Project-Sylos/Sylos-DB/pkg/bolt"
)

// GetLevelProgress retrieves the count of nodes in each status at a specific level.
// Returns counts for pending, completed (successful), and failed nodes.
func (s *Store) GetLevelProgress(queueType string, level int) (pending, completed, failed int, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check conflicts and flush if needed
	if err := s.checkConflict("stats"); err != nil {
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
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check conflicts and flush if needed
	if err := s.checkConflict("stats"); err != nil {
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
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check conflicts and flush if needed
	if err := s.checkConflict("stats"); err != nil {
		return 0, err
	}

	// Use CountByPrefix from bolt package (which internally uses CountStatusBucket)
	return s.db.CountByPrefix(queueType, level, status)
}

// CountNodes returns the total number of nodes in the nodes bucket for a queue type.
func (s *Store) CountNodes(queueType string) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check conflicts and flush if needed
	if err := s.checkConflict("stats"); err != nil {
		return 0, err
	}

	return s.db.CountNodes(queueType)
}
