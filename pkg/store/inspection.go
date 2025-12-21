// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package store

import (
	"sort"

	"github.com/Project-Sylos/Sylos-DB/pkg/bolt"
)

// LevelStatus contains status counts for a single level.
type LevelStatus struct {
	Level      int
	Pending    int
	Successful int
	Failed     int
	NotOnSrc   int // DST only
}

// QueueReport contains a complete inspection report for a queue (SRC or DST).
type QueueReport struct {
	QueueType       string
	TotalNodes      int
	Levels          []LevelStatus
	TotalPending    int
	TotalSuccessful int
	TotalFailed     int
	TotalNotOnSrc   int // DST only
	MinPendingLevel *int
}

// DatabaseReport contains a complete inspection report for the entire database.
type DatabaseReport struct {
	Src QueueReport
	Dst QueueReport
}

// InspectionMode determines whether to use stats-based (fast) or scan-based (accurate) counting.
type InspectionMode int

const (
	// InspectionModeStats uses O(1) stats bucket lookups for fast inspection.
	// This is faster but may not reflect actual bucket contents if stats are stale.
	InspectionModeStats InspectionMode = iota

	// InspectionModeScan uses O(n) bucket scans for accurate inspection.
	// This ensures we're counting actual nodes, not cached stats.
	InspectionModeScan
)

// InspectQueue generates a QueueReport for a single queue (SRC or DST).
// mode determines whether to use stats-based (fast) or scan-based (accurate) counting.
func (s *Store) InspectQueue(queueType string, mode InspectionMode) (*QueueReport, error) {

	report := &QueueReport{
		QueueType: queueType,
	}

	// Count total nodes
	var totalNodes int
	var err error
	topic := getTopicForQueueType(queueType)
	bucketsToRead := [][]string{
		{"Traversal-Data", "STATS"}, // Stats bucket
		bolt.GetNodesBucketPath(queueType),
	}

	if mode == InspectionModeScan {
		// Flush buffer to ensure accurate scan
		if err := s.checkConflict(topic, bucketsToRead); err != nil {
			return nil, err
		}
		totalNodes, err = s.db.CountNodes(queueType)
	} else {
		// Stats-based: use GetBucketCount (O(1))
		if err := s.checkConflict(topic, bucketsToRead); err != nil {
			return nil, err
		}
		count, err := s.db.GetBucketCount(bolt.GetNodesBucketPath(queueType))
		if err != nil {
			// Stats might not exist, fall back to scan
			totalNodes, err = s.db.CountNodes(queueType)
			if err != nil {
				return nil, err
			}
		} else {
			totalNodes = int(count)
		}
	}
	if err != nil {
		return nil, err
	}
	report.TotalNodes = totalNodes

	// Get all levels
	levels, err := s.db.GetAllLevels(queueType)
	if err != nil {
		return nil, err
	}

	// Sort levels
	sort.Ints(levels)

	// Inspect each level
	levelMap := make(map[int]*LevelStatus)
	for _, level := range levels {
		levelStatus := &LevelStatus{Level: level}

		// Count pending
		pending, err := s.countStatusAtLevel(queueType, level, bolt.StatusPending, mode)
		if err != nil {
			pending = 0
		}
		levelStatus.Pending = pending

		// Count successful
		successful, err := s.countStatusAtLevel(queueType, level, bolt.StatusSuccessful, mode)
		if err != nil {
			successful = 0
		}
		levelStatus.Successful = successful

		// Count failed
		failed, err := s.countStatusAtLevel(queueType, level, bolt.StatusFailed, mode)
		if err != nil {
			failed = 0
		}
		levelStatus.Failed = failed

		// Count not_on_src (DST only)
		if queueType == "DST" {
			notOnSrc, err := s.countStatusAtLevel(queueType, level, bolt.StatusNotOnSrc, mode)
			if err != nil {
				notOnSrc = 0
			}
			levelStatus.NotOnSrc = notOnSrc
		}

		levelMap[level] = levelStatus
		report.TotalPending += pending
		report.TotalSuccessful += successful
		report.TotalFailed += failed
		if queueType == "DST" {
			report.TotalNotOnSrc += levelStatus.NotOnSrc
		}

		// Track minimum pending level
		if pending > 0 {
			if report.MinPendingLevel == nil || level < *report.MinPendingLevel {
				levelCopy := level
				report.MinPendingLevel = &levelCopy
			}
		}
	}

	// Convert map to sorted slice
	report.Levels = make([]LevelStatus, 0, len(levelMap))
	for _, level := range levels {
		report.Levels = append(report.Levels, *levelMap[level])
	}

	return report, nil
}

// countStatusAtLevel counts nodes at a specific level with a specific status.
// Uses either stats-based (fast) or scan-based (accurate) counting based on mode.
func (s *Store) countStatusAtLevel(queueType string, level int, status string, mode InspectionMode) (int, error) {
	if mode == InspectionModeScan {
		// Use CountStatusBucket which falls back to scan if stats unavailable
		return s.db.CountStatusBucket(queueType, level, status)
	}

	// Stats-based: use GetBucketCount (O(1))
	count, err := s.db.GetBucketCount(bolt.GetStatusBucketPath(queueType, level, status))
	if err != nil {
		// Stats might not exist, fall back to scan
		return s.db.CountStatusBucket(queueType, level, status)
	}
	return int(count), nil
}

// InspectDatabase generates a DatabaseReport for the entire database.
// mode determines whether to use stats-based (fast) or scan-based (accurate) counting.
func (s *Store) InspectDatabase(mode InspectionMode) (*DatabaseReport, error) {
	report := &DatabaseReport{}

	// Inspect SRC
	srcReport, err := s.InspectQueue("SRC", mode)
	if err != nil {
		return nil, err
	}
	report.Src = *srcReport

	// Inspect DST
	dstReport, err := s.InspectQueue("DST", mode)
	if err != nil {
		return nil, err
	}
	report.Dst = *dstReport

	return report, nil
}

// GetMinPendingLevel finds the minimum level that has pending items for a queue.
// Returns -1 if no pending items exist.
func (s *Store) GetMinPendingLevel(queueType string) (int, error) {
	topic := getTopicForQueueType(queueType)

	// Check conflicts: reads from stats bucket
	bucketsToRead := [][]string{
		{"Traversal-Data", "STATS"}, // Stats bucket
	}
	if err := s.checkConflict(topic, bucketsToRead); err != nil {
		return -1, err
	}

	// Use bolt package method
	return s.db.FindMinPendingLevel(queueType)
}

// IsQueueComplete checks if a queue has no pending items.
// Returns true if the queue is complete (no pending), false otherwise.
func (s *Store) IsQueueComplete(queueType string) (bool, error) {
	report, err := s.InspectQueue(queueType, InspectionModeStats)
	if err != nil {
		return false, err
	}
	return report.TotalPending == 0, nil
}

// IsMigrationComplete checks if both SRC and DST queues are complete.
// Returns true if the migration is complete (no pending in either queue), false otherwise.
func (s *Store) IsMigrationComplete() (bool, error) {
	report, err := s.InspectDatabase(InspectionModeStats)
	if err != nil {
		return false, err
	}
	return report.Src.TotalPending == 0 && report.Dst.TotalPending == 0, nil
}
