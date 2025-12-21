// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package store

import (
	"fmt"

	"github.com/Project-Sylos/Sylos-DB/pkg/bolt"
	bbolt "go.etcd.io/bbolt"
)

// MarkExcluded marks a node as excluded (either explicitly or inherited).
// This updates the node state and optionally moves it to the excluded status bucket.
func (s *Store) MarkExcluded(queueType string, nodeID string, inherited bool) error {
	if nodeID == "" {
		return fmt.Errorf("node ID cannot be empty")
	}

	topic := getTopicForQueueType(queueType)

	// Check conflicts: reads from nodes bucket
	bucketsToRead := [][]string{
		bolt.GetNodesBucketPath(queueType),
	}
	if err := s.checkConflict(topic, bucketsToRead); err != nil {
		return err
	}

	// Queue the write operation: writes to nodes bucket
	bucketsToWrite := [][]string{
		bolt.GetNodesBucketPath(queueType),
	}
	return s.queueWrite(topic, func(tx *bbolt.Tx) error {
		// Get current node state
		state, err := bolt.GetNodeStateInTx(tx, queueType, nodeID)
		if err != nil {
			return fmt.Errorf("failed to get node state: %w", err)
		}

		// Update exclusion flags
		if inherited {
			state.InheritedExcluded = true
		} else {
			state.ExplicitExcluded = true
		}

		// Write back to database
		return bolt.SetNodeStateInTx(tx, queueType, []byte(nodeID), state)
	}, bucketsToWrite, nil)
}

// SweepInheritedExclusions performs an exclusion sweep at a specific level.
// This scans the exclusion holding bucket and updates affected nodes.
func (s *Store) SweepInheritedExclusions(queueType string, level int) error {
	topic := getTopicForQueueType(queueType)

	// Check conflicts: reads from exclusion holding bucket and nodes bucket
	bucketsToRead := [][]string{
		bolt.GetExclusionHoldingBucketPath(queueType),
		bolt.GetNodesBucketPath(queueType),
	}
	if err := s.checkConflict(topic, bucketsToRead); err != nil {
		return err
	}

	// Queue the write operation: writes to nodes bucket
	bucketsToWrite := [][]string{
		bolt.GetNodesBucketPath(queueType),
	}
	return s.queueWrite(topic, func(tx *bbolt.Tx) error {
		// Scan exclusion holding bucket (limit 0 = no limit)
		entries, hasMore, err := bolt.ScanExclusionHoldingBucketByLevelInTx(tx, queueType, level, 0)
		if err != nil {
			return fmt.Errorf("failed to scan exclusion holding bucket: %w", err)
		}
		_ = hasMore // Ignore for now

		// Update each affected node
		for _, entry := range entries {
			state, err := bolt.GetNodeStateInTx(tx, queueType, entry.NodeID)
			if err != nil {
				continue // Skip nodes that don't exist
			}

			state.InheritedExcluded = true

			if err := bolt.SetNodeStateInTx(tx, queueType, []byte(entry.NodeID), state); err != nil {
				return fmt.Errorf("failed to update node %s: %w", entry.NodeID, err)
			}
		}

		return nil
	}, bucketsToWrite, nil)
}

// CheckExclusionStatus checks if a node is excluded and whether it's inherited.
func (s *Store) CheckExclusionStatus(queueType string, nodeID string) (excluded bool, inherited bool, err error) {
	if nodeID == "" {
		return false, false, fmt.Errorf("node ID cannot be empty")
	}

	topic := getTopicForQueueType(queueType)

	// Check conflicts: reads from nodes bucket
	bucketsToRead := [][]string{
		bolt.GetNodesBucketPath(queueType),
	}
	if err := s.checkConflict(topic, bucketsToRead); err != nil {
		return false, false, err
	}

	state, err := bolt.GetNodeState(s.db, queueType, nodeID)
	if err != nil {
		return false, false, err
	}

	excluded = state.ExplicitExcluded || state.InheritedExcluded
	inherited = state.InheritedExcluded && !state.ExplicitExcluded

	return excluded, inherited, nil
}

// ScanExclusionHoldingAtLevel scans the exclusion holding bucket at a specific level.
// Returns entries and whether there are more entries beyond the limit.
func (s *Store) ScanExclusionHoldingAtLevel(queueType string, level int, limit int) ([]bolt.ExclusionEntry, bool, error) {
	topic := getTopicForQueueType(queueType)

	// Check conflicts: reads from exclusion holding bucket
	bucketsToRead := [][]string{
		bolt.GetExclusionHoldingBucketPath(queueType),
	}
	if err := s.checkConflict(topic, bucketsToRead); err != nil {
		return nil, false, err
	}

	return bolt.ScanExclusionHoldingBucketByLevel(s.db, queueType, level, limit)
}

// CheckHoldingEntry checks if a node has a holding entry and returns the mode.
func (s *Store) CheckHoldingEntry(queueType string, nodeID string) (exists bool, mode string, err error) {
	if nodeID == "" {
		return false, "", fmt.Errorf("node ID cannot be empty")
	}

	topic := getTopicForQueueType(queueType)

	// Check conflicts: reads from exclusion holding buckets
	bucketsToRead := [][]string{
		bolt.GetExclusionHoldingBucketPath(queueType),
		bolt.GetUnexclusionHoldingBucketPath(queueType),
	}
	if err := s.checkConflict(topic, bucketsToRead); err != nil {
		return false, "", err
	}

	return bolt.CheckHoldingEntry(s.db, queueType, nodeID)
}

// AddHoldingEntry adds a node to the exclusion holding bucket.
func (s *Store) AddHoldingEntry(queueType string, nodeID string, depth int, mode string) error {
	if nodeID == "" {
		return fmt.Errorf("node ID cannot be empty")
	}

	topic := getTopicForQueueType(queueType)

	// Determine which holding bucket to write to
	var bucketPath []string
	if mode == "exclude" {
		bucketPath = bolt.GetExclusionHoldingBucketPath(queueType)
	} else {
		bucketPath = bolt.GetUnexclusionHoldingBucketPath(queueType)
	}

	// Queue the write operation
	bucketsToWrite := [][]string{bucketPath}
	return s.queueWrite(topic, func(tx *bbolt.Tx) error {
		return bolt.AddHoldingEntryInTx(tx, queueType, nodeID, depth, mode)
	}, bucketsToWrite, nil)
}

// RemoveHoldingEntry removes a node from the exclusion holding bucket.
func (s *Store) RemoveHoldingEntry(queueType string, nodeID string, mode string) error {
	if nodeID == "" {
		return fmt.Errorf("node ID cannot be empty")
	}

	topic := getTopicForQueueType(queueType)

	// Determine which holding bucket to remove from
	var bucketPath []string
	if mode == "exclude" {
		bucketPath = bolt.GetExclusionHoldingBucketPath(queueType)
	} else {
		bucketPath = bolt.GetUnexclusionHoldingBucketPath(queueType)
	}

	// Queue the write operation
	bucketsToWrite := [][]string{bucketPath}
	return s.queueWrite(topic, func(tx *bbolt.Tx) error {
		return bolt.RemoveHoldingEntryInTx(tx, queueType, nodeID, mode)
	}, bucketsToWrite, nil)
}
