// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package store

import (
	"fmt"

	"github.com/Project-Sylos/Sylos-DB/pkg/bolt"
)

// MarkExcluded marks a node as excluded (either explicitly or inherited).
// This updates the node state and optionally moves it to the excluded status bucket.
func (s *Store) MarkExcluded(queueType string, nodeID string, inherited bool) error {
	if nodeID == "" {
		return fmt.Errorf("node ID cannot be empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Check conflicts before reading node state
	if err := s.checkConflict("nodes"); err != nil {
		return err
	}

	// Queue the write operation
	return s.queueWrite(func(db *bolt.DB) error {
		// Get current node state
		state, err := bolt.GetNodeState(db, queueType, nodeID)
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
		return bolt.SetNodeState(db, queueType, []byte(nodeID), state)
	}, "nodes")
}

// SweepInheritedExclusions performs an exclusion sweep at a specific level.
// This scans the exclusion holding bucket and updates affected nodes.
func (s *Store) SweepInheritedExclusions(queueType string, level int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check conflicts before sweep
	if err := s.checkConflict("nodes", "exclusion-holding"); err != nil {
		return err
	}

	// Queue the write operation
	return s.queueWrite(func(db *bolt.DB) error {
		// Scan exclusion holding bucket (limit 0 = no limit)
		entries, hasMore, err := bolt.ScanExclusionHoldingBucketByLevel(db, queueType, level, 0)
		if err != nil {
			return fmt.Errorf("failed to scan exclusion holding bucket: %w", err)
		}
		_ = hasMore // Ignore for now

		// Update each affected node
		for _, entry := range entries {
			state, err := bolt.GetNodeState(db, queueType, entry.NodeID)
			if err != nil {
				continue // Skip nodes that don't exist
			}

			state.InheritedExcluded = true

			if err := bolt.SetNodeState(db, queueType, []byte(entry.NodeID), state); err != nil {
				return fmt.Errorf("failed to update node %s: %w", entry.NodeID, err)
			}
		}

		return nil
	}, "nodes", "exclusion-holding")
}

// CheckExclusionStatus checks if a node is excluded and whether it's inherited.
func (s *Store) CheckExclusionStatus(queueType string, nodeID string) (excluded bool, inherited bool, err error) {
	if nodeID == "" {
		return false, false, fmt.Errorf("node ID cannot be empty")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check conflicts and flush if needed
	if err := s.checkConflict("nodes"); err != nil {
		return false, false, err
	}

	// Get node state
	state, err := bolt.GetNodeState(s.db, queueType, nodeID)
	if err != nil {
		return false, false, err
	}

	excluded = state.ExplicitExcluded || state.InheritedExcluded
	inherited = state.InheritedExcluded

	return excluded, inherited, nil
}

// ScanExclusionHoldingAtLevel scans the exclusion holding bucket for entries at a specific level.
// Returns entries, a flag indicating if higher levels exist, and any error.
func (s *Store) ScanExclusionHoldingAtLevel(queueType string, level int, limit int) ([]bolt.ExclusionEntry, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check conflicts and flush if needed
	if err := s.checkConflict("exclusion-holding"); err != nil {
		return nil, false, err
	}

	return bolt.ScanExclusionHoldingBucketByLevel(s.db, queueType, level, limit)
}

// CheckHoldingEntry checks if a node exists in either exclusion or unexclusion holding bucket.
// Returns (exists, mode, error) where mode is "exclude", "unexclude", or "" if not found.
func (s *Store) CheckHoldingEntry(queueType string, nodeID string) (bool, string, error) {
	if nodeID == "" {
		return false, "", fmt.Errorf("node ID cannot be empty")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check conflicts and flush if needed
	if err := s.checkConflict("exclusion-holding"); err != nil {
		return false, "", err
	}

	return bolt.CheckHoldingEntry(s.db, queueType, nodeID)
}

// AddHoldingEntry adds a node to the exclusion or unexclusion holding bucket.
// mode should be "exclude" or "unexclude".
func (s *Store) AddHoldingEntry(queueType string, nodeID string, depth int, mode string) error {
	if nodeID == "" {
		return fmt.Errorf("node ID cannot be empty")
	}
	if mode != "exclude" && mode != "unexclude" {
		return fmt.Errorf("mode must be 'exclude' or 'unexclude', got '%s'", mode)
	}

	// Queue the write operation
	return s.queueWrite(func(db *bolt.DB) error {
		return bolt.AddHoldingEntry(db, queueType, nodeID, depth, mode)
	}, "exclusion-holding")
}

// RemoveHoldingEntry removes a node from the exclusion or unexclusion holding bucket.
// mode should be "exclude" or "unexclude".
func (s *Store) RemoveHoldingEntry(queueType string, nodeID string, mode string) error {
	if nodeID == "" {
		return fmt.Errorf("node ID cannot be empty")
	}
	if mode != "exclude" && mode != "unexclude" {
		return fmt.Errorf("mode must be 'exclude' or 'unexclude', got '%s'", mode)
	}

	// Queue the write operation
	return s.queueWrite(func(db *bolt.DB) error {
		return bolt.RemoveHoldingEntry(db, queueType, nodeID, mode)
	}, "exclusion-holding")
}
