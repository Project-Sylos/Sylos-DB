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

	// Check conflicts before reading node state (all levels since we don't know which level)
	qr := QueueRound{QueueType: queueType, Level: -1}
	if err := s.checkDomainConflict(DomainDependency{
		Nodes: []QueueRound{qr},
	}); err != nil {
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
	}, DomainImpact{
		Nodes:     []QueueRound{qr},
		Exclusion: true,
	})
}

// SweepInheritedExclusions performs an exclusion sweep at a specific level.
// This scans the exclusion holding bucket and updates affected nodes.
func (s *Store) SweepInheritedExclusions(queueType string, level int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	qr := QueueRound{QueueType: queueType, Level: level}

	// Check conflicts before sweep
	if err := s.checkDomainConflict(DomainDependency{
		Nodes:     []QueueRound{qr},
		Exclusion: true,
	}); err != nil {
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
	}, DomainImpact{
		Nodes:     []QueueRound{qr},
		Exclusion: true,
	})
}

// CheckExclusionStatus checks if a node is excluded and whether it's inherited.
func (s *Store) CheckExclusionStatus(queueType string, nodeID string) (excluded bool, inherited bool, err error) {
	if nodeID == "" {
		return false, false, fmt.Errorf("node ID cannot be empty")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check conflicts before reading
	qr := QueueRound{QueueType: queueType, Level: -1}
	if err := s.checkDomainConflict(DomainDependency{
		Nodes: []QueueRound{qr},
	}); err != nil {
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
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check conflicts before reading
	if err := s.checkDomainConflict(DomainDependency{
		Exclusion: true,
	}); err != nil {
		return nil, false, err
	}

	return bolt.ScanExclusionHoldingBucketByLevel(s.db, queueType, level, limit)
}

// CheckHoldingEntry checks if a node has a holding entry and returns the mode.
func (s *Store) CheckHoldingEntry(queueType string, nodeID string) (exists bool, mode string, err error) {
	if nodeID == "" {
		return false, "", fmt.Errorf("node ID cannot be empty")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check conflicts before reading
	if err := s.checkDomainConflict(DomainDependency{
		Exclusion: true,
	}); err != nil {
		return false, "", err
	}

	return bolt.CheckHoldingEntry(s.db, queueType, nodeID)
}

// AddHoldingEntry adds a node to the exclusion holding bucket.
func (s *Store) AddHoldingEntry(queueType string, nodeID string, depth int, mode string) error {
	if nodeID == "" {
		return fmt.Errorf("node ID cannot be empty")
	}

	// Queue the write operation
	return s.queueWrite(func(db *bolt.DB) error {
		return bolt.AddHoldingEntry(db, queueType, nodeID, depth, mode)
	}, DomainImpact{
		Exclusion: true,
	})
}

// RemoveHoldingEntry removes a node from the exclusion holding bucket.
func (s *Store) RemoveHoldingEntry(queueType string, nodeID string, mode string) error {
	if nodeID == "" {
		return fmt.Errorf("node ID cannot be empty")
	}

	// Queue the write operation
	return s.queueWrite(func(db *bolt.DB) error {
		return bolt.RemoveHoldingEntry(db, queueType, nodeID, mode)
	}, DomainImpact{
		Exclusion: true,
	})
}
