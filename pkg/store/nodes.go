// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package store

import (
	"fmt"

	"github.com/Project-Sylos/Sylos-DB/pkg/bolt"
)

// RegisterNode atomically inserts a node into the database with all necessary indexes and stats updates.
// This consolidates: node insertion, status bucket membership, status-lookup index, children index,
// stats updates, and optional SRC↔DST join mappings.
func (s *Store) RegisterNode(queueType string, level int, status string, state *bolt.NodeState) error {
	if state.ID == "" {
		return fmt.Errorf("node ID (ULID) cannot be empty")
	}

	qr := QueueRound{QueueType: queueType, Level: level}

	// Queue the write operation with domain impact declaration
	return s.queueWrite(func(db *bolt.DB) error {
		return bolt.InsertNodeWithIndex(db, queueType, level, status, state)
	}, DomainImpact{
		Pending: []QueueRound{qr},     // Affects pending if status is "pending"
		Status:  []QueueRound{qr},     // Affects status bucket for this level
		Nodes:   []QueueRound{qr},     // Affects node data at this level
		Lookups: state.ParentID != "", // Affects lookups if has parent (children index)
		Stats:   true,                 // Always affects stats
	})
}

// TransitionNodeStatus atomically transitions a node from one status to another.
// This consolidates: node state update, status bucket membership changes, status-lookup update,
// and stats updates (decrement old status, increment new status).
func (s *Store) TransitionNodeStatus(queueType string, level int, oldStatus, newStatus string, nodeID string) error {
	if nodeID == "" {
		return fmt.Errorf("node ID cannot be empty")
	}

	qr := QueueRound{QueueType: queueType, Level: level}

	// For reads after writes, we need to check conflicts
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if we need to flush before this operation (in case we need to read current state)
	if err := s.checkDomainConflict(DomainDependency{
		Nodes:  []QueueRound{qr},
		Status: []QueueRound{qr},
	}); err != nil {
		return err
	}

	// Queue the write operation with domain impact
	return s.queueWrite(func(db *bolt.DB) error {
		// Use the public UpdateNodeByID method
		_, err := bolt.UpdateNodeByID(db, queueType, level, "status", oldStatus, newStatus, nodeID)
		return err
	}, DomainImpact{
		Pending: []QueueRound{qr}, // May affect pending bucket
		Status:  []QueueRound{qr}, // Affects status buckets
		Nodes:   []QueueRound{qr}, // Affects node data
		Stats:   true,             // Affects stats
	})
}

// UpdateNodeCopyStatus updates only the copy status field of a node.
// This does not affect status bucket membership or stats (copy status is not tracked in status buckets).
func (s *Store) UpdateNodeCopyStatus(nodeID string, newCopyStatus string) error {
	if nodeID == "" {
		return fmt.Errorf("node ID cannot be empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Check conflicts before reading node state (all levels for both queues)
	// We don't know which level the node is at, so we flush if any nodes are dirty
	// This is conservative but correct
	if err := s.checkDomainConflict(DomainDependency{
		Nodes: []QueueRound{
			{QueueType: "SRC", Level: -1}, // -1 indicates "all levels"
			{QueueType: "DST", Level: -1},
		},
	}); err != nil {
		return err
	}

	// Queue the write operation
	// Note: We need to determine queueType by checking which bucket has the node
	// For now, try both SRC and DST
	return s.queueWrite(func(db *bolt.DB) error {
		// Try SRC first
		_, err := bolt.UpdateNodeByID(db, "SRC", 0, "copy", "", newCopyStatus, nodeID)
		if err == nil {
			return nil
		}
		// Try DST
		_, err = bolt.UpdateNodeByID(db, "DST", 0, "copy", "", newCopyStatus, nodeID)
		return err
	}, DomainImpact{
		Nodes: []QueueRound{
			{QueueType: "SRC", Level: -1}, // Conservative: mark all SRC nodes dirty
			{QueueType: "DST", Level: -1}, // Conservative: mark all DST nodes dirty
		},
	})
}

// SetNodeExclusionFlag updates the ExplicitExcluded flag on a node without affecting status buckets.
// This is used primarily by test utilities to mark nodes as excluded or unexcluded.
// This does not affect status bucket membership or stats (exclusion flags are not tracked in status buckets).
func (s *Store) SetNodeExclusionFlag(queueType, nodeID string, explicitExcluded bool) error {
	if nodeID == "" {
		return fmt.Errorf("node ID cannot be empty")
	}
	if queueType == "" {
		return fmt.Errorf("queue type cannot be empty")
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
		nodeState, err := bolt.GetNodeState(db, queueType, nodeID)
		if err != nil {
			return fmt.Errorf("failed to get node state: %w", err)
		}
		if nodeState == nil {
			return fmt.Errorf("node not found: %s", nodeID)
		}

		// Update exclusion flag
		nodeState.ExplicitExcluded = explicitExcluded

		// Save updated node state
		return bolt.SetNodeState(db, queueType, []byte(nodeID), nodeState)
	}, DomainImpact{
		Nodes: []QueueRound{qr}, // Mark nodes at all levels as dirty
	})
}

// DeleteNode removes a node and all its associated indexes and stats.
func (s *Store) DeleteNode(queueType string, nodeID string) error {
	if nodeID == "" {
		return fmt.Errorf("node ID cannot be empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Check conflicts before reading node state (all levels since we don't know which level)
	qr := QueueRound{QueueType: queueType, Level: -1}
	if err := s.checkDomainConflict(DomainDependency{
		Nodes:  []QueueRound{qr},
		Status: []QueueRound{qr},
	}); err != nil {
		return err
	}

	// Queue the write operation with domain impact
	return s.queueWrite(func(db *bolt.DB) error {
		return bolt.BatchDeleteNodes(db, queueType, []string{nodeID})
	}, DomainImpact{
		Pending: []QueueRound{qr}, // May affect pending
		Status:  []QueueRound{qr}, // Affects status buckets
		Nodes:   []QueueRound{qr}, // Affects node data
		Lookups: true,             // Affects children index
		Stats:   true,             // Affects stats
	})
}
