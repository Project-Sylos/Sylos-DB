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

	// Queue the write operation
	return s.queueWrite(func(db *bolt.DB) error {
		return bolt.InsertNodeWithIndex(db, queueType, level, status, state)
	}, "nodes", "status", "status-lookup", "children", "stats")
}

// TransitionNodeStatus atomically transitions a node from one status to another.
// This consolidates: node state update, status bucket membership changes, status-lookup update,
// and stats updates (decrement old status, increment new status).
func (s *Store) TransitionNodeStatus(queueType string, level int, oldStatus, newStatus string, nodeID string) error {
	if nodeID == "" {
		return fmt.Errorf("node ID cannot be empty")
	}

	// For reads after writes, we need to check conflicts
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if we need to flush before this operation (in case we need to read current state)
	if err := s.checkConflict("nodes", "status"); err != nil {
		return err
	}

	// Queue the write operation
	return s.queueWrite(func(db *bolt.DB) error {
		// Use the public UpdateNodeByID method
		_, err := bolt.UpdateNodeByID(db, queueType, level, "status", oldStatus, newStatus, nodeID)
		return err
	}, "nodes", "status", "status-lookup", "stats")
}

// UpdateNodeCopyStatus updates only the copy status field of a node.
// This does not affect status bucket membership or stats (copy status is not tracked in status buckets).
func (s *Store) UpdateNodeCopyStatus(nodeID string, newCopyStatus string) error {
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
	}, "nodes")
}

// DeleteNode removes a node and all its associated indexes and stats.
func (s *Store) DeleteNode(queueType string, nodeID string) error {
	if nodeID == "" {
		return fmt.Errorf("node ID cannot be empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Check conflicts before reading node state
	if err := s.checkConflict("nodes", "status"); err != nil {
		return err
	}

	// Queue the write operation
	return s.queueWrite(func(db *bolt.DB) error {
		return bolt.BatchDeleteNodes(db, queueType, []string{nodeID})
	}, "nodes", "status", "status-lookup", "children", "stats")
}
