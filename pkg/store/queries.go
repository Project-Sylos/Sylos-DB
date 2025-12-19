// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package store

import (
	"fmt"

	"github.com/Project-Sylos/Sylos-DB/pkg/bolt"
)

// GetNode retrieves a node by its ULID.
// Automatically flushes buffer if there are pending writes to the nodes bucket.
func (s *Store) GetNode(nodeID string) (*bolt.NodeState, error) {
	if nodeID == "" {
		return nil, fmt.Errorf("node ID cannot be empty")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check for conflicts and flush if needed
	if err := s.checkConflict("nodes"); err != nil {
		return nil, err
	}

	// Try SRC first, then DST
	node, err := bolt.GetNodeState(s.db, "SRC", nodeID)
	if err == nil {
		return node, nil
	}

	return bolt.GetNodeState(s.db, "DST", nodeID)
}

// GetNodeByPath retrieves a node by its path using the path-to-ULID lookup.
// Automatically flushes buffer if there are pending writes to path-to-ulid or nodes buckets.
func (s *Store) GetNodeByPath(queueType, path string) (*bolt.NodeState, error) {
	if path == "" {
		return nil, fmt.Errorf("path cannot be empty")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check for conflicts and flush if needed
	if err := s.checkConflict("path-to-ulid", "nodes"); err != nil {
		return nil, err
	}

	// Get ULID from path
	nodeID, err := bolt.GetULIDFromPathOrHash(s.db, queueType, path)
	if err != nil {
		return nil, fmt.Errorf("failed to get ULID from path: %w", err)
	}
	if nodeID == "" {
		return nil, fmt.Errorf("node not found for path: %s", path)
	}

	// Get node state
	return bolt.GetNodeState(s.db, queueType, nodeID)
}

// GetChildren retrieves the child node IDs for a given parent node.
// Automatically flushes buffer if there are pending writes to the children bucket.
func (s *Store) GetChildren(queueType, parentID string) ([]string, error) {
	if parentID == "" {
		return nil, fmt.Errorf("parent ID cannot be empty")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check for conflicts and flush if needed
	if err := s.checkConflict("children"); err != nil {
		return nil, err
	}

	// Get children IDs
	result, err := bolt.GetChildrenByParentID(s.db, queueType, parentID, "ids")
	if err != nil {
		return nil, err
	}

	if childIDs, ok := result.([]string); ok {
		return childIDs, nil
	}

	return nil, fmt.Errorf("unexpected result type from GetChildrenByParentID")
}

// ListPendingAtLevel retrieves pending nodes at a specific level.
// Automatically flushes buffer if there are pending writes to status buckets.
// Limit of 0 means no limit.
func (s *Store) ListPendingAtLevel(queueType string, level int, limit int) ([]*bolt.NodeState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check for conflicts and flush if needed
	if err := s.checkConflict("status", "nodes"); err != nil {
		return nil, err
	}

	// Iterate pending status bucket
	var nodes []*bolt.NodeState
	opts := bolt.IteratorOptions{Limit: limit}
	err := s.db.IterateStatusBucket(queueType, level, bolt.StatusPending, opts, func(nodeID []byte) error {
		// Get node state
		state, err := bolt.GetNodeState(s.db, queueType, string(nodeID))
		if err != nil {
			return err
		}
		nodes = append(nodes, state)
		return nil
	})

	if err != nil {
		return nil, err
	}

	return nodes, nil
}

// HasPendingAtLevel checks if there are any pending nodes at a specific level.
// This is optimized for the common "is round complete?" check.
func (s *Store) HasPendingAtLevel(queueType string, level int) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check for conflicts and flush if needed
	if err := s.checkConflict("status", "stats"); err != nil {
		return false, err
	}

	// Use stats for O(1) check
	count, err := s.db.GetBucketCount(bolt.GetStatusBucketPath(queueType, level, bolt.StatusPending))
	if err != nil {
		return false, err
	}

	return count > 0, nil
}

// GetAllLevels returns all level numbers that exist for a queue type.
func (s *Store) GetAllLevels(queueType string) ([]int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check conflicts and flush if needed
	if err := s.checkConflict("levels"); err != nil {
		return nil, err
	}

	return s.db.GetAllLevels(queueType)
}

// GetMaxKnownDepth scans the levels bucket and returns the highest level number found.
// Returns -1 if no levels exist.
func (s *Store) GetMaxKnownDepth(queueType string) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check conflicts and flush if needed
	if err := s.checkConflict("levels"); err != nil {
		return -1, err
	}

	maxDepth := s.db.GetMaxKnownDepth(queueType)
	return maxDepth, nil
}

// LeaseResult represents a leased task for worker processing.
type LeaseResult struct {
	Key   string // ULID for deduplication tracking
	State *bolt.NodeState
}

// LeaseTasksAtLevel leases tasks (fetches nodes) from a status bucket at a specific level.
// This is used for worker task leasing. Returns node IDs and states.
func (s *Store) LeaseTasksAtLevel(queueType string, level int, status string, limit int) ([]LeaseResult, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check conflicts and flush if needed (lease depends on status bucket)
	if err := s.checkConflict("status", "nodes"); err != nil {
		return nil, err
	}

	// Use the bolt package's BatchFetchWithKeys
	results, err := bolt.BatchFetchWithKeys(s.db, queueType, level, status, limit)
	if err != nil {
		return nil, err
	}

	// Convert FetchResult to LeaseResult
	leaseResults := make([]LeaseResult, len(results))
	for i, result := range results {
		leaseResults[i] = LeaseResult{
			Key:   result.Key,
			State: result.State,
		}
	}

	return leaseResults, nil
}
