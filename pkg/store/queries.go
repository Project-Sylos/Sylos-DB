// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package store

import (
	"fmt"

	"github.com/Project-Sylos/Sylos-DB/pkg/bolt"
)

// GetNode retrieves a node's state by its ID.
func (s *Store) GetNode(queueType string, nodeID string) (*bolt.NodeState, error) {
	if nodeID == "" {
		return nil, fmt.Errorf("node ID cannot be empty")
	}

	topic := getTopicForQueueType(queueType)

	// Check conflicts: reads from nodes bucket
	bucketsToRead := [][]string{
		bolt.GetNodesBucketPath(queueType),
	}
	if err := s.checkConflict(topic, bucketsToRead); err != nil {
		return nil, err
	}

	return bolt.GetNodeState(s.db, queueType, nodeID)
}

// GetNodeByPath retrieves a node's state by its filesystem path.
// This uses the path-to-ULID lookup index.
func (s *Store) GetNodeByPath(queueType string, path string) (*bolt.NodeState, error) {
	if path == "" {
		return nil, fmt.Errorf("path cannot be empty")
	}

	topic := getTopicForQueueType(queueType)

	// Check conflicts: reads from path-to-ulid lookup and nodes bucket
	bucketsToRead := [][]string{
		bolt.GetPathToULIDBucketPath(queueType),
		bolt.GetNodesBucketPath(queueType),
	}
	if err := s.checkConflict(topic, bucketsToRead); err != nil {
		return nil, err
	}

	// Look up ULID from path
	nodeID, err := bolt.GetULIDFromPathOrHash(s.db, queueType, path)
	if err != nil {
		return nil, fmt.Errorf("failed to look up node ID: %w", err)
	}
	if nodeID == "" {
		return nil, fmt.Errorf("node not found for path: %s", path)
	}

	// Get node state
	return bolt.GetNodeState(s.db, queueType, nodeID)
}

// GetChildren retrieves child node information for a given parent node.
// returnType can be "ids" (returns IDs only) or "states" (returns full NodeState objects).
func (s *Store) GetChildren(queueType string, parentID string, returnType string) (interface{}, error) {
	if parentID == "" {
		return nil, fmt.Errorf("parent ID cannot be empty")
	}

	topic := getTopicForQueueType(queueType)

	// Check conflicts: reads from children bucket and nodes bucket
	bucketsToRead := [][]string{
		bolt.GetChildrenBucketPath(queueType),
		bolt.GetNodesBucketPath(queueType),
	}
	if err := s.checkConflict(topic, bucketsToRead); err != nil {
		return nil, err
	}

	switch returnType {
	case "ids":
		return bolt.GetChildrenByParentID(s.db, queueType, parentID, "ids")
	case "states":
		return bolt.GetChildrenByParentID(s.db, queueType, parentID, "states")
	default:
		return nil, fmt.Errorf("invalid return type: %s (must be 'ids' or 'states')", returnType)
	}
}

// ListPendingAtLevel retrieves a list of pending nodes at a specific level.
// Returns up to 'limit' nodes (0 = no limit).
func (s *Store) ListPendingAtLevel(queueType string, level int, limit int) ([]*bolt.NodeState, error) {
	topic := getTopicForQueueType(queueType)

	// Check conflicts: reads from status bucket (pending) and nodes bucket
	bucketsToRead := [][]string{
		bolt.GetStatusBucketPath(queueType, level, bolt.StatusPending),
		bolt.GetNodesBucketPath(queueType),
		bolt.GetStatusLookupBucketPath(queueType, level),
	}
	if err := s.checkConflict(topic, bucketsToRead); err != nil {
		return nil, err
	}

	var results []*bolt.NodeState

	opts := bolt.IteratorOptions{
		Limit: limit,
	}

	// Iterate over pending status bucket
	err := s.db.IterateStatusBucket(queueType, level, bolt.StatusPending, opts, func(nodeID []byte) error {
		// Get node state for each pending node
		state, err := bolt.GetNodeState(s.db, queueType, string(nodeID))
		if err != nil {
			return err
		}
		results = append(results, state)
		return nil
	})

	return results, err
}

// HasPendingAtLevel checks if there are any pending nodes at a specific level.
func (s *Store) HasPendingAtLevel(queueType string, level int) (bool, error) {
	topic := getTopicForQueueType(queueType)

	// Check conflicts: reads from status bucket (pending) and stats
	bucketsToRead := [][]string{
		bolt.GetStatusBucketPath(queueType, level, bolt.StatusPending),
		{"Traversal-Data", "STATS"}, // Stats bucket
	}
	if err := s.checkConflict(topic, bucketsToRead); err != nil {
		return false, err
	}

	return s.db.HasStatusBucketItems(queueType, level, bolt.StatusPending)
}

// GetAllLevels returns all levels that exist for a queue type.
func (s *Store) GetAllLevels(queueType string) ([]int, error) {
	topic := getTopicForQueueType(queueType)

	// Check conflicts: reads from levels buckets (via GetAllLevels which scans level buckets)
	// Conservative: check nodes bucket
	bucketsToRead := [][]string{
		bolt.GetNodesBucketPath(queueType),
	}
	if err := s.checkConflict(topic, bucketsToRead); err != nil {
		return nil, err
	}

	return s.db.GetAllLevels(queueType)
}

// GetMaxKnownDepth returns the maximum level number that exists in the database.
func (s *Store) GetMaxKnownDepth(queueType string) (int, error) {
	topic := getTopicForQueueType(queueType)

	// Check conflicts: reads from levels buckets (via GetAllLevels)
	bucketsToRead := [][]string{
		bolt.GetNodesBucketPath(queueType),
	}
	if err := s.checkConflict(topic, bucketsToRead); err != nil {
		return 0, err
	}

	levels, err := s.db.GetAllLevels(queueType)
	if err != nil {
		return 0, err
	}

	if len(levels) == 0 {
		return 0, nil
	}

	maxLevel := levels[0]
	for _, level := range levels {
		if level > maxLevel {
			maxLevel = level
		}
	}

	return maxLevel, nil
}

// LeaseTasksAtLevel atomically leases pending tasks from a specific level for processing.
// This atomically moves tasks from "pending" to "in-progress" status (like MongoDB's findAndModify).
// This prevents race conditions where multiple workers could lease the same task.
//
// This is used for concurrent task processing with lease-based coordination.
// limit specifies the maximum number of tasks to lease (0 = no limit).
// Returns the leased node IDs (which are now in "in-progress" status).
func (s *Store) LeaseTasksAtLevel(queueType string, level int, limit int) ([][]byte, error) {
	topic := getTopicForQueueType(queueType)

	// Check conflicts: reads/writes from status bucket (pending -> in-progress), status-lookup, and nodes
	bucketsToRead := [][]string{
		bolt.GetStatusBucketPath(queueType, level, bolt.StatusPending),
		bolt.GetStatusBucketPath(queueType, level, bolt.StatusInProgress),
		bolt.GetStatusLookupBucketPath(queueType, level),
		bolt.GetNodesBucketPath(queueType),
	}
	if err := s.checkConflict(topic, bucketsToRead); err != nil {
		return nil, err
	}

	// Execute lease operation synchronously (must flush first to ensure consistency)
	// This atomically moves tasks from pending -> in-progress, so we need a write transaction
	// We don't queue this write because it needs to return the leased tasks immediately
	return s.db.LeaseTasksFromStatus(queueType, level, bolt.StatusPending, limit)
}
