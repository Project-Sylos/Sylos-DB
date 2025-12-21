// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package store

import (
	"fmt"

	"github.com/Project-Sylos/Sylos-DB/pkg/bolt"
	bbolt "go.etcd.io/bbolt"
)

// RegisterNode inserts a new node into the database with its status bucket, lookup index, and stats updates.
// This performs an IMMEDIATE write (bypasses the buffer) and is intended for root seeding or single-node inserts.
// For traversal task completion (parent + children), use CommitTraversalTask instead (which is buffered).
func (s *Store) RegisterNode(queueType string, level int, status string, state *bolt.NodeState) error {
	if state.ID == "" {
		return fmt.Errorf("node ID (ULID) cannot be empty")
	}

	// Immediate write - no buffering
	// This is used for root seeding which needs to be immediately visible
	return s.db.Update(func(tx *bbolt.Tx) error {
		// Ensure TraversalStatus is set
		if state.TraversalStatus == "" {
			state.TraversalStatus = status
		}

		// Serialize node state
		nodeData, err := state.Serialize()
		if err != nil {
			return fmt.Errorf("failed to serialize node state: %w", err)
		}

		// Insert into nodes bucket
		traversalBucket := tx.Bucket([]byte("Traversal-Data"))
		if traversalBucket == nil {
			return fmt.Errorf("Traversal-Data bucket not found")
		}
		queueBucket := traversalBucket.Bucket([]byte(queueType))
		if queueBucket == nil {
			return fmt.Errorf("queue bucket %s not found", queueType)
		}
		nodesBucket := queueBucket.Bucket([]byte("nodes"))
		if nodesBucket == nil {
			return fmt.Errorf("nodes bucket not found for %s", queueType)
		}
		if err := nodesBucket.Put([]byte(state.ID), nodeData); err != nil {
			return fmt.Errorf("failed to insert node: %w", err)
		}

		// Insert into status bucket
		statusBucket, err := bolt.GetOrCreateStatusBucket(tx, queueType, level, status)
		if err != nil {
			return fmt.Errorf("failed to get status bucket: %w", err)
		}
		if err := statusBucket.Put([]byte(state.ID), []byte{}); err != nil {
			return fmt.Errorf("failed to add to status bucket: %w", err)
		}

		// Update status-lookup
		if err := bolt.UpdateStatusLookup(tx, queueType, level, []byte(state.ID), status); err != nil {
			return fmt.Errorf("failed to update status lookup: %w", err)
		}

		// Update stats
		if err := bolt.UpdateBucketStats(tx, bolt.GetNodesBucketPath(queueType), 1); err != nil {
			return fmt.Errorf("failed to update nodes stats: %w", err)
		}
		if err := bolt.UpdateBucketStats(tx, bolt.GetStatusBucketPath(queueType, level, status), 1); err != nil {
			return fmt.Errorf("failed to update status stats: %w", err)
		}

		return nil
	})
}

// TransitionNodeStatus atomically transitions a node from one status to another.
// This consolidates: node state update, status bucket membership changes, status-lookup update,
// and stats updates (decrement old status, increment new status).
func (s *Store) TransitionNodeStatus(queueType string, level int, oldStatus, newStatus string, nodeID string) error {
	if nodeID == "" {
		return fmt.Errorf("node ID cannot be empty")
	}

	topic := getTopicForQueueType(queueType)

	// Check conflicts: reads from nodes bucket and status buckets
	bucketsToRead := [][]string{
		bolt.GetNodesBucketPath(queueType),
		bolt.GetStatusBucketPath(queueType, level, oldStatus),
		bolt.GetStatusLookupBucketPath(queueType, level),
	}
	if err := s.checkConflict(topic, bucketsToRead); err != nil {
		return err
	}

	// Queue the write operation: writes to nodes, status buckets, status-lookup, and stats
	bucketsToWrite := [][]string{
		bolt.GetNodesBucketPath(queueType),
		bolt.GetStatusBucketPath(queueType, level, oldStatus),
		bolt.GetStatusBucketPath(queueType, level, newStatus),
		bolt.GetStatusLookupBucketPath(queueType, level),
	}

	// Stats deltas
	statsDeltas := make(map[string]int64)
	statsDeltas[normalizeBucketPath(bolt.GetStatusBucketPath(queueType, level, oldStatus))] = -1
	statsDeltas[normalizeBucketPath(bolt.GetStatusBucketPath(queueType, level, newStatus))] = 1

	return s.queueWrite(topic, func(tx *bbolt.Tx) error {
		// Use the TX-level status update
		return bolt.UpdateNodeStatusInTxByID(tx, queueType, level, oldStatus, newStatus, []byte(nodeID))
	}, bucketsToWrite, statsDeltas)
}

// UpdateNodeCopyStatus updates only the copy status field of a node.
// This does not affect status bucket membership or stats (copy status is not tracked in status buckets).
func (s *Store) UpdateNodeCopyStatus(nodeID string, newCopyStatus string) error {
	if nodeID == "" {
		return fmt.Errorf("node ID cannot be empty")
	}

	// Check conflicts: reads from nodes buckets (try both SRC and DST since we don't know which one)
	bucketsToRead := [][]string{
		bolt.GetNodesBucketPath("SRC"),
		bolt.GetNodesBucketPath("DST"),
	}
	// Check both buffers
	if err := s.checkConflict("src", bucketsToRead); err != nil {
		return err
	}
	if err := s.checkConflict("dst", bucketsToRead); err != nil {
		return err
	}

	// Queue the write operation - we need to determine which queue, so queue to both topics
	// (Only one will actually write, the other will fail gracefully if node not found)
	bucketsToWriteSrc := [][]string{
		bolt.GetNodesBucketPath("SRC"),
	}
	bucketsToWriteDst := [][]string{
		bolt.GetNodesBucketPath("DST"),
	}

	// Try SRC first
	if err := s.queueWrite("src", func(tx *bbolt.Tx) error {
		srcState, err := bolt.GetNodeStateInTx(tx, "SRC", nodeID)
		if err == nil && srcState != nil {
			srcState.CopyStatus = newCopyStatus
			return bolt.SetNodeStateInTx(tx, "SRC", []byte(nodeID), srcState)
		}
		return nil // Not in SRC, try DST
	}, bucketsToWriteSrc, nil); err != nil {
		return err
	}

	// Try DST
	return s.queueWrite("dst", func(tx *bbolt.Tx) error {
		dstState, err := bolt.GetNodeStateInTx(tx, "DST", nodeID)
		if err == nil && dstState != nil {
			dstState.CopyStatus = newCopyStatus
			return bolt.SetNodeStateInTx(tx, "DST", []byte(nodeID), dstState)
		}
		return fmt.Errorf("node not found: %s", nodeID)
	}, bucketsToWriteDst, nil)
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
		nodeState, err := bolt.GetNodeStateInTx(tx, queueType, nodeID)
		if err != nil {
			return fmt.Errorf("failed to get node state: %w", err)
		}
		if nodeState == nil {
			return fmt.Errorf("node not found: %s", nodeID)
		}

		// Update exclusion flag
		nodeState.ExplicitExcluded = explicitExcluded

		// Save updated node state
		return bolt.SetNodeStateInTx(tx, queueType, []byte(nodeID), nodeState)
	}, bucketsToWrite, nil)
}

// DeleteNode removes a node and all its associated indexes and stats.
func (s *Store) DeleteNode(queueType string, nodeID string) error {
	if nodeID == "" {
		return fmt.Errorf("node ID cannot be empty")
	}

	topic := getTopicForQueueType(queueType)

	// Check conflicts: reads from nodes and status buckets (we don't know which level)
	// For delete, we need to read the node first to determine what to delete
	bucketsToRead := [][]string{
		bolt.GetNodesBucketPath(queueType),
		// We don't know the exact status buckets, so we can't list them all
		// The delete operation will handle reading the node state to determine what to delete
	}
	if err := s.checkConflict(topic, bucketsToRead); err != nil {
		return err
	}

	// Queue the write operation: BatchDeleteNodesInTx handles all the deletes
	// It touches: nodes, status buckets, status-lookup, children, join mappings, path mappings, stats
	// We can't know all the exact buckets ahead of time, so we mark the main ones
	bucketsToWrite := [][]string{
		bolt.GetNodesBucketPath(queueType),
		bolt.GetChildrenBucketPath(queueType),
		bolt.GetSrcToDstBucketPath(), // May affect join mappings
		bolt.GetDstToSrcBucketPath(),
	}
	return s.queueWrite(topic, func(tx *bbolt.Tx) error {
		return bolt.BatchDeleteNodesInTx(tx, queueType, []string{nodeID})
	}, bucketsToWrite, nil)
}
