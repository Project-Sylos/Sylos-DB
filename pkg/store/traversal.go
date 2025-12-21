// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package store

import (
	"encoding/json"
	"fmt"

	"github.com/Project-Sylos/Sylos-DB/pkg/bolt"
	bbolt "go.etcd.io/bbolt"
)

// TraversalTaskResult represents the result of completing a traversal task.
type TraversalTaskResult struct {
	ParentID     string
	OldStatus    string // Optional: defaults to "in-progress" if empty (tasks are leased in in-progress state)
	NewStatus    string
	Children     []*bolt.NodeState // All discovered children
	QueueType    string
	Level        int
	JoinMappings map[string]string // srcID → dstID (for DST queue only)
	PathMappings map[string]string // path → nodeID (for all children)
}

// CommitTraversalTask atomically commits the result of a traversal task by queuing
// individual write operations to the buffer. All operations will be flushed together
// in ONE transaction when the buffer flushes.
//
// This queues:
// - Parent status transition
// - All children node inserts
// - Children index update
// - Join mappings (if DST queue)
// - Path mappings
// - Stats updates (accumulated)
func (s *Store) CommitTraversalTask(result *TraversalTaskResult) error {
	topic := getTopicForQueueType(result.QueueType)

	// Default OldStatus to "in-progress" if not specified (tasks are leased in in-progress state)
	oldStatus := result.OldStatus
	if oldStatus == "" {
		oldStatus = bolt.StatusInProgress
	}

	// Collect child IDs and affected levels
	childIDs := make([]string, len(result.Children))
	affectedLevels := make(map[int]bool)
	affectedLevels[result.Level] = true // Parent level
	for i, child := range result.Children {
		childIDs[i] = child.ID
		if child.Depth >= 0 {
			affectedLevels[child.Depth] = true
		}
	}

	// Build list of buckets we'll be reading/writing for conflict checking
	bucketsToRead := [][]string{
		bolt.GetNodesBucketPath(result.QueueType),
		bolt.GetStatusBucketPath(result.QueueType, result.Level, oldStatus),
		bolt.GetStatusLookupBucketPath(result.QueueType, result.Level),
		bolt.GetChildrenBucketPath(result.QueueType),
	}
	// Add child status buckets
	for level := range affectedLevels {
		if level != result.Level { // Already added parent level
			bucketsToRead = append(bucketsToRead, bolt.GetStatusLookupBucketPath(result.QueueType, level))
		}
	}
	if len(result.JoinMappings) > 0 {
		bucketsToRead = append(bucketsToRead, bolt.GetSrcToDstBucketPath(), bolt.GetDstToSrcBucketPath())
	}
	if len(result.PathMappings) > 0 {
		bucketsToRead = append(bucketsToRead, bolt.GetPathToULIDBucketPath(result.QueueType))
	}

	// Check conflicts before writing
	if err := s.checkConflict(topic, bucketsToRead); err != nil {
		return err
	}

	// 1. Queue parent status transition (from in-progress to final status)
	{
		statsDeltas := make(map[string]int64)
		oldStatusPath := bolt.GetStatusBucketPath(result.QueueType, result.Level, oldStatus)
		newStatusPath := bolt.GetStatusBucketPath(result.QueueType, result.Level, result.NewStatus)
		statsDeltas[normalizeBucketPath(oldStatusPath)] = -1
		statsDeltas[normalizeBucketPath(newStatusPath)] = 1

		bucketsToWrite := [][]string{
			bolt.GetNodesBucketPath(result.QueueType),
			oldStatusPath,
			newStatusPath,
			bolt.GetStatusLookupBucketPath(result.QueueType, result.Level),
		}

		if err := s.queueWrite(topic, func(tx *bbolt.Tx) error {
			return bolt.UpdateNodeStatusInTxByID(tx, result.QueueType, result.Level,
				oldStatus, result.NewStatus, []byte(result.ParentID))
		}, bucketsToWrite, statsDeltas); err != nil {
			return fmt.Errorf("failed to queue parent status transition: %w", err)
		}
	}

	// 2. Queue all children node inserts (one operation per child for simplicity)
	// Alternative: could batch into fewer operations if needed
	for _, child := range result.Children {
		childCopy := child // Capture for closure

		// Validate child has depth set
		if childCopy.Depth < 0 {
			return fmt.Errorf("child %s missing Depth field", childCopy.ID)
		}

		// Ensure TraversalStatus is set
		if childCopy.TraversalStatus == "" {
			childCopy.TraversalStatus = childCopy.Status
		}

		statsDeltas := make(map[string]int64)
		nodesPath := bolt.GetNodesBucketPath(result.QueueType)
		statusPath := bolt.GetStatusBucketPath(result.QueueType, childCopy.Depth, childCopy.Status)
		statsDeltas[normalizeBucketPath(nodesPath)] = 1
		statsDeltas[normalizeBucketPath(statusPath)] = 1

		bucketsToWrite := [][]string{
			nodesPath,
			statusPath,
			bolt.GetStatusLookupBucketPath(result.QueueType, childCopy.Depth),
		}

		if err := s.queueWrite(topic, func(tx *bbolt.Tx) error {
			// Get nodes bucket
			nodesBucketPath := bolt.GetNodesBucketPath(result.QueueType)
			nodesBucket := tx.Bucket([]byte(nodesBucketPath[0]))
			if nodesBucket != nil {
				for i := 1; i < len(nodesBucketPath); i++ {
					nodesBucket = nodesBucket.Bucket([]byte(nodesBucketPath[i]))
					if nodesBucket == nil {
						break
					}
				}
			}
			if nodesBucket == nil {
				return fmt.Errorf("nodes bucket not found for %s", result.QueueType)
			}

			// Serialize and insert node
			nodeData, err := childCopy.Serialize()
			if err != nil {
				return fmt.Errorf("failed to serialize child node %s: %w", childCopy.ID, err)
			}

			if err := nodesBucket.Put([]byte(childCopy.ID), nodeData); err != nil {
				return fmt.Errorf("failed to insert child node %s: %w", childCopy.ID, err)
			}

			// Add to status bucket AT CHILD'S DEPTH
			statusBucket, err := bolt.GetOrCreateStatusBucket(tx, result.QueueType, childCopy.Depth, childCopy.Status)
			if err != nil {
				return fmt.Errorf("failed to get status bucket for child %s: %w", childCopy.ID, err)
			}
			if err := statusBucket.Put([]byte(childCopy.ID), []byte{}); err != nil {
				return fmt.Errorf("failed to add child to status bucket: %w", err)
			}

			// Update status-lookup index AT CHILD'S DEPTH
			return bolt.UpdateStatusLookup(tx, result.QueueType, childCopy.Depth, []byte(childCopy.ID), childCopy.Status)
		}, bucketsToWrite, statsDeltas); err != nil {
			return fmt.Errorf("failed to queue child insert: %w", err)
		}
	}

	// 3. Queue children index update
	{
		statsDeltas := make(map[string]int64)
		childrenPath := bolt.GetChildrenBucketPath(result.QueueType)
		bucketsToWrite := [][]string{childrenPath}

		if err := s.queueWrite(topic, func(tx *bbolt.Tx) error {
			// Get children bucket
			childrenBucketPath := bolt.GetChildrenBucketPath(result.QueueType)
			childrenBucket := tx.Bucket([]byte(childrenBucketPath[0]))
			if childrenBucket != nil {
				for i := 1; i < len(childrenBucketPath); i++ {
					childrenBucket = childrenBucket.Bucket([]byte(childrenBucketPath[i]))
					if childrenBucket == nil {
						break
					}
				}
			}
			if childrenBucket == nil {
				return fmt.Errorf("children bucket not found for %s", result.QueueType)
			}

			// Check if parent already has children entry (for stats correctness)
			parentKey := []byte(result.ParentID)
			existingEntry := childrenBucket.Get(parentKey)
			isNewEntry := (existingEntry == nil)

			// Marshal children IDs to JSON and write ONCE
			childrenData, err := json.Marshal(childIDs)
			if err != nil {
				return fmt.Errorf("failed to marshal children list: %w", err)
			}

			if err := childrenBucket.Put(parentKey, childrenData); err != nil {
				return fmt.Errorf("failed to write children index: %w", err)
			}

			// Update stats delta only if new entry
			if isNewEntry {
				statsDeltas[normalizeBucketPath(childrenPath)] = 1
			}

			return nil
		}, bucketsToWrite, statsDeltas); err != nil {
			return fmt.Errorf("failed to queue children index update: %w", err)
		}
	}

	// 4. Queue join mappings (if DST queue)
	// Join mappings span both queues, so we queue to both buffers
	for srcID, dstID := range result.JoinMappings {
		srcIDCopy, dstIDCopy := srcID, dstID

		// Write to src buffer (src-to-dst bucket)
		if err := s.queueWrite("src", func(tx *bbolt.Tx) error {
			return bolt.SetJoinMappingInTx(tx, "src-to-dst", srcIDCopy, dstIDCopy)
		}, [][]string{bolt.GetSrcToDstBucketPath()}, nil); err != nil {
			return fmt.Errorf("failed to queue src-to-dst mapping: %w", err)
		}

		// Write to dst buffer (dst-to-src bucket)
		if err := s.queueWrite("dst", func(tx *bbolt.Tx) error {
			return bolt.SetJoinMappingInTx(tx, "dst-to-src", dstIDCopy, srcIDCopy)
		}, [][]string{bolt.GetDstToSrcBucketPath()}, nil); err != nil {
			return fmt.Errorf("failed to queue dst-to-src mapping: %w", err)
		}
	}

	// 5. Queue path mappings
	for path, nodeID := range result.PathMappings {
		pathCopy, nodeIDCopy := path, nodeID

		bucketsToWrite := [][]string{
			bolt.GetPathToULIDBucketPath(result.QueueType),
		}
		if err := s.queueWrite(topic, func(tx *bbolt.Tx) error {
			return bolt.SetPathToULIDMapping(tx, result.QueueType, pathCopy, nodeIDCopy)
		}, bucketsToWrite, nil); err != nil {
			return fmt.Errorf("failed to queue path mapping: %w", err)
		}
	}

	return nil
}
