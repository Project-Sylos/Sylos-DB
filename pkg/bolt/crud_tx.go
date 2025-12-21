// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package bolt

import (
	"encoding/json"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// GetNodeStateInTx retrieves a NodeState from the nodes bucket by ULID within an existing transaction.
func GetNodeStateInTx(tx *bolt.Tx, queueType string, nodeID string) (*NodeState, error) {
	nodesBucket := getBucket(tx, GetNodesBucketPath(queueType))
	if nodesBucket == nil {
		return nil, fmt.Errorf("nodes bucket not found for %s", queueType)
	}

	nodeData := nodesBucket.Get([]byte(nodeID))
	if nodeData == nil {
		return nil, nil // Not found
	}

	ns, err := DeserializeNodeState(nodeData)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize node state: %w", err)
	}

	return ns, nil
}

// SetNodeStateInTx stores a NodeState in the nodes bucket within an existing transaction.
func SetNodeStateInTx(tx *bolt.Tx, queueType string, nodeID []byte, state *NodeState) error {
	if state.ID == "" {
		return fmt.Errorf("node state must have ID (ULID)")
	}
	// Ensure the nodeID parameter matches the state's ID
	if string(nodeID) != state.ID {
		return fmt.Errorf("nodeID parameter must match state.ID")
	}

	value, err := state.Serialize()
	if err != nil {
		return fmt.Errorf("failed to serialize node state: %w", err)
	}

	nodesBucket := getBucket(tx, GetNodesBucketPath(queueType))
	if nodesBucket == nil {
		return fmt.Errorf("nodes bucket not found for %s", queueType)
	}
	return nodesBucket.Put(nodeID, value)
}

// BatchDeleteNodesInTx deletes multiple nodes by their IDs within an existing transaction.
func BatchDeleteNodesInTx(tx *bolt.Tx, queueType string, nodeIDs []string) error {
	if len(nodeIDs) == 0 {
		return nil
	}

	nodesBucket := getBucket(tx, GetNodesBucketPath(queueType))
	if nodesBucket == nil {
		return fmt.Errorf("nodes bucket not found for %s", queueType)
	}

	childrenBucket := getBucket(tx, GetChildrenBucketPath(queueType))
	if childrenBucket == nil {
		return fmt.Errorf("children bucket not found for %s", queueType)
	}

	// Get join-lookup buckets
	var srcToDstBucket, dstToSrcBucket *bolt.Bucket
	switch queueType {
	case "SRC":
		srcToDstBucket = getBucket(tx, GetSrcToDstBucketPath())
	case "DST":
		dstToSrcBucket = getBucket(tx, GetDstToSrcBucketPath())
	}

	for _, nodeIDStr := range nodeIDs {
		nodeID := []byte(nodeIDStr)

		// Get node state to determine level, status, parent
		nodeData := nodesBucket.Get(nodeID)
		if nodeData == nil {
			continue // Node doesn't exist, skip
		}

		ns, err := DeserializeNodeState(nodeData)
		if err != nil {
			return fmt.Errorf("failed to deserialize node %s: %w", nodeIDStr, err)
		}

		// Delete from nodes bucket
		if err := nodesBucket.Delete(nodeID); err != nil {
			return fmt.Errorf("failed to delete node %s: %w", nodeIDStr, err)
		}

		// Delete from status bucket
		if ns.Depth >= 0 {
			statusBucket := getBucket(tx, GetStatusBucketPath(queueType, ns.Depth, ns.Status))
			if statusBucket != nil {
				statusBucket.Delete(nodeID)
			}
		}

		// Delete from status-lookup
		if ns.Depth >= 0 {
			DeleteStatusLookupInTx(tx, queueType, ns.Depth, nodeID)
		}

		// Delete from parent's children list (if has parent)
		if ns.ParentID != "" {
			// Load parent's children, remove this node, save back
			parentChildrenData := childrenBucket.Get([]byte(ns.ParentID))
			if parentChildrenData != nil {
				var children []string
				if err := json.Unmarshal(parentChildrenData, &children); err == nil {
					filtered := make([]string, 0, len(children))
					for _, c := range children {
						if c != nodeIDStr {
							filtered = append(filtered, c)
						}
					}
					if updatedData, err := json.Marshal(filtered); err == nil {
						childrenBucket.Put([]byte(ns.ParentID), updatedData)
					}
				}
			}
		}

		// Delete node's own children list (if folder)
		if ns.Type == "folder" {
			childrenBucket.Delete(nodeID)
		}

		// Delete from join-lookup tables
		if queueType == "SRC" && srcToDstBucket != nil {
			srcToDstBucket.Delete(nodeID)
		} else if queueType == "DST" && dstToSrcBucket != nil {
			dstToSrcBucket.Delete(nodeID)
		}

		// Delete from path-to-ulid lookup table
		if ns.Path != "" {
			DeletePathToULIDMapping(tx, queueType, ns.Path) // Ignore errors
		}
	}

	return nil
}

// DeleteStatusLookupInTx deletes a status-lookup entry within an existing transaction.
func DeleteStatusLookupInTx(tx *bolt.Tx, queueType string, level int, nodeID []byte) error {
	statusLookupBucket := getBucket(tx, GetStatusLookupBucketPath(queueType, level))
	if statusLookupBucket == nil {
		return nil // Bucket doesn't exist, nothing to delete
	}
	return statusLookupBucket.Delete(nodeID)
}
