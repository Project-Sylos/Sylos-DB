// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package bolt

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	bolt "go.etcd.io/bbolt"
)

// InsertOperation represents a single node insertion operation for batch inserts.
type InsertOperation struct {
	QueueType string
	Level     int
	Status    string
	State     *NodeState
}

// InsertNodeWithIndex atomically inserts a node into the nodes bucket, adds it to a status bucket,
// and updates the parent's children list in the children bucket.
func InsertNodeWithIndex(db *DB, queueType string, level int, status string, state *NodeState) error {
	if state.ID == "" {
		return fmt.Errorf("node ID (ULID) cannot be empty")
	}

	nodeID := []byte(state.ID)
	var parentID []byte

	return db.Update(func(tx *bolt.Tx) error {
		// ParentID can be empty for root nodes, otherwise must be set
		if state.ParentID != "" {
			parentID = []byte(state.ParentID)
		}

		// 1. Insert into nodes bucket
		nodesBucket := getBucket(tx, GetNodesBucketPath(queueType))
		if nodesBucket == nil {
			return fmt.Errorf("nodes bucket not found for %s", queueType)
		}

		// Ensure TraversalStatus is set
		if state.TraversalStatus == "" {
			state.TraversalStatus = status
		}

		nodeData, err := state.Serialize()
		if err != nil {
			return fmt.Errorf("failed to serialize node state: %w", err)
		}

		if err := nodesBucket.Put(nodeID, nodeData); err != nil {
			return fmt.Errorf("failed to insert node: %w", err)
		}

		// 2. Add to status bucket
		statusBucket, err := GetOrCreateStatusBucket(tx, queueType, level, status)
		if err != nil {
			return fmt.Errorf("failed to get status bucket: %w", err)
		}

		if err := statusBucket.Put(nodeID, []byte{}); err != nil {
			return fmt.Errorf("failed to add to status bucket: %w", err)
		}

		// 3. Update status-lookup index
		if err := UpdateStatusLookup(tx, queueType, level, nodeID, status); err != nil {
			return fmt.Errorf("failed to update status-lookup: %w", err)
		}

		// 4. Update parent's children list in children bucket
		if state.ParentID != "" {
			childrenBucket := getBucket(tx, GetChildrenBucketPath(queueType))
			if childrenBucket == nil {
				return fmt.Errorf("children bucket not found for %s", queueType)
			}

			// Get existing children list
			var children []string
			childrenData := childrenBucket.Get(parentID)
			if childrenData != nil {
				if err := json.Unmarshal(childrenData, &children); err != nil {
					return fmt.Errorf("failed to unmarshal children list: %w", err)
				}
			}

			// Add this child's ULID if not already present
			found := false
			for _, c := range children {
				if c == state.ID {
					found = true
					break
				}
			}

			if !found {
				children = append(children, state.ID)

				// Save updated children list
				childrenData, err := json.Marshal(children)
				if err != nil {
					return fmt.Errorf("failed to marshal children list: %w", err)
				}

				if err := childrenBucket.Put(parentID, childrenData); err != nil {
					return fmt.Errorf("failed to update children list: %w", err)
				}
			}
		}

		return nil
	})
}

// DeleteNodeWithIndex atomically deletes a node from the nodes bucket, removes it from status buckets,
// and updates the parent's children list.
func DeleteNodeWithIndex(db *DB, queueType string, level int, status string, state *NodeState) error {
	if state.ID == "" {
		return fmt.Errorf("node ID (ULID) cannot be empty")
	}

	nodeID := []byte(state.ID)
	var parentID []byte

	return db.Update(func(tx *bolt.Tx) error {
		// ParentID must be set - no path-based lookup
		if state.ParentID == "" {
			// Skip parent operations if no ParentID
			parentID = nil
		} else {
			parentID = []byte(state.ParentID)
		}

		// 1. Delete from nodes bucket
		nodesBucket := getBucket(tx, GetNodesBucketPath(queueType))
		if nodesBucket != nil {
			nodesBucket.Delete(nodeID) // Ignore errors
		}

		// 2. Remove from status bucket
		statusBucket := getBucket(tx, GetStatusBucketPath(queueType, level, status))
		if statusBucket != nil {
			statusBucket.Delete(nodeID) // Ignore errors
		}

		// 3. Remove from status-lookup index
		lookupBucket := getBucket(tx, GetStatusLookupBucketPath(queueType, level))
		if lookupBucket != nil {
			lookupBucket.Delete(nodeID) // Ignore errors
		}

		// 4. Remove from parent's children list
		if state.ParentID != "" {
			childrenBucket := getBucket(tx, GetChildrenBucketPath(queueType))
			if childrenBucket != nil {
				var children []string
				childrenData := childrenBucket.Get(parentID)
				if childrenData != nil {
					if err := json.Unmarshal(childrenData, &children); err == nil {
						// Remove this child's ULID
						filtered := make([]string, 0, len(children))
						for _, c := range children {
							if c != state.ID {
								filtered = append(filtered, c)
							}
						}

						// Save updated list
						if len(filtered) > 0 {
							childrenData, err := json.Marshal(filtered)
							if err == nil {
								childrenBucket.Put(parentID, childrenData)
							}
						} else {
							// No children left, remove entry
							childrenBucket.Delete(parentID)
						}
					}
				}
			}
		}

		return nil
	})
}

// GetChildrenByParentID retrieves children of a parent by parent ULID.
// returnType is either "ids" (returns []string of ULIDs) or "states" (returns []*NodeState).
// This avoids two transactions by doing everything in one View transaction.
func GetChildrenByParentID(db *DB, queueType string, parentID string, returnType string) (interface{}, error) {
	switch returnType {
	case "ids":
		var childIDs []string
		err := db.View(func(tx *bolt.Tx) error {
			childrenBucket := getBucket(tx, GetChildrenBucketPath(queueType))
			if childrenBucket == nil {
				return fmt.Errorf("children bucket not found for %s", queueType)
			}

			childrenData := childrenBucket.Get([]byte(parentID))
			if childrenData == nil {
				return nil // No children
			}

			if err := json.Unmarshal(childrenData, &childIDs); err != nil {
				return fmt.Errorf("failed to unmarshal children list: %w", err)
			}

			return nil
		})

		if err != nil {
			return nil, err
		}

		if childIDs == nil {
			return []string{}, nil
		}
		return childIDs, nil

	case "states":
		var children []*NodeState
		err := db.View(func(tx *bolt.Tx) error {
			childrenBucket := getBucket(tx, GetChildrenBucketPath(queueType))
			if childrenBucket == nil {
				return fmt.Errorf("children bucket not found for %s", queueType)
			}

			childrenData := childrenBucket.Get([]byte(parentID))
			if childrenData == nil {
				return nil // No children
			}

			var childIDs []string
			if err := json.Unmarshal(childrenData, &childIDs); err != nil {
				return fmt.Errorf("failed to unmarshal children list: %w", err)
			}

			if len(childIDs) == 0 {
				return nil
			}

			nodesBucket := getBucket(tx, GetNodesBucketPath(queueType))
			if nodesBucket == nil {
				return fmt.Errorf("nodes bucket not found for %s", queueType)
			}

			for _, childID := range childIDs {
				nodeData := nodesBucket.Get([]byte(childID))
				if nodeData == nil {
					continue // Child may have been deleted
				}

				ns, err := DeserializeNodeState(nodeData)
				if err != nil {
					return fmt.Errorf("failed to deserialize child node: %w", err)
				}

				children = append(children, ns)
			}

			return nil
		})

		if err != nil {
			return nil, err
		}

		if children == nil {
			return []*NodeState{}, nil
		}
		return children, nil

	default:
		return nil, fmt.Errorf("invalid returnType: %s (must be 'ids' or 'states')", returnType)
	}
}

// computeBatchInsertStatsDeltas analyzes insert operations and computes stats deltas.
// Returns a map of bucket path (as string) -> delta count.
// Simply counts all inserts and groups by bucket - one update per bucket.
func computeBatchInsertStatsDeltas(tx *bolt.Tx, ops []InsertOperation) map[string]int64 {
	deltas := make(map[string]int64)

	// Group by bucket path and count
	nodesCounts := make(map[string]int64)    // queueType -> count
	statusCounts := make(map[string]int64)   // "queueType/level/status" -> count
	childrenCounts := make(map[string]int64) // queueType -> count of new parent entries

	for _, op := range ops {
		if op.State == nil || op.State.ID == "" {
			continue
		}

		nodeID := []byte(op.State.ID)
		var parentID []byte

		// ParentID must be set - no path-based lookup
		if op.State.ParentID != "" {
			parentID = []byte(op.State.ParentID)
		}

		// Check if node already exists - only count new nodes
		nodesBucket := getBucket(tx, GetNodesBucketPath(op.QueueType))
		if nodesBucket != nil && nodesBucket.Get(nodeID) == nil {
			nodesCounts[op.QueueType]++
		}

		// Check if status entry already exists - only count new entries
		statusBucket := getBucket(tx, GetStatusBucketPath(op.QueueType, op.Level, op.Status))
		if statusBucket == nil || statusBucket.Get(nodeID) == nil {
			statusKey := fmt.Sprintf("%s/%d/%s", op.QueueType, op.Level, op.Status)
			statusCounts[statusKey]++
		}

		// Check if children entry needs to be created
		if op.State.ParentID != "" {
			childrenBucket := getBucket(tx, GetChildrenBucketPath(op.QueueType))
			if childrenBucket != nil && childrenBucket.Get(parentID) == nil {
				childrenCounts[op.QueueType]++
			}
		}
	}

	// Convert to bucket path strings
	for queueType, count := range nodesCounts {
		path := strings.Join(GetNodesBucketPath(queueType), "/")
		deltas[path] += count
	}

	for statusKey, count := range statusCounts {
		parts := strings.Split(statusKey, "/")
		if len(parts) == 3 {
			queueType := parts[0]
			level, _ := strconv.Atoi(parts[1])
			status := parts[2]
			path := strings.Join(GetStatusBucketPath(queueType, level, status), "/")
			deltas[path] += count
		}
	}

	for queueType, count := range childrenCounts {
		path := strings.Join(GetChildrenBucketPath(queueType), "/")
		deltas[path] += count
	}

	return deltas
}

// BatchInsertNodes inserts multiple nodes with their indices in a single transaction.
// SrcID is already populated in NodeState during matching, so no join-lookup needed.
func BatchInsertNodes(db *DB, ops []InsertOperation) error {
	if len(ops) == 0 {
		return nil
	}

	return db.Update(func(tx *bolt.Tx) error {
		// Ensure stats bucket exists
		if _, err := getStatsBucket(tx); err != nil {
			return fmt.Errorf("failed to get stats bucket: %w", err)
		}

		// Compute stats deltas BEFORE executing writes (check what exists first)
		statsDeltas := computeBatchInsertStatsDeltas(tx, ops)

		// Execute all inserts
		var nodesBucket *bolt.Bucket
		var currentQueueType string

		for _, op := range ops {
			if op.State == nil || op.State.ID == "" {
				return fmt.Errorf("node state must have ID (ULID)")
			}

			nodeID := []byte(op.State.ID)
			var parentID []byte

			// ParentID must be set - no path-based lookup
			if op.State.ParentID != "" {
				parentID = []byte(op.State.ParentID)
			}

			// Get or cache nodes bucket
			if currentQueueType != op.QueueType {
				nodesBucket = getBucket(tx, GetNodesBucketPath(op.QueueType))
				if nodesBucket == nil {
					return fmt.Errorf("nodes bucket not found for %s", op.QueueType)
				}
				currentQueueType = op.QueueType
			}

			if op.State.TraversalStatus == "" {
				op.State.TraversalStatus = op.Status
			}

			// 1. Insert into nodes bucket
			nodeData, err := op.State.Serialize()
			if err != nil {
				return fmt.Errorf("failed to serialize node: %w", err)
			}

			if err := nodesBucket.Put(nodeID, nodeData); err != nil {
				return fmt.Errorf("failed to insert node: %w", err)
			}

			// 2. Add to status bucket
			statusBucket, err := GetOrCreateStatusBucket(tx, op.QueueType, op.Level, op.Status)
			if err != nil {
				return fmt.Errorf("failed to get status bucket: %w", err)
			}

			if err := statusBucket.Put(nodeID, []byte{}); err != nil {
				return fmt.Errorf("failed to add to status bucket: %w", err)
			}

			// 3. Update status-lookup index
			if err := UpdateStatusLookup(tx, op.QueueType, op.Level, nodeID, op.Status); err != nil {
				return fmt.Errorf("failed to update status-lookup: %w", err)
			}

			// 4. Update children index
			if op.State.ParentID != "" {
				childrenBucket := getBucket(tx, GetChildrenBucketPath(op.QueueType))
				if childrenBucket == nil {
					return fmt.Errorf("children bucket not found for %s", op.QueueType)
				}

				var children []string
				childrenData := childrenBucket.Get(parentID)
				if childrenData != nil {
					json.Unmarshal(childrenData, &children)
				}

				found := false
				for _, c := range children {
					if c == op.State.ID {
						found = true
						break
					}
				}

				if !found {
					children = append(children, op.State.ID)
					childrenData, _ := json.Marshal(children)
					childrenBucket.Put(parentID, childrenData)
				}
			}

			// Store lookup mappings if SrcID is present in NodeState (for backward compatibility)
			// For DST nodes, store DST→SRC and SRC→DST mappings
			if op.QueueType == "DST" && op.State.SrcID != "" {
				// Store DST→SRC mapping
				dstToSrcBucket, err := getOrCreateBucket(tx, GetDstToSrcBucketPath())
				if err == nil {
					dstToSrcBucket.Put(nodeID, []byte(op.State.SrcID))
				}
				// Store SRC→DST mapping
				srcToDstBucket, err := getOrCreateBucket(tx, GetSrcToDstBucketPath())
				if err == nil {
					srcToDstBucket.Put([]byte(op.State.SrcID), nodeID)
				}
			}
		}

		// Apply all stats updates in one batch
		for bucketPathStr, delta := range statsDeltas {
			// Convert string path back to []string for UpdateBucketStats
			bucketPath := strings.Split(bucketPathStr, "/")
			if err := UpdateBucketStats(tx, bucketPath, delta); err != nil {
				return fmt.Errorf("failed to update stats for %s: %w", bucketPathStr, err)
			}
		}

		return nil
	})
}

// BatchDeleteNodes deletes multiple nodes by their IDs in a single transaction.
// For each node ID, it retrieves the node state to determine level and status,
// then deletes from all relevant buckets (nodes, status, status-lookup, children).
func BatchDeleteNodes(db *DB, queueType string, nodeIDs []string) error {
	if len(nodeIDs) == 0 {
		return nil
	}

	return db.Update(func(tx *bolt.Tx) error {
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

		// Track parent updates and stats deltas
		parentUpdates := make(map[string][]string) // parentID -> remaining children
		statsDeltas := make(map[string]int64)      // bucket path -> delta

		for _, nodeIDStr := range nodeIDs {
			nodeID := []byte(nodeIDStr)

			// Get node state to determine level and status
			nodeData := nodesBucket.Get(nodeID)
			if nodeData == nil {
				continue // Node already deleted, skip
			}

			ns, err := DeserializeNodeState(nodeData)
			if err != nil {
				return fmt.Errorf("failed to deserialize node state for %s: %w", nodeIDStr, err)
			}

			// Determine current status from status-lookup
			lookupBucket := getBucket(tx, GetStatusLookupBucketPath(queueType, ns.Depth))
			var currentStatus string
			if lookupBucket != nil {
				statusData := lookupBucket.Get(nodeID)
				if statusData != nil {
					currentStatus = string(statusData)
				}
			}

			// 1. Delete from nodes bucket
			if err := nodesBucket.Delete(nodeID); err != nil {
				return fmt.Errorf("failed to delete from nodes bucket: %w", err)
			}
			// Decrement nodes bucket count
			nodesPath := GetNodesBucketPath(queueType)
			statsDeltas[strings.Join(nodesPath, "/")]--

			// 2. Delete from status bucket
			if currentStatus != "" {
				statusBucket := getBucket(tx, GetStatusBucketPath(queueType, ns.Depth, currentStatus))
				if statusBucket != nil {
					statusBucket.Delete(nodeID) // Ignore errors
					// Decrement status bucket count
					statusPath := GetStatusBucketPath(queueType, ns.Depth, currentStatus)
					statsDeltas[strings.Join(statusPath, "/")]--
				}
			}

			// 3. Delete from status-lookup bucket
			if lookupBucket != nil {
				lookupBucket.Delete(nodeID) // Ignore errors
			}

			// 4. Track parent for children list update
			if ns.ParentID != "" {
				if _, exists := parentUpdates[ns.ParentID]; !exists {
					// Load current children list
					parentID := []byte(ns.ParentID)
					childrenData := childrenBucket.Get(parentID)
					if childrenData != nil {
						var children []string
						if err := json.Unmarshal(childrenData, &children); err == nil {
							parentUpdates[ns.ParentID] = children
						}
					} else {
						parentUpdates[ns.ParentID] = []string{}
					}
				}
				// Remove this child from the list
				children := parentUpdates[ns.ParentID]
				filtered := make([]string, 0, len(children))
				for _, c := range children {
					if c != nodeIDStr {
						filtered = append(filtered, c)
					}
				}
				parentUpdates[ns.ParentID] = filtered
			}

			// 5. Delete node's own children list (if folder)
			if ns.Type == "folder" {
				if childrenBucket.Get(nodeID) != nil {
					childrenBucket.Delete(nodeID)
					// Decrement children bucket count
					childrenPath := GetChildrenBucketPath(queueType)
					statsDeltas[strings.Join(childrenPath, "/")]--
				}
			}

			// 6. Delete from join-lookup tables
			if queueType == "SRC" && srcToDstBucket != nil {
				srcToDstBucket.Delete(nodeID)
			} else if queueType == "DST" && dstToSrcBucket != nil {
				dstToSrcBucket.Delete(nodeID)
			}

			// 7. Delete from path-to-ulid lookup table
			if ns.Path != "" {
				DeletePathToULIDMapping(tx, queueType, ns.Path) // Ignore errors
			}
		}

		// Apply all parent children list updates
		for parentIDStr, children := range parentUpdates {
			parentID := []byte(parentIDStr)
			if len(children) > 0 {
				childrenData, err := json.Marshal(children)
				if err != nil {
					return fmt.Errorf("failed to marshal children list: %w", err)
				}
				if err := childrenBucket.Put(parentID, childrenData); err != nil {
					return fmt.Errorf("failed to update children list: %w", err)
				}
			} else {
				// No children left, remove entry
				childrenBucket.Delete(parentID)
			}
		}

		// Update stats bucket for all deletions
		for pathStr, delta := range statsDeltas {
			bucketPath := strings.Split(pathStr, "/")
			if err := UpdateBucketStats(tx, bucketPath, delta); err != nil {
				return fmt.Errorf("failed to update stats for %s: %w", pathStr, err)
			}
		}

		return nil
	})
}

// UpdateNodeStatusInTxByID updates a node's status within an existing transaction using ULID.
// This is the preferred method - use ULID directly instead of path lookup.
func UpdateNodeStatusInTxByID(tx *bolt.Tx, queueType string, level int, oldStatus, newStatus string, nodeID []byte) error {
	// Get the node data from nodes bucket
	nodesBucket := getBucket(tx, GetNodesBucketPath(queueType))
	if nodesBucket == nil {
		return fmt.Errorf("nodes bucket not found for %s", queueType)
	}

	nodeData := nodesBucket.Get(nodeID)
	if nodeData == nil {
		return fmt.Errorf("node not found in nodes bucket: %s", string(nodeID))
	}

	// Deserialize and update traversal status
	ns, err := DeserializeNodeState(nodeData)
	if err != nil {
		return fmt.Errorf("failed to deserialize node state: %w", err)
	}
	ns.TraversalStatus = newStatus

	// Serialize updated state back to nodes bucket
	updatedData, err := ns.Serialize()
	if err != nil {
		return fmt.Errorf("failed to serialize node state: %w", err)
	}

	if err := nodesBucket.Put(nodeID, updatedData); err != nil {
		return fmt.Errorf("failed to update node in nodes bucket: %w", err)
	}

	// Update status bucket membership
	// Delete from old status bucket
	oldBucket := getBucket(tx, GetStatusBucketPath(queueType, level, oldStatus))
	if oldBucket != nil {
		if err := oldBucket.Delete(nodeID); err != nil {
			return fmt.Errorf("failed to delete from old status bucket: %w", err)
		}
	}

	// Add to new status bucket
	newBucket, err := GetOrCreateStatusBucket(tx, queueType, level, newStatus)
	if err != nil {
		return fmt.Errorf("failed to get new status bucket: %w", err)
	}

	if err := newBucket.Put(nodeID, []byte{}); err != nil {
		return fmt.Errorf("failed to add to new status bucket: %w", err)
	}

	// Update status-lookup index
	if err := UpdateStatusLookup(tx, queueType, level, nodeID, newStatus); err != nil {
		return fmt.Errorf("failed to update status-lookup: %w", err)
	}

	return nil
}

// BatchInsertNodesInTx inserts multiple nodes within an existing transaction.
// This is used by queue.Complete() to insert discovered children atomically.
// Stats updates are handled separately by batch processing in output buffer flush.
// SrcID is already populated in NodeState during matching, so no join-lookup needed.
func BatchInsertNodesInTx(tx *bolt.Tx, operations []InsertOperation) error {
	var nodesBucket *bolt.Bucket
	var currentQueueType string

	for _, op := range operations {
		if op.State == nil || op.State.ID == "" {
			return fmt.Errorf("node state must have ID (ULID)")
		}

		// Ensure NodeState has the status field populated
		if op.State.TraversalStatus == "" {
			op.State.TraversalStatus = op.Status
		}

		nodeID := []byte(op.State.ID)
		var parentID []byte

		// ParentID must be set - no path-based lookup
		if op.State.ParentID != "" {
			parentID = []byte(op.State.ParentID)
		}

		// Get or cache nodes bucket
		if currentQueueType != op.QueueType {
			nodesBucket = getBucket(tx, GetNodesBucketPath(op.QueueType))
			if nodesBucket == nil {
				return fmt.Errorf("nodes bucket not found for %s", op.QueueType)
			}
			currentQueueType = op.QueueType
		}

		// 1. Insert into nodes bucket
		nodeData, err := op.State.Serialize()
		if err != nil {
			return fmt.Errorf("failed to serialize node state: %w", err)
		}

		if err := nodesBucket.Put(nodeID, nodeData); err != nil {
			return fmt.Errorf("failed to insert node: %w", err)
		}

		// 2. Add to status bucket
		statusBucket, err := GetOrCreateStatusBucket(tx, op.QueueType, op.Level, op.Status)
		if err != nil {
			return fmt.Errorf("failed to get status bucket: %w", err)
		}

		if err := statusBucket.Put(nodeID, []byte{}); err != nil {
			return fmt.Errorf("failed to add to status bucket: %w", err)
		}

		// 3. Update status-lookup index
		if err := UpdateStatusLookup(tx, op.QueueType, op.Level, nodeID, op.Status); err != nil {
			return fmt.Errorf("failed to update status-lookup: %w", err)
		}

		// 4. Update children index
		if op.State.ParentID != "" {
			childrenBucket := getBucket(tx, GetChildrenBucketPath(op.QueueType))
			if childrenBucket == nil {
				return fmt.Errorf("children bucket not found for %s", op.QueueType)
			}

			// Get existing children list
			var children []string
			childrenData := childrenBucket.Get(parentID)
			if childrenData != nil {
				if err := DeserializeStringSlice(childrenData, &children); err != nil {
					return fmt.Errorf("failed to unmarshal children list: %w", err)
				}
			}

			// Add this child's ULID if not already present
			found := false
			for _, c := range children {
				if c == op.State.ID {
					found = true
					break
				}
			}

			if !found {
				children = append(children, op.State.ID)
				childrenData, err := SerializeStringSlice(children)
				if err != nil {
					return fmt.Errorf("failed to marshal children list: %w", err)
				}

				if err := childrenBucket.Put(parentID, childrenData); err != nil {
					return fmt.Errorf("failed to update children list: %w", err)
				}
			}
		}

		// SrcID is already populated in NodeState during matching, no join-lookup needed
	}

	return nil
}

// UpdateNodeByID updates a node in one transaction, either for traversal status or copy status, depending on updateType.
// updateType is either "status" (for traversal status) or "copy" (for copy status).
// For updateType "status": oldValue is oldStatus, newValue is newStatus, level is required.
// For updateType "copy": oldValue is unused (can be ""), newValue is newCopyStatus, level is unused (can be 0).
// Returns the updated NodeState.
func UpdateNodeByID(
	db *DB,
	queueType string,
	level int,
	updateType string, // "status" or "copy"
	oldValue string, // if "status", this is oldStatus; ignored for "copy"
	newValue string, // if "status", this is newStatus; if "copy", this is newCopyStatus
	nodeID string,
) (*NodeState, error) {
	var nodeState *NodeState

	err := db.Update(func(tx *bolt.Tx) error {
		nodeIDBytes := []byte(nodeID)

		// Get the node data from nodes bucket
		nodesBucket := getBucket(tx, GetNodesBucketPath(queueType))
		if nodesBucket == nil {
			return fmt.Errorf("nodes bucket not found for %s", queueType)
		}

		nodeData := nodesBucket.Get(nodeIDBytes)
		if nodeData == nil {
			return fmt.Errorf("node not found in nodes bucket: %s", nodeID)
		}

		// Deserialize node state
		ns, err := DeserializeNodeState(nodeData)
		if err != nil {
			return fmt.Errorf("failed to deserialize node state: %w", err)
		}

		switch updateType {
		case "status":
			// Use the TX-level function to avoid duplication
			if err := UpdateNodeStatusInTxByID(tx, queueType, level, oldValue, newValue, nodeIDBytes); err != nil {
				return err
			}

			// Get the updated node state to return
			updatedNodeData := nodesBucket.Get(nodeIDBytes)
			if updatedNodeData == nil {
				return fmt.Errorf("node not found after update: %s", nodeID)
			}

			ns, err := DeserializeNodeState(updatedNodeData)
			if err != nil {
				return fmt.Errorf("failed to deserialize updated node state: %w", err)
			}

			nodeState = ns

		case "copy":
			// Update copy status and copy_needed flag
			ns.CopyStatus = newValue
			ns.CopyNeeded = (newValue == CopyStatusPending)

			// Serialize and save
			updatedData, err := ns.Serialize()
			if err != nil {
				return fmt.Errorf("failed to serialize node state: %w", err)
			}

			if err := nodesBucket.Put(nodeIDBytes, updatedData); err != nil {
				return fmt.Errorf("failed to update node: %w", err)
			}

			nodeState = ns

		default:
			return fmt.Errorf("unknown updateType: %s", updateType)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return nodeState, nil
}

// SetNodeState stores a NodeState in the nodes bucket.
// This is used for initial insertion or updates without state transitions.
// nodeID is the ULID of the node (as []byte).
func SetNodeState(db *DB, queueType string, nodeID []byte, state *NodeState) error {
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

	return db.Update(func(tx *bolt.Tx) error {
		nodesBucket := getBucket(tx, GetNodesBucketPath(queueType))
		if nodesBucket == nil {
			return fmt.Errorf("nodes bucket not found for %s", queueType)
		}
		return nodesBucket.Put(nodeID, value)
	})
}

// GetNodeState retrieves a NodeState from the nodes bucket by ULID.
func GetNodeState(db *DB, queueType string, nodeID string) (*NodeState, error) {
	var nodeState *NodeState

	err := db.View(func(tx *bolt.Tx) error {
		nodesBucket := getBucket(tx, GetNodesBucketPath(queueType))
		if nodesBucket == nil {
			return fmt.Errorf("nodes bucket not found for %s", queueType)
		}

		nodeData := nodesBucket.Get([]byte(nodeID))
		if nodeData == nil {
			return nil // Not found
		}

		ns, err := DeserializeNodeState(nodeData)
		if err != nil {
			return fmt.Errorf("failed to deserialize node state: %w", err)
		}

		nodeState = ns
		return nil
	})

	if err != nil {
		return nil, err
	}

	return nodeState, nil
}

// BatchUpdateNodesByID updates multiple nodes in one transaction, either for traversal status or copy status, depending on updateType.
// updateType is either "status" (for traversal status) or "copy" (for copy status).
// For updateType "status": oldValue is oldStatus, newValue is newStatus.
// For updateType "copy": oldValue is unused (can be ""), newValue is newCopyStatus.
func BatchUpdateNodesByID(
	db *DB,
	queueType string,
	level int,
	updateType string, // "status" or "copy"
	oldValue string, // if "status", this is oldStatus; ignored for "copy"
	newValue string, // if "status", this is newStatus; if "copy", this is newCopyStatus
	nodeIDs []string,
) (map[string]*NodeState, error) {
	results := make(map[string]*NodeState)

	err := db.Update(func(tx *bolt.Tx) error {
		nodesBucket := getBucket(tx, GetNodesBucketPath(queueType))
		if nodesBucket == nil {
			return fmt.Errorf("nodes bucket not found for %s", queueType)
		}

		var oldBucket, newStatusBucket *bolt.Bucket
		var err error
		if updateType == "status" {
			oldBucket = getBucket(tx, GetStatusBucketPath(queueType, level, oldValue))
			newStatusBucket, err = GetOrCreateStatusBucket(tx, queueType, level, newValue)
			if err != nil {
				return fmt.Errorf("failed to get new status bucket: %w", err)
			}
		}

		for _, nodeIDStr := range nodeIDs {
			nodeID := []byte(nodeIDStr)
			nodeData := nodesBucket.Get(nodeID)
			if nodeData == nil {
				continue
			}

			ns, err := DeserializeNodeState(nodeData)
			if err != nil {
				return fmt.Errorf("failed to deserialize node state: %w", err)
			}

			switch updateType {
			case "status":
				// Update traversal status
				ns.TraversalStatus = newValue

				updatedData, err := ns.Serialize()
				if err != nil {
					return fmt.Errorf("failed to serialize node state: %w", err)
				}

				if err := nodesBucket.Put(nodeID, updatedData); err != nil {
					return fmt.Errorf("failed to update node: %w", err)
				}

				// Update status bucket membership
				if oldBucket != nil {
					oldBucket.Delete(nodeID) // Ignore errors
				}
				if err := newStatusBucket.Put(nodeID, []byte{}); err != nil {
					return fmt.Errorf("failed to add to new status bucket: %w", err)
				}

				// Update status-lookup index
				if err := UpdateStatusLookup(tx, queueType, level, nodeID, newValue); err != nil {
					return fmt.Errorf("failed to update status-lookup: %w", err)
				}
				results[nodeIDStr] = ns

			case "copy":
				// Update copy status
				ns.CopyStatus = newValue
				ns.CopyNeeded = (newValue == CopyStatusPending)

				updatedData, err := ns.Serialize()
				if err != nil {
					return fmt.Errorf("failed to serialize node state: %w", err)
				}

				if err := nodesBucket.Put(nodeID, updatedData); err != nil {
					return fmt.Errorf("failed to update node: %w", err)
				}
				results[nodeIDStr] = ns

			default:
				return fmt.Errorf("unknown updateType: %s", updateType)
			}
		}
		return nil
	})

	return results, err
}
