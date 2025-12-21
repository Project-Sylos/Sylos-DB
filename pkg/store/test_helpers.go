// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package store

import (
	"github.com/Project-Sylos/Sylos-DB/pkg/bolt"
)

// SubtreeStats contains statistics about a subtree.
// Used for test utilities to verify tree structure and counts.
type SubtreeStats struct {
	TotalNodes   int
	TotalFolders int
	TotalFiles   int
	MaxDepth     int
}

// CountSubtree performs a DFS traversal to count all nodes in a subtree.
// Returns statistics including total nodes, folders, files, and max depth.
//
// This is a test utility method and should not be used in production code.
// It performs O(n) traversal of the entire subtree.
func (s *Store) CountSubtree(queueType, rootPath string) (SubtreeStats, error) {
	stats := SubtreeStats{}
	visited := make(map[string]bool)

	var dfs func(nodeID string, depth int) error
	dfs = func(nodeID string, depth int) error {
		if visited[nodeID] {
			return nil
		}
		visited[nodeID] = true

		// Domain dependency check happens in GetNode
		node, err := s.GetNode(queueType, nodeID)
		if err != nil {
			return err
		}

		stats.TotalNodes++
		if node.Type == "folder" {
			stats.TotalFolders++
		} else {
			stats.TotalFiles++
		}
		if depth > stats.MaxDepth {
			stats.MaxDepth = depth
		}

		if node.Type == "folder" {
			childIDsIface, err := s.GetChildren(queueType, nodeID, "ids")
			if err != nil {
				return err
			}
			childIDs, ok := childIDsIface.([]string)
			if !ok {
				return nil // No children or conversion failed
			}

			for _, childID := range childIDs {
				if err := dfs(childID, depth+1); err != nil {
					return err
				}
			}
		}

		return nil
	}

	rootNode, err := s.GetNodeByPath(queueType, rootPath)
	if err != nil {
		return stats, err
	}

	if err := dfs(rootNode.ID, 0); err != nil {
		return stats, err
	}

	return stats, nil
}

// DeleteSubtree performs a DFS traversal to delete all nodes in a subtree from the database.
// This removes nodes from the nodes bucket, status buckets, children bucket, and status-lookup index.
// Does NOT delete the root node itself - only its children.
//
// This is a test utility method and should not be used in production code.
// It performs O(n) traversal and batch deletion of all nodes in the subtree.
func (s *Store) DeleteSubtree(queueType, rootPath string) error {
	visited := make(map[string]bool)
	var nodesToDelete []string

	// First pass: collect all nodes in subtree (children only, not root)
	var dfsCollect func(nodeID string) error
	dfsCollect = func(nodeID string) error {
		if visited[nodeID] {
			return nil
		}
		visited[nodeID] = true

		// Get node state
		node, err := s.GetNode(queueType, nodeID)
		if err != nil {
			return err
		}

		// Collect this node
		nodesToDelete = append(nodesToDelete, nodeID)

		// Recurse into children (if folder)
		if node.Type == "folder" {
			childIDsIface, err := s.GetChildren(queueType, nodeID, "ids")
			if err != nil {
				return err
			}
			childIDs, ok := childIDsIface.([]string)
			if !ok {
				return nil // No children or conversion failed
			}

			for _, childID := range childIDs {
				if err := dfsCollect(childID); err != nil {
					return err
				}
			}
		}

		return nil
	}

	// Find root node by path
	rootNode, err := s.GetNodeByPath(queueType, rootPath)
	if err != nil {
		return err
	}

	// Collect all children of the root node (but NOT the root node itself)
	if rootNode.Type == "folder" {
		childIDsIface, err := s.GetChildren(queueType, rootNode.ID, "ids")
		if err != nil {
			return err
		}
		childIDs, ok := childIDsIface.([]string)
		if !ok {
			return nil // No children
		}

		for _, childID := range childIDs {
			if err := dfsCollect(childID); err != nil {
				return err
			}
		}
	}

	// Second pass: delete all collected nodes using Store API
	// Use queueWrite to batch the operation
	if len(nodesToDelete) == 0 {
		return nil // Nothing to delete
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Check conflicts before deletion (all levels since we don't know which levels are affected)
	qr := QueueRound{QueueType: queueType, Level: -1}
	if err := s.checkDomainConflict(DomainDependency{
		Nodes:  []QueueRound{qr},
		Status: []QueueRound{qr},
	}); err != nil {
		return err
	}

	// Queue the batch delete operation
	return s.queueWrite(func(db *bolt.DB) error {
		return bolt.BatchDeleteNodes(db, queueType, nodesToDelete)
	}, DomainImpact{
		Pending: []QueueRound{qr}, // May affect pending
		Status:  []QueueRound{qr}, // Affects status buckets
		Nodes:   []QueueRound{qr}, // Affects node data
		Lookups: true,             // Affects children index, path-to-ULID, join mappings
		Stats:   true,             // Affects stats
	})
}

// CountExcludedNodes counts all excluded nodes (ExplicitExcluded OR InheritedExcluded) in a queue.
// A node is considered excluded if it is either explicitly excluded by the user or inherited exclusion from a parent.
//
// This is a test utility method and should not be used in production code.
// It performs O(n) iteration through all nodes in the queue.
func (s *Store) CountExcludedNodes(queueType string) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check conflicts before reading (all levels)
	qr := QueueRound{QueueType: queueType, Level: -1}
	if err := s.checkDomainConflict(DomainDependency{
		Nodes: []QueueRound{qr},
	}); err != nil {
		return 0, err
	}

	count := 0
	err := s.db.IterateNodeStates(queueType, bolt.IteratorOptions{}, func(nodeIDBytes []byte, state *bolt.NodeState) error {
		// Count if explicitly excluded OR inherited excluded
		if state.ExplicitExcluded || state.InheritedExcluded {
			count++
		}
		return nil
	})

	return count, err
}

// CountExcludedInSubtree counts excluded nodes (ExplicitExcluded OR InheritedExcluded) within a subtree.
// Performs DFS traversal starting from the root path.
//
// This is a test utility method and should not be used in production code.
// It performs O(n) traversal of the entire subtree.
func (s *Store) CountExcludedInSubtree(queueType, rootPath string) (int, error) {
	count := 0
	visited := make(map[string]bool)

	var dfs func(nodeID string) error
	dfs = func(nodeID string) error {
		if visited[nodeID] {
			return nil
		}
		visited[nodeID] = true

		// Domain dependency check happens in GetNode
		node, err := s.GetNode(queueType, nodeID)
		if err != nil {
			return err
		}

		// Count if excluded (explicitly OR inherited)
		if node.ExplicitExcluded || node.InheritedExcluded {
			count++
		}

		// Recurse into children (if folder)
		if node.Type == "folder" {
			childIDsIface, err := s.GetChildren(queueType, nodeID, "ids")
			if err != nil {
				return err
			}
			childIDs, ok := childIDsIface.([]string)
			if !ok {
				return nil // No children or conversion failed
			}

			for _, childID := range childIDs {
				if err := dfs(childID); err != nil {
					return err
				}
			}
		}

		return nil
	}

	// Find root node by path
	rootNode, err := s.GetNodeByPath(queueType, rootPath)
	if err != nil {
		return 0, err
	}

	if err := dfs(rootNode.ID); err != nil {
		return 0, err
	}

	return count, nil
}
