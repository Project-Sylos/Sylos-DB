// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package store

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Project-Sylos/Sylos-DB/pkg/bolt"
	bbolt "go.etcd.io/bbolt"
)

// BarrierReason represents the semantic reason for establishing a consistency barrier.
type BarrierReason int

const (
	// BarrierRoundCompletion is called before checking if a round is complete
	BarrierRoundCompletion BarrierReason = iota
	// BarrierRoundAdvance is called before advancing to the next round
	BarrierRoundAdvance
	// BarrierPhaseTransition is called between major phases (traversal/copy/exclusion)
	BarrierPhaseTransition
	// BarrierCompletionCheck is called before final "done" determination
	BarrierCompletionCheck
	// BarrierModeSwitch is called when switching between parallel/serial modes
	BarrierModeSwitch
)

// Store provides a domain-focused API for migration state management.
// It consolidates multi-step operations, provides read-your-writes consistency,
// and exposes semantic barriers for coordination points.
type Store struct {
	db      *bolt.DB
	buffers map[string]*writeBuffer // Topic-based buffers: "src", "dst", "logs"
}

// writeBuffer tracks pending write operations for a specific topic.
type writeBuffer struct {
	topic        string          // "src", "dst", or "logs"
	operations   []writeOp       // Buffered write operations
	dirtyBuckets map[string]bool // Bucket paths (as strings) that have pending writes
	mu           sync.Mutex      // Protects operations and dirtyBuckets

	// Buffer configuration
	maxSize       int
	flushInterval time.Duration
	ticker        *time.Ticker
	stopChan      chan struct{}
}

// writeOp represents a pending write operation.
type writeOp struct {
	fn          func(*bbolt.Tx) error // Write operation function
	buckets     []string              // Bucket paths this write touches (normalized to strings)
	statsDeltas map[string]int64      // Bucket path → delta (for accumulation)
}

// normalizeBucketPath converts a bucket path []string to a normalized string for comparison.
// Example: ["Traversal-Data", "SRC", "nodes"] -> "Traversal-Data/SRC/nodes"
func normalizeBucketPath(path []string) string {
	return strings.Join(path, "/")
}

// getTopicForQueueType returns the buffer topic for a given queue type.
// "SRC" -> "src", "DST" -> "dst", anything else -> "src" (default)
func getTopicForQueueType(queueType string) string {
	switch queueType {
	case "SRC":
		return "src"
	case "DST":
		return "dst"
	default:
		return "src" // Default to src
	}
}

// Open creates a new Store instance wrapping a bolt.DB.
func Open(dbPath string) (*Store, error) {
	db, err := bolt.Open(bolt.Options{Path: dbPath})
	if err != nil {
		return nil, err
	}

	s := &Store{
		db:      db,
		buffers: make(map[string]*writeBuffer),
	}

	// Create topic-based buffers
	topics := []string{"src", "dst", "logs"}
	for _, topic := range topics {
		buffer := &writeBuffer{
			topic:         topic,
			operations:    make([]writeOp, 0, 1000),
			dirtyBuckets:  make(map[string]bool),
			maxSize:       1000,
			flushInterval: 2 * time.Second,
			stopChan:      make(chan struct{}),
		}
		buffer.ticker = time.NewTicker(buffer.flushInterval)
		s.buffers[topic] = buffer

		// Start background flush ticker for each buffer
		go buffer.flushLoop(s)
	}

	return s, nil
}

// Close flushes any pending writes and closes the underlying database.
func (s *Store) Close() error {
	// Stop all background flushers and flush buffers
	for topic, buffer := range s.buffers {
		close(buffer.stopChan)
		if buffer.ticker != nil {
			buffer.ticker.Stop()
		}
		if err := s.flushBuffer(topic); err != nil {
			return fmt.Errorf("failed to flush %s buffer: %w", topic, err)
		}
	}

	return s.db.Close()
}

// Barrier establishes a consistency checkpoint at a semantic coordination point.
// All prior writes are guaranteed to be visible, consistent, and durable after this call.
func (s *Store) Barrier(reason BarrierReason) error {
	// Flush all buffered writes (flushBuffer handles its own locking)
	for topic := range s.buffers {
		if err := s.flushBuffer(topic); err != nil {
			return fmt.Errorf("failed to flush %s buffer: %w", topic, err)
		}
	}

	// For critical barriers, ensure durability with fsync
	// BoltDB syncs on Update() commit by default, so this is already durable
	// Future: could add explicit sync if needed

	return nil
}

// flushBuffer writes all buffered operations for a topic to the database in ONE transaction.
// topic is the buffer topic ("src", "dst", or "logs").
// This function handles its own locking.
func (s *Store) flushBuffer(topic string) error {
	buffer := s.buffers[topic]
	if buffer == nil {
		return fmt.Errorf("unknown buffer topic: %s", topic)
	}

	buffer.mu.Lock()
	defer buffer.mu.Unlock()

	if len(buffer.operations) == 0 {
		return nil
	}

	// Accumulate all stats deltas from buffered operations
	accumulatedStats := make(map[string]int64)
	for _, op := range buffer.operations {
		for bucketPath, delta := range op.statsDeltas {
			accumulatedStats[bucketPath] += delta
		}
	}

	// Execute ALL operations in ONE transaction
	err := s.db.Update(func(tx *bbolt.Tx) error {
		// Execute all buffered write operations
		for _, op := range buffer.operations {
			if err := op.fn(tx); err != nil {
				return err
			}
		}

		// Apply accumulated stats deltas (once per bucket)
		for bucketPath, delta := range accumulatedStats {
			if delta == 0 {
				continue // Skip no-op
			}
			// Convert string path back to []string
			pathParts := strings.Split(bucketPath, "/")
			if err := bolt.UpdateBucketStats(tx, pathParts, delta); err != nil {
				return fmt.Errorf("failed to update stats for %s: %w", bucketPath, err)
			}
		}

		return nil
	})

	if err != nil {
		return err
	}

	// Clear buffer and dirty buckets
	buffer.operations = buffer.operations[:0]
	buffer.dirtyBuckets = make(map[string]bool)

	return nil
}

// checkConflict checks if a read would conflict with buffered writes.
// bucketPaths is the list of bucket paths the read operation touches (as [][]string, each will be normalized).
// topic is the buffer topic to check ("src", "dst", or "logs").
// If conflict detected, flushes the buffer before returning.
// For "logs" topic, this always returns nil (logs are eventually consistent).
func (s *Store) checkConflict(topic string, bucketPaths [][]string) error {
	// Logs are eventually consistent - no conflict checking
	if topic == "logs" {
		return nil
	}

	buffer := s.buffers[topic]
	if buffer == nil {
		return fmt.Errorf("unknown buffer topic: %s", topic)
	}

	buffer.mu.Lock()
	hasConflict := false

	// Normalize and check if any of the requested buckets have pending writes
	for _, path := range bucketPaths {
		normalizedPath := normalizeBucketPath(path)
		if buffer.dirtyBuckets[normalizedPath] {
			hasConflict = true
			break
		}
	}

	buffer.mu.Unlock()

	if hasConflict {
		// flushBuffer handles its own locking
		return s.flushBuffer(topic)
	}

	return nil
}

// queueWrite adds a write operation to the buffer.
// topic is the buffer topic ("src", "dst", or "logs").
// fn is the write operation function.
// bucketPaths is the list of bucket paths this write touches (as [][]string, each will be normalized).
// statsDeltas is the map of bucket path strings (normalized) to stat deltas.
func (s *Store) queueWrite(topic string, fn func(*bbolt.Tx) error, bucketPaths [][]string, statsDeltas map[string]int64) error {
	buffer := s.buffers[topic]
	if buffer == nil {
		return fmt.Errorf("unknown buffer topic: %s", topic)
	}

	buffer.mu.Lock()
	defer buffer.mu.Unlock()

	// Normalize bucket paths to strings
	normalizedPaths := make([]string, len(bucketPaths))
	for i, path := range bucketPaths {
		normalizedPaths[i] = normalizeBucketPath(path)
	}

	// Add operation to buffer
	buffer.operations = append(buffer.operations, writeOp{
		fn:          fn,
		buckets:     normalizedPaths,
		statsDeltas: statsDeltas,
	})

	// Mark affected buckets as dirty
	for _, path := range normalizedPaths {
		buffer.dirtyBuckets[path] = true
	}

	// Flush if buffer is full
	if len(buffer.operations) >= buffer.maxSize {
		buffer.mu.Unlock()
		err := s.flushBuffer(topic) // flushBuffer will acquire buffer.mu itself
		buffer.mu.Lock()
		return err
	}

	return nil
}

// flushLoop periodically flushes the buffer in the background.
func (wb *writeBuffer) flushLoop(s *Store) {
	for {
		select {
		case <-wb.stopChan:
			return
		case <-wb.ticker.C:
			s.flushBuffer(wb.topic) // flushBuffer handles its own locking
		}
	}
}
