// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package store

import (
	"sync"
	"time"

	"github.com/Project-Sylos/Sylos-DB/pkg/bolt"
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
	db     *bolt.DB
	buffer *writeBuffer
	mu     sync.RWMutex
}

// writeBuffer tracks pending write operations and which buckets they affect.
type writeBuffer struct {
	operations      []writeOp
	affectedBuckets map[string]bool
	mu              sync.Mutex
	
	// Buffer configuration
	maxSize         int
	flushInterval   time.Duration
	ticker          *time.Ticker
	stopChan        chan struct{}
}

// writeOp represents a pending write operation.
type writeOp struct {
	fn func(*bolt.DB) error
	affectedBuckets []string
}

// Open creates a new Store instance wrapping a bolt.DB.
func Open(dbPath string) (*Store, error) {
	db, err := bolt.Open(bolt.Options{Path: dbPath})
	if err != nil {
		return nil, err
	}

	s := &Store{
		db: db,
		buffer: &writeBuffer{
			operations:      make([]writeOp, 0, 1000),
			affectedBuckets: make(map[string]bool),
			maxSize:         1000,
			flushInterval:   2 * time.Second,
			stopChan:        make(chan struct{}),
		},
	}

	// Start background flush ticker
	s.buffer.ticker = time.NewTicker(s.buffer.flushInterval)
	go s.buffer.flushLoop(s)

	return s, nil
}

// Close flushes any pending writes and closes the underlying database.
func (s *Store) Close() error {
	// Stop background flusher
	close(s.buffer.stopChan)
	if s.buffer.ticker != nil {
		s.buffer.ticker.Stop()
	}

	// Flush any remaining writes
	if err := s.flushBuffer(); err != nil {
		return err
	}

	return s.db.Close()
}

// Barrier establishes a consistency checkpoint at a semantic coordination point.
// All prior writes are guaranteed to be visible, consistent, and durable after this call.
func (s *Store) Barrier(reason BarrierReason) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Flush all buffered writes
	if err := s.flushBuffer(); err != nil {
		return err
	}

	// For critical barriers, ensure durability with fsync
	// BoltDB syncs on Update() commit by default, so this is already durable
	// Future: could add explicit sync if needed

	return nil
}

// flushBuffer writes all buffered operations to the database.
// Caller must hold s.mu lock.
func (s *Store) flushBuffer() error {
	s.buffer.mu.Lock()
	defer s.buffer.mu.Unlock()

	if len(s.buffer.operations) == 0 {
		return nil
	}

	// Execute all buffered operations
	for _, op := range s.buffer.operations {
		if err := op.fn(s.db); err != nil {
			return err
		}
	}

	// Clear buffer
	s.buffer.operations = s.buffer.operations[:0]
	s.buffer.affectedBuckets = make(map[string]bool)

	return nil
}

// checkConflict checks if a read would conflict with buffered writes.
// If conflict detected, flushes buffer before returning.
// Caller must hold s.mu lock.
func (s *Store) checkConflict(readBuckets ...string) error {
	s.buffer.mu.Lock()
	hasConflict := false
	for _, bucket := range readBuckets {
		if s.buffer.affectedBuckets[bucket] {
			hasConflict = true
			break
		}
	}
	s.buffer.mu.Unlock()

	if hasConflict {
		return s.flushBuffer()
	}

	return nil
}

// queueWrite adds a write operation to the buffer.
func (s *Store) queueWrite(fn func(*bolt.DB) error, affectedBuckets ...string) error {
	s.buffer.mu.Lock()
	defer s.buffer.mu.Unlock()

	// Add operation to buffer
	s.buffer.operations = append(s.buffer.operations, writeOp{
		fn:              fn,
		affectedBuckets: affectedBuckets,
	})

	// Track affected buckets
	for _, bucket := range affectedBuckets {
		s.buffer.affectedBuckets[bucket] = true
	}

	// Flush if buffer is full
	if len(s.buffer.operations) >= s.buffer.maxSize {
		s.buffer.mu.Unlock()
		s.mu.Lock()
		err := s.flushBuffer()
		s.mu.Unlock()
		s.buffer.mu.Lock()
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
			s.mu.Lock()
			s.flushBuffer()
			s.mu.Unlock()
		}
	}
}

