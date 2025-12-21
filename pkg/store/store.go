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

// QueueRound represents a (queueType, level) pair for domain tracking.
type QueueRound struct {
	QueueType string
	Level     int
}

// DirtyDomains tracks which domain slices have unflushed writes.
// This is O(1) metadata, not a scan of buffered operations.
type DirtyDomains struct {
	pendingByRound map[QueueRound]bool // Pending index affected
	statusByRound  map[QueueRound]bool // Status indexes affected
	nodesByRound   map[QueueRound]bool // Node data affected
	lookups        bool                // Path-to-ULID, SRC↔DST mappings
	stats          bool                // Stats bucket affected
	logs           bool                // Log writes
	exclusion      bool                // Exclusion status/holding buckets
	queueStats     bool                // Queue observer metrics
}

// writeBuffer tracks pending write operations and affected domain slices.
type writeBuffer struct {
	operations   []writeOp
	dirtyDomains DirtyDomains
	mu           sync.Mutex

	// Buffer configuration
	maxSize       int
	flushInterval time.Duration
	ticker        *time.Ticker
	stopChan      chan struct{}
}

// writeOp represents a pending write operation with its domain impact.
type writeOp struct {
	fn           func(*bolt.DB) error
	domainImpact DomainImpact
}

// DomainImpact describes which domain slices a write operation affects.
type DomainImpact struct {
	Pending    []QueueRound
	Status     []QueueRound
	Nodes      []QueueRound
	Lookups    bool
	Stats      bool
	Logs       bool
	Exclusion  bool
	QueueStats bool
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
			operations: make([]writeOp, 0, 1000),
			dirtyDomains: DirtyDomains{
				pendingByRound: make(map[QueueRound]bool),
				statusByRound:  make(map[QueueRound]bool),
				nodesByRound:   make(map[QueueRound]bool),
			},
			maxSize:       1000,
			flushInterval: 2 * time.Second,
			stopChan:      make(chan struct{}),
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

	// Clear buffer and dirty domains
	s.buffer.operations = s.buffer.operations[:0]
	s.buffer.dirtyDomains = DirtyDomains{
		pendingByRound: make(map[QueueRound]bool),
		statusByRound:  make(map[QueueRound]bool),
		nodesByRound:   make(map[QueueRound]bool),
	}

	return nil
}

// checkDomainConflict checks if a read would conflict with buffered writes in specific domain slices.
// If conflict detected, flushes buffer before returning.
// Caller must hold s.mu lock.
func (s *Store) checkDomainConflict(deps DomainDependency) error {
	s.buffer.mu.Lock()
	hasConflict := false

	// Check pending index conflicts
	for _, qr := range deps.Pending {
		if s.buffer.dirtyDomains.pendingByRound[qr] {
			hasConflict = true
			break
		}
	}

	// Check status index conflicts
	if !hasConflict {
		for _, qr := range deps.Status {
			if s.buffer.dirtyDomains.statusByRound[qr] {
				hasConflict = true
				break
			}
		}
	}

	// Check node data conflicts
	if !hasConflict {
		for _, qr := range deps.Nodes {
			if s.buffer.dirtyDomains.nodesByRound[qr] {
				hasConflict = true
				break
			}
		}
	}

	// Check global domain conflicts
	if !hasConflict {
		hasConflict = (deps.Lookups && s.buffer.dirtyDomains.lookups) ||
			(deps.Stats && s.buffer.dirtyDomains.stats) ||
			(deps.Logs && s.buffer.dirtyDomains.logs) ||
			(deps.Exclusion && s.buffer.dirtyDomains.exclusion) ||
			(deps.QueueStats && s.buffer.dirtyDomains.queueStats)
	}

	s.buffer.mu.Unlock()

	if hasConflict {
		return s.flushBuffer()
	}

	return nil
}

// DomainDependency describes which domain slices a read operation depends on.
type DomainDependency struct {
	Pending    []QueueRound
	Status     []QueueRound
	Nodes      []QueueRound
	Lookups    bool
	Stats      bool
	Logs       bool
	Exclusion  bool
	QueueStats bool
}

// queueWrite adds a write operation to the buffer with its domain impact.
func (s *Store) queueWrite(fn func(*bolt.DB) error, impact DomainImpact) error {
	s.buffer.mu.Lock()
	defer s.buffer.mu.Unlock()

	// Add operation to buffer
	s.buffer.operations = append(s.buffer.operations, writeOp{
		fn:           fn,
		domainImpact: impact,
	})

	// Mark affected domain slices as dirty
	for _, qr := range impact.Pending {
		s.buffer.dirtyDomains.pendingByRound[qr] = true
	}
	for _, qr := range impact.Status {
		s.buffer.dirtyDomains.statusByRound[qr] = true
	}
	for _, qr := range impact.Nodes {
		s.buffer.dirtyDomains.nodesByRound[qr] = true
	}
	if impact.Lookups {
		s.buffer.dirtyDomains.lookups = true
	}
	if impact.Stats {
		s.buffer.dirtyDomains.stats = true
	}
	if impact.Logs {
		s.buffer.dirtyDomains.logs = true
	}
	if impact.Exclusion {
		s.buffer.dirtyDomains.exclusion = true
	}
	if impact.QueueStats {
		s.buffer.dirtyDomains.queueStats = true
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
