// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package buffer

import (
	"fmt"
	"strings"
	"sync"
	"time"

	boltDB "github.com/Project-Sylos/Sylos-DB/pkg/bolt"
	bolt "go.etcd.io/bbolt"
)

// Buffer batches write operations for efficient database writes.
// It supports flush triggers: forced, size threshold, and time-based.
// Stats updates are aggregated before being applied.
type Buffer struct {
	db              *boltDB.DB
	mu              sync.Mutex
	operations      []*boltDB.WriteOperation
	batchSize       int
	flushInterval   time.Duration
	flushTicker     *time.Ticker
	stopChan        chan struct{}
	wg              sync.WaitGroup
	paused          bool
	enableTimer     bool
	enableThreshold bool
}

// Config holds buffer configuration options.
type Config struct {
	BatchSize       int           // Number of operations before threshold flush (default: 1000)
	FlushInterval   time.Duration // Time interval for timer-based flush (default: 2 seconds)
	EnableTimer     bool          // Enable timer-based flushing (default: true)
	EnableThreshold bool          // Enable threshold-based flushing (default: true)
}

// DefaultConfig returns default buffer configuration.
func DefaultConfig() Config {
	return Config{
		BatchSize:       1000,
		FlushInterval:   2 * time.Second,
		EnableTimer:     true,
		EnableThreshold: true,
	}
}

// New creates a new buffer with the specified configuration.
func New(db *boltDB.DB, config Config) *Buffer {
	b := &Buffer{
		db:              db,
		operations:      make([]*boltDB.WriteOperation, 0, config.BatchSize),
		batchSize:       config.BatchSize,
		flushInterval:   config.FlushInterval,
		stopChan:        make(chan struct{}),
		paused:          false,
		enableTimer:     config.EnableTimer,
		enableThreshold: config.EnableThreshold,
	}

	// Start timer-based flushing if enabled
	if b.enableTimer {
		b.flushTicker = time.NewTicker(b.flushInterval)
		b.wg.Add(1)
		go b.flushLoop()
	}

	return b
}

// QueueWrite queues a write operation to the buffer.
// If threshold-based flushing is enabled and batch size is reached, it triggers a flush.
func (b *Buffer) QueueWrite(op *boltDB.WriteOperation) {
	if op == nil {
		return
	}

	b.mu.Lock()
	b.operations = append(b.operations, op)
	shouldFlush := b.enableThreshold && len(b.operations) >= b.batchSize
	b.mu.Unlock()

	if shouldFlush {
		b.Flush()
	}
}

// Flush writes all buffered operations to BoltDB in a single transaction.
// Operations are executed in the order they were added to the buffer.
// Stats updates are aggregated and applied in a single batch.
// This is synchronous and blocks until the flush completes.
func (b *Buffer) Flush() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.operations) == 0 {
		return nil
	}

	// Take snapshot and clear buffer
	batch := make([]*boltDB.WriteOperation, len(b.operations))
	copy(batch, b.operations)
	b.operations = make([]*boltDB.WriteOperation, 0, b.batchSize)

	// Aggregate stats updates before executing writes
	statsMap := make(map[string]int64)
	for _, op := range batch {
		if op.StatsUpdate != nil {
			pathStr := strings.Join(op.StatsUpdate.StatsBucketPath, "/")
			statsMap[pathStr] += op.StatsUpdate.Delta
		}
	}

	// Execute all operations in a single transaction
	err := b.db.Update(func(tx *bolt.Tx) error {
		// Execute all write operations in order
		for i, op := range batch {
			if err := op.Execute(tx); err != nil {
				return fmt.Errorf("failed to execute operation %d of %d: %w", i+1, len(batch), err)
			}
		}

		// Apply aggregated stats updates
		if len(statsMap) > 0 {
			if err := boltDB.UpdateBucketStatsBatch(tx, statsMap); err != nil {
				return fmt.Errorf("failed to update stats: %w", err)
			}
		}

		return nil
	})

	if err != nil {
		// Log error with details
		fmt.Printf("ERROR flushing buffer (%d operations): %v\n", len(batch), err)
		// Re-add operations to buffer for retry
		b.operations = append(b.operations, batch...)
		return err
	}

	return nil
}

// Clear clears the buffer without flushing.
// This discards all queued operations.
func (b *Buffer) Clear() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.operations = make([]*boltDB.WriteOperation, 0, b.batchSize)
}

// flushLoop runs in a goroutine and periodically flushes the buffer.
func (b *Buffer) flushLoop() {
	defer b.wg.Done()

	for {
		select {
		case <-b.flushTicker.C:
			b.mu.Lock()
			paused := b.paused
			b.mu.Unlock()
			if !paused {
				b.Flush()
			}
		case <-b.stopChan:
			b.flushTicker.Stop()
			b.Flush() // Final flush before stopping
			return
		}
	}
}

// Pause pauses the buffer (stops time-based flushing).
// Force-flushes before pausing to ensure state is persisted.
func (b *Buffer) Pause() error {
	err := b.Flush() // Force flush before pausing
	b.mu.Lock()
	b.paused = true
	b.mu.Unlock()
	return err
}

// Resume resumes the buffer (resumes time-based flushing).
func (b *Buffer) Resume() {
	b.mu.Lock()
	b.paused = false
	b.mu.Unlock()
}

// SetBatchSize sets the batch size threshold for flushing.
// Takes effect on next flush.
func (b *Buffer) SetBatchSize(size int) {
	if size <= 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.batchSize = size
}

// SetFlushInterval sets the flush interval for timer-based flushing.
// Takes effect after restarting the timer.
func (b *Buffer) SetFlushInterval(interval time.Duration) {
	if interval <= 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	wasRunning := b.flushTicker != nil && !b.paused
	if wasRunning && b.flushTicker != nil {
		b.flushTicker.Stop()
	}

	b.flushInterval = interval

	if wasRunning && b.enableTimer {
		b.flushTicker = time.NewTicker(b.flushInterval)
		b.wg.Add(1)
		go b.flushLoop()
	}
}

// EnableTimerFlush enables timer-based flushing.
func (b *Buffer) EnableTimerFlush() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.enableTimer {
		return // Already enabled
	}

	b.enableTimer = true
	b.flushTicker = time.NewTicker(b.flushInterval)
	b.wg.Add(1)
	go b.flushLoop()
}

// DisableTimerFlush disables timer-based flushing.
func (b *Buffer) DisableTimerFlush() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.enableTimer {
		return // Already disabled
	}

	b.enableTimer = false
	if b.flushTicker != nil {
		b.flushTicker.Stop()
		b.flushTicker = nil
	}
}

// EnableThresholdFlush enables threshold-based flushing.
func (b *Buffer) EnableThresholdFlush() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.enableThreshold = true
}

// DisableThresholdFlush disables threshold-based flushing.
func (b *Buffer) DisableThresholdFlush() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.enableThreshold = false
}

// Stop gracefully stops the buffer and flushes remaining operations.
// Uses a timeout to prevent indefinite blocking if the flush loop is stuck.
func (b *Buffer) Stop() error {
	if b.flushTicker != nil {
		close(b.stopChan)

		// Wait for flush loop to finish, but with a timeout to prevent hanging
		done := make(chan struct{}, 1)
		go func() {
			b.wg.Wait()
			done <- struct{}{}
		}()

		select {
		case <-done:
			// Flush loop completed successfully
		case <-time.After(b.flushInterval):
			// Timeout - flush loop may be stuck or slow
			// Continue anyway to prevent blocking the entire shutdown
		}
	}

	// Final flush
	return b.Flush()
}
