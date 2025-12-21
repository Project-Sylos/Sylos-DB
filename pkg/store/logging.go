// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package store

import (
	"time"

	"github.com/Project-Sylos/Sylos-DB/pkg/bolt"
	"github.com/Project-Sylos/Sylos-DB/pkg/utils"
)

// RecordLog writes a log entry to the database.
// level is the log level (e.g., "info", "error", "warning").
// entity is the component/entity that produced the log (e.g., "traversal", "copy").
// entityID is the specific instance ID if applicable.
// message is the log message content.
func (s *Store) RecordLog(level, entity, entityID, message string) error {
	entry := bolt.LogEntry{
		ID:        utils.GenerateULID(),
		Timestamp: time.Now().Format(time.RFC3339Nano),
		Level:     level,
		Entity:    entity,
		EntityID:  entityID,
		Message:   message,
	}

	// Queue the write operation
	return s.queueWrite(func(db *bolt.DB) error {
		return bolt.InsertLogEntry(db, entry)
	}, DomainImpact{
		Logs: true,
	})
}

// QueryLogs retrieves log entries from the database.
// level is the log level to filter by (e.g., "error", "warning", "" for all).
// limit is the maximum number of entries to return (0 = no limit).
// Returns logs in reverse chronological order (newest first).
//
// NOTE: Logs are eventually consistent - this method does NOT flush the buffer.
// Log reads are used for UI polling (200ms interval) and don't require immediate consistency.
// Logs are flushed at semantic barriers (e.g., shutdown, phase transitions) for durability.
func (s *Store) QueryLogs(level string, limit int) ([]*bolt.LogEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Logs are eventually consistent - no conflict check needed
	// UI polling can tolerate slight delays (buffer flushes every 2 seconds by default)

	// Use bolt package methods for querying logs
	if level != "" {
		return bolt.GetLogsByLevel(s.db, level)
	}

	return bolt.GetAllLogs(s.db)
}
