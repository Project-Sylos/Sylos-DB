// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package store

import (
	"fmt"
	"time"

	"github.com/Project-Sylos/Sylos-DB/pkg/bolt"
	"github.com/Project-Sylos/Sylos-DB/pkg/utils"
	bbolt "go.etcd.io/bbolt"
)

// RecordLog writes a log entry to the database.
// Logs are buffered and flushed automatically.
func (s *Store) RecordLog(level, entity, entityID, message string) error {
	if level == "" || message == "" {
		return fmt.Errorf("level and message cannot be empty")
	}

	// Create log entry
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
		// Serialize log entry
		data, err := bolt.SerializeLogEntry(entry)
		if err != nil {
			return fmt.Errorf("failed to serialize log entry: %w", err)
		}

		// Write to log level bucket
		return db.Update(func(tx *bbolt.Tx) error {
			levelBucket, err := bolt.GetOrCreateLogLevelBucket(tx, level)
			if err != nil {
				return fmt.Errorf("failed to get log level bucket: %w", err)
			}

			return levelBucket.Put([]byte(entry.ID), data)
		})
	}, "logs")
}

// QueryLogs retrieves log entries matching the specified filters.
// For now, this is a simple implementation that can be enhanced later.
func (s *Store) QueryLogs(level string, limit int) ([]*bolt.LogEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check conflicts and flush if needed
	if err := s.checkConflict("logs"); err != nil {
		return nil, err
	}

	var logs []*bolt.LogEntry

	// Query log level bucket
	err := s.db.View(func(tx *bbolt.Tx) error {
		levelBucket, err := bolt.GetOrCreateLogLevelBucket(tx, level)
		if err != nil {
			return err
		}

		cursor := levelBucket.Cursor()
		count := 0

		for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
			if limit > 0 && count >= limit {
				break
			}

			entry, err := bolt.DeserializeLogEntry(v)
			if err != nil {
				continue // Skip invalid entries
			}

			logs = append(logs, entry)
			count++
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return logs, nil
}
