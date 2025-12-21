// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package store

import (
	"github.com/Project-Sylos/Sylos-DB/pkg/bolt"
	bbolt "go.etcd.io/bbolt"
)

// GetBucketCount returns the count for a bucket from the stats bucket.
// This is an O(1) operation using pre-computed stats.
// bucketPath is the path to the bucket (e.g., ["Traversal-Data", "SRC", "nodes"]).
func (s *Store) GetBucketCount(bucketPath []string) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check conflicts for stats bucket
	if err := s.checkDomainConflict(DomainDependency{
		Stats: true,
	}); err != nil {
		return 0, err
	}

	count, err := s.db.GetBucketCount(bucketPath)
	if err != nil {
		return 0, err
	}

	return int(count), nil
}

// SetJoinMapping creates a SRC↔DST mapping for a node.
// This creates bidirectional mappings: SRC→DST and DST→SRC.
// srcID is the SRC node ULID, dstID is the DST node ULID.
func (s *Store) SetJoinMapping(srcID, dstID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check conflicts before writing
	if err := s.checkDomainConflict(DomainDependency{
		Lookups: true,
	}); err != nil {
		return err
	}

	// Queue the write operation
	return s.queueWrite(func(db *bolt.DB) error {
		// Create both mappings in a single transaction
		if err := bolt.SetJoinMapping(db, "src-to-dst", srcID, dstID); err != nil {
			return err
		}
		return bolt.SetJoinMapping(db, "dst-to-src", dstID, srcID)
	}, DomainImpact{
		Lookups: true, // Affects join mapping lookups
	})
}

// GetJoinMapping retrieves a node mapping from the join lookup table.
// mappingType is either "src-to-dst" or "dst-to-src".
// For "src-to-dst": sourceID is SRC node ULID, returns DST node ULID.
// For "dst-to-src": sourceID is DST node ULID, returns SRC node ULID.
// Returns empty string if no mapping exists.
func (s *Store) GetJoinMapping(mappingType, sourceID string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check conflicts before reading
	if err := s.checkDomainConflict(DomainDependency{
		Lookups: true,
	}); err != nil {
		return "", err
	}

	return bolt.GetJoinMapping(s.db, mappingType, sourceID)
}

// SetPathMapping creates a path→ULID mapping for a node.
// This allows looking up nodes by their filesystem path.
// The path is hashed using SHA-256 for storage efficiency.
func (s *Store) SetPathMapping(queueType, path, nodeID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check conflicts before writing
	if err := s.checkDomainConflict(DomainDependency{
		Lookups: true,
	}); err != nil {
		return err
	}

	// Queue the write operation
	return s.queueWrite(func(db *bolt.DB) error {
		// SetPathToULIDMapping is a TX-level function, we need to wrap it in Update
		return db.Update(func(tx *bbolt.Tx) error {
			return bolt.SetPathToULIDMapping(tx, queueType, path, nodeID)
		})
	}, DomainImpact{
		Lookups: true, // Affects path-to-ULID lookups
	})
}
