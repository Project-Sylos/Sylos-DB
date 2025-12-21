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
	// Stats bucket is shared - check both src and dst buffers
	// Try to determine topic from path, default to checking both
	topic := "src" // Default
	if len(bucketPath) > 1 {
		if bucketPath[1] == "DST" {
			topic = "dst"
		}
	}

	// Check conflicts: reads from stats bucket (which is shared, but path might indicate queue)
	// For simplicity, check the relevant topic's buffer
	bucketsToRead := [][]string{
		{"Traversal-Data", "STATS"}, // Stats bucket
	}
	if err := s.checkConflict(topic, bucketsToRead); err != nil {
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
	// Join mappings span both queues - we need to queue writes to both buffers
	// But we can do this as two separate queue writes since they're independent buckets

	// Write to src buffer (src-to-dst bucket)
	srcBuckets := [][]string{
		bolt.GetSrcToDstBucketPath(),
	}
	if err := s.queueWrite("src", func(tx *bbolt.Tx) error {
		return bolt.SetJoinMappingInTx(tx, "src-to-dst", srcID, dstID)
	}, srcBuckets, nil); err != nil {
		return err
	}

	// Write to dst buffer (dst-to-src bucket)
	dstBuckets := [][]string{
		bolt.GetDstToSrcBucketPath(),
	}
	return s.queueWrite("dst", func(tx *bbolt.Tx) error {
		return bolt.SetJoinMappingInTx(tx, "dst-to-src", dstID, srcID)
	}, dstBuckets, nil)
}

// GetJoinMapping retrieves a node mapping from the join lookup table.
// mappingType is either "src-to-dst" or "dst-to-src".
// For "src-to-dst": sourceID is SRC node ULID, returns DST node ULID.
// For "dst-to-src": sourceID is DST node ULID, returns SRC node ULID.
// Returns empty string if no mapping exists.
func (s *Store) GetJoinMapping(mappingType, sourceID string) (string, error) {
	var topic string
	var bucketsToRead [][]string

	if mappingType == "src-to-dst" {
		topic = "src"
		bucketsToRead = [][]string{
			bolt.GetSrcToDstBucketPath(),
		}
	} else {
		topic = "dst"
		bucketsToRead = [][]string{
			bolt.GetDstToSrcBucketPath(),
		}
	}

	// Check conflicts before reading
	if err := s.checkConflict(topic, bucketsToRead); err != nil {
		return "", err
	}

	return bolt.GetJoinMapping(s.db, mappingType, sourceID)
}

// SetPathMapping creates a path→ULID mapping for a node.
// This allows looking up nodes by their filesystem path.
// The path is hashed using SHA-256 for storage efficiency.
func (s *Store) SetPathMapping(queueType, path, nodeID string) error {
	topic := getTopicForQueueType(queueType)

	// Check conflicts: path mappings bucket
	bucketsToRead := [][]string{
		bolt.GetPathToULIDBucketPath(queueType),
	}
	if err := s.checkConflict(topic, bucketsToRead); err != nil {
		return err
	}

	// Queue the write operation
	bucketsToWrite := [][]string{
		bolt.GetPathToULIDBucketPath(queueType),
	}
	return s.queueWrite(topic, func(tx *bbolt.Tx) error {
		return bolt.SetPathToULIDMapping(tx, queueType, path, nodeID)
	}, bucketsToWrite, nil)
}
