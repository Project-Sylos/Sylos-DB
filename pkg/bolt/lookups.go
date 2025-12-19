// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package bolt

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// HashPath creates a SHA-256 hash of a path for use as a lookup key.
// This allows path-based queries while using ULIDs as primary keys.
func HashPath(path string) string {
	hash := sha256.Sum256([]byte(path))
	return hex.EncodeToString(hash[:])
}

// GetPathToULIDBucket returns the path-to-ulid lookup bucket for the given queue type.
// Path: /Traversal-Data/{queueType}/path-to-ulid
func GetPathToULIDBucket(tx *bolt.Tx, queueType string) *bolt.Bucket {
	traversalBucket := tx.Bucket([]byte(TraversalDataBucket))
	if traversalBucket == nil {
		return nil
	}

	topBucket := traversalBucket.Bucket([]byte(queueType))
	if topBucket == nil {
		return nil
	}

	return topBucket.Bucket([]byte(SubBucketPathToULID))
}

// GetPathToULIDBucketPath returns the bucket path for path-to-ulid lookup.
func GetPathToULIDBucketPath(queueType string) []string {
	return []string{TraversalDataBucket, queueType, SubBucketPathToULID}
}

// EnsurePathToULIDBucket ensures the path-to-ulid bucket exists.
func EnsurePathToULIDBucket(tx *bolt.Tx, queueType string) (*bolt.Bucket, error) {
	traversalBucket := tx.Bucket([]byte(TraversalDataBucket))
	if traversalBucket == nil {
		return nil, fmt.Errorf("traversal-data bucket not found")
	}

	topBucket := traversalBucket.Bucket([]byte(queueType))
	if topBucket == nil {
		return nil, fmt.Errorf("%s bucket not found", queueType)
	}

	pathBucket, err := topBucket.CreateBucketIfNotExists([]byte(SubBucketPathToULID))
	if err != nil {
		return nil, fmt.Errorf("failed to create path-to-ulid bucket: %w", err)
	}

	return pathBucket, nil
}

// SetPathToULIDMapping creates a mapping from path hash to ULID within a transaction.
func SetPathToULIDMapping(tx *bolt.Tx, queueType string, path string, ulid string) error {
	pathBucket, err := EnsurePathToULIDBucket(tx, queueType)
	if err != nil {
		return err
	}

	pathHash := HashPath(path)
	return pathBucket.Put([]byte(pathHash), []byte(ulid))
}

// isHexHash checks if a string is a 64-character hex string (SHA-256 hash).
func isHexHash(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return true
}

// GetULIDFromPathOrHash retrieves the ULID for a given path or path hash.
// Automatically detects if the input is a path (needs hashing) or a hash (64-char hex string).
func GetULIDFromPathOrHash(db *DB, queueType string, pathOrHash string) (string, error) {
	var ulid string

	err := db.View(func(tx *bolt.Tx) error {
		pathBucket := GetPathToULIDBucket(tx, queueType)
		if pathBucket == nil {
			return fmt.Errorf("path-to-ulid bucket not found for %s", queueType)
		}

		// Determine if input is a hash or a path
		var pathHash string
		if isHexHash(pathOrHash) {
			// Already a hash
			pathHash = pathOrHash
		} else {
			// It's a path, hash it
			pathHash = HashPath(pathOrHash)
		}

		ulidBytes := pathBucket.Get([]byte(pathHash))
		if ulidBytes == nil {
			return fmt.Errorf("path/hash not found: %s", pathOrHash)
		}

		ulid = string(ulidBytes)
		return nil
	})

	return ulid, err
}

// DeletePathToULIDMapping removes a path hash → ULID mapping within a transaction.
func DeletePathToULIDMapping(tx *bolt.Tx, queueType string, path string) error {
	pathBucket := GetPathToULIDBucket(tx, queueType)
	if pathBucket == nil {
		return fmt.Errorf("path-to-ulid bucket not found for %s", queueType)
	}

	pathHash := HashPath(path)
	return pathBucket.Delete([]byte(pathHash))
}

// BatchGetULIDsFromPathsOrHashes retrieves ULIDs for multiple paths or path hashes in a single transaction.
// Automatically detects if each input is a path (needs hashing) or a hash (64-char hex string).
func BatchGetULIDsFromPathsOrHashes(db *DB, queueType string, pathsOrHashes []string) (map[string]string, error) {
	results := make(map[string]string)

	err := db.View(func(tx *bolt.Tx) error {
		pathBucket := GetPathToULIDBucket(tx, queueType)
		if pathBucket == nil {
			return fmt.Errorf("path-to-ulid bucket not found for %s", queueType)
		}

		for _, pathOrHash := range pathsOrHashes {
			// Determine if input is a hash or a path
			var pathHash string
			if isHexHash(pathOrHash) {
				// Already a hash
				pathHash = pathOrHash
			} else {
				// It's a path, hash it
				pathHash = HashPath(pathOrHash)
			}

			ulidBytes := pathBucket.Get([]byte(pathHash))
			if ulidBytes != nil {
				// Use original input as key (preserves whether it was path or hash)
				results[pathOrHash] = string(ulidBytes)
			}
		}

		return nil
	})

	return results, err
}

// SetJoinMapping stores a node mapping in the lookup table.
// direction is either "src-to-dst" or "dst-to-src".
// For "src-to-dst": fromID is SRC node ULID, toID is DST node ULID.
// For "dst-to-src": fromID is DST node ULID, toID is SRC node ULID.
func SetJoinMapping(db *DB, direction string, fromID, toID string) error {
	var bucketPath []string
	switch direction {
	case "src-to-dst":
		bucketPath = GetSrcToDstBucketPath()
	case "dst-to-src":
		bucketPath = GetDstToSrcBucketPath()
	default:
		return fmt.Errorf("invalid direction: %s (must be 'src-to-dst' or 'dst-to-src')", direction)
	}

	return db.Update(func(tx *bolt.Tx) error {
		bucket, err := getOrCreateBucket(tx, bucketPath)
		if err != nil {
			return fmt.Errorf("failed to get join mapping bucket: %w", err)
		}
		return bucket.Put([]byte(fromID), []byte(toID))
	})
}

// GetJoinMapping retrieves a node mapping from the lookup table.
// direction is either "src-to-dst" or "dst-to-src".
// For "src-to-dst": fromID is SRC node ULID, returns DST node ULID.
// For "dst-to-src": fromID is DST node ULID, returns SRC node ULID.
// Returns empty string if no mapping exists.
func GetJoinMapping(db *DB, direction string, fromID string) (string, error) {
	var bucketPath []string
	switch direction {
	case "src-to-dst":
		bucketPath = GetSrcToDstBucketPath()
	case "dst-to-src":
		bucketPath = GetDstToSrcBucketPath()
	default:
		return "", fmt.Errorf("invalid direction: %s (must be 'src-to-dst' or 'dst-to-src')", direction)
	}

	var toID string
	err := db.View(func(tx *bolt.Tx) error {
		bucket := getBucket(tx, bucketPath)
		if bucket == nil {
			return nil // Bucket doesn't exist, no mapping
		}
		value := bucket.Get([]byte(fromID))
		if value != nil {
			toID = string(value)
		}
		return nil
	})
	return toID, err
}

// DeleteJoinMapping removes a node mapping from the lookup table.
// direction is either "src-to-dst" or "dst-to-src".
// For "src-to-dst": fromID is SRC node ULID.
// For "dst-to-src": fromID is DST node ULID.
func DeleteJoinMapping(db *DB, direction string, fromID string) error {
	var bucketPath []string
	switch direction {
	case "src-to-dst":
		bucketPath = GetSrcToDstBucketPath()
	case "dst-to-src":
		bucketPath = GetDstToSrcBucketPath()
	default:
		return fmt.Errorf("invalid direction: %s (must be 'src-to-dst' or 'dst-to-src')", direction)
	}

	return db.Update(func(tx *bolt.Tx) error {
		bucket := getBucket(tx, bucketPath)
		if bucket == nil {
			return nil // Bucket doesn't exist, nothing to delete
		}
		return bucket.Delete([]byte(fromID))
	})
}
