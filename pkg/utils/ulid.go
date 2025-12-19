// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package utils

import (
	"time"

	"github.com/oklog/ulid/v2"
)

// GenerateULID generates a new ULID (Universally Unique Lexicographically Sortable Identifier).
// ULIDs are used as database keys and identifiers throughout the Sylos system.
func GenerateULID() string {
	entropy := ulid.DefaultEntropy()
	now := time.Now()
	id, err := ulid.New(ulid.Timestamp(now), entropy)
	if err != nil {
		// This should never happen in practice, but handle it gracefully
		// Fallback to timestamp-based ID if ULID generation fails
		return ulid.Make().String()
	}
	return id.String()
}
