# Sylos-DB

**Database mechanics and invariants for the Sylos migration system.**

Sylos-DB is a shared infrastructure repository that centralizes **database mechanics and invariants** for the Sylos data migration platform. It provides a domain-focused Store API that encapsulates multi-step database operations, guarantees consistency, and enforces correctness without abstracting away the underlying storage details.

## Purpose

Sylos uses multiple databases intentionally, each optimized for different phases:

- **BoltDB** → High-throughput operational state (traversal, copy, retries, logging)
- **DuckDB (SQL)** → Analytical, review, querying, and snapshot inspection

Sylos-DB exists to:

- Encapsulate **dangerous / correctness-sensitive mechanics**
- Reduce duplication (output buffers, batching, flush semantics)
- Provide explicit, trustworthy primitives for Engine & API code
- **Not** hide business intent or query meaning

## Core Design Principle

> **Sylos-DB owns mechanics, not meaning.**

Sylos-DB:
- Enforces invariants
- Guarantees consistency
- Exposes safe verbs
- Does **not** decide *why* something is queried or filtered

The Engine and API remain responsible for:
- Business logic
- UI-driven intent
- Query composition
- User-facing behavior

## Architecture

```
Sylos-DB/
├── docs/               # Detailed documentation (see docs/README.md)
├── pkg/
│   ├── store/          # Domain-focused Store API (public)
│   ├── bolt/           # BoltDB primitives and mechanics (internal)
│   ├── duck/           # DuckDB support (future)
│   └── utils/          # Shared utilities (ULID generation, etc.)
└── README.md           # This file (repository overview)
```

### Package Structure

#### `pkg/store` - Public API

The Store API is the **only** interface that Migration-Engine and API code should use. It provides:

- **Domain verbs**: `RegisterNode`, `TransitionNodeStatus`, `MarkExcluded`
- **Read-your-writes consistency**: Automatic conflict detection and flush-on-read
- **Semantic barriers**: Coordination points for round completion, phase transitions
- **Atomic operations**: Multi-step database operations consolidated into single calls

**Never import `go.etcd.io/bbolt` or use `pkg/bolt` directly from Engine/API code.**

#### `pkg/bolt` - Internal Implementation

Low-level BoltDB operations, bucket management, and storage primitives. This package is **internal implementation detail** and should not be imported by Engine or API code.

#### `pkg/utils` - Shared Utilities

Reusable utilities like ULID generation used across the codebase.

## Key Features

### 1. Automatic Read-Your-Writes Consistency

The Store automatically flushes buffered writes before reads that depend on them:

```go
store.RegisterNode("SRC", level, status, state)  // Buffered internally
node := store.GetNode(nodeID)                    // Auto-flush → correct data
```

No explicit flush calls needed on the hot path.

### 2. Semantic Barriers

At coordination points (round completion checks, phase transitions), use barriers instead of explicit flush calls:

```go
store.Barrier(store.BarrierRoundCompletion)
hasPending := store.HasPendingAtLevel("SRC", level)
```

Barriers declare **intent** (why), while Store controls the **mechanism** (how).

### 3. Consolidated Operations

Operations like "register node" automatically handle all related updates:

- Node insertion
- Status bucket membership
- Index maintenance (status-lookup, children)
- Stats updates
- Join mappings (SRC↔DST)

What used to require 5+ separate operations is now one method call.

### 4. Internal Buffering

Write operations are buffered internally for performance:
- Default: 1000 entries or 2 seconds
- Automatic flush on read conflicts
- Explicit flush at semantic barriers
- **No buffering controls exposed to callers**

## Usage Example

```go
import "github.com/Project-Sylos/Sylos-DB/pkg/store"

// Open store
s, err := store.Open("/path/to/database.db")
if err != nil {
    return err
}
defer s.Close()

// Register a node (automatically handles indexes, stats, etc.)
state := &bolt.NodeState{
    ID:              nodeID,
    Name:            "myfile.txt",
    Path:            "/path/to/myfile.txt",
    Type:            "file",
    TraversalStatus: bolt.StatusPending,
}
err = s.RegisterNode("SRC", level, bolt.StatusPending, state)

// Transition status (automatically updates buckets, stats, indexes)
err = s.TransitionNodeStatus("SRC", level, 
    bolt.StatusPending, bolt.StatusSuccessful, nodeID)

// Query with automatic read-your-writes
node, err := s.GetNode(nodeID)

// Coordination point
err = s.Barrier(store.BarrierRoundCompletion)
hasPending, err := s.HasPendingAtLevel("SRC", level)
```

## What Sylos-DB Is **Not**

Sylos-DB is **not**:
- A unified DB interface
- An ORM
- A query DSL
- A backend switcher
- A business-logic layer

BoltDB and DuckDB are **intentionally different tools** and should remain visible as such.

## Design Philosophy: Toolkit, Not Framework

Sylos-DB is a **toolkit**, not a facade:

- BoltDB → Fast, correct operations
- DuckDB → Powerful review & insight
- ETL → Explicit, controlled transitions
- API & Engine → Remain honest and readable

No fake abstractions. No hidden intent.

## Boundary Rule

When deciding where logic belongs:

> **If changing this code would change product behavior, it does *not* belong in Sylos-DB.**  
> **If changing this code would break correctness, consistency, or safety, it *does* belong in Sylos-DB.**

### Example: Exclusion Logic

**Belongs in Sylos-DB** (storage primitives):
- `AddHoldingEntry`
- `RemoveHoldingEntry`
- `CheckHoldingEntry`
- `ScanExclusionHoldingBucketByLevel`

**Does not belong** (business logic):
- "Scan, then enqueue tasks"
- "Scan, then advance round"
- "If holding bucket empty, do X"

## Database Schema

### BoltDB Structure

```
/Traversal-Data
  /SRC
    /nodes                  → ULID: NodeState JSON (canonical data)
    /children               → parentULID: []childULID JSON (tree relationships)
    /src-to-dst             → srcULID: dstULID (bidirectional join-lookup)
    /path-to-ulid           → pathHash: ULID (path-based lookup for API)
    /levels
      /00000000
        /pending            → ULID: empty (membership set)
        /successful         → ULID: empty
        /failed             → ULID: empty
        /status-lookup      → ULID: status string (reverse index)
      /00000001/...
  /DST
    /nodes                  → ULID: NodeState JSON
    /children               → parentULID: []childULID JSON
    /dst-to-src             → dstULID: srcULID
    /path-to-ulid           → pathHash: ULID
    /levels
      /00000000
        /pending
        /successful
        /failed
        /not_on_src         → ULID: empty (DST-specific)
        /status-lookup      → ULID: status string
      /00000001/...
  /STATS
    /totals                 → bucketPath: int64 (bucket count statistics)
    /queue-stats            → queueKey: QueueObserverMetrics JSON
```

**Logs** (separate island, not under Traversal-Data):
```
/LOGS
  /trace                   → uuid: LogEntry JSON
  /debug                   → uuid: LogEntry JSON
  /info                    → uuid: LogEntry JSON
  /warning                 → uuid: LogEntry JSON
  /error                   → uuid: LogEntry JSON
  /critical                → uuid: LogEntry JSON
```

## API Usage Model

The API is **explicit**, not abstracted:

- During operational phases → Talks directly to Bolt via Sylos-DB Store API
- During review phase → Queries DuckDB directly
- Uses Sylos-DB for:
  - DB lifecycle
  - ETL triggers
  - Batch writes
  - Buffer flushing (internal)

The API:
- Writes SQL queries directly when needed
- Does not route SQL through a fake abstraction
- Understands which DB it is talking to at all times

This avoids ORM-style opacity and preserves intent.

## ETL Semantics (Bolt ↔ DuckDB)

### Bolt → DuckDB
- Runs after traversal completes
- Materializes a **readable snapshot** of filesystem state
- Includes node IDs to allow reverse mapping
- Snapshot is versioned / phase-labeled

### DuckDB → Bolt
- Runs when user commits review changes
- Applies status/exclusion edits back into Bolt
- Bulk operations preferred
- Uses Sylos-DB invariant-safe write methods

### ETL Goals
- Predictable
- Explicit
- Version-aware
- Easy to validate (`COUNT(*)`, status checks)

### Important Constraint
- During review, DuckDB is the *active view*
- Bolt remains authoritative for execution
- No silent drift allowed

## Contributing

### Adding New Operations

1. **Start with domain verb**: `RegisterNode`, `MarkExcluded`, not `PutNode`, `UpdateBucket`
2. **Consolidate related operations**: One method = all related updates
3. **Track affected buckets**: For conflict detection
4. **Update stats automatically**: Don't require separate calls
5. **Document semantic barriers**: If operation needs explicit consistency checkpoint

### Testing

- Test correctness with multiple operations
- Verify read-your-writes consistency
- Test barrier behavior at coordination points
- Stress test with realistic workloads

### Migration Guide

When migrating Engine/API code from direct Bolt usage:

1. Replace `import "go.etcd.io/bbolt"` with `import "github.com/Project-Sylos/Sylos-DB/pkg/store"`
2. Replace `db *bolt.DB` with `store *store.Store`
3. Replace multi-step operations with single Store method calls
4. Replace explicit `Flush()` calls with semantic barriers
5. Remove manual stats update calls (now automatic)

## Documentation

This repository includes comprehensive documentation:

- **[`docs/README.md`](docs/README.md)** - Documentation index and table of contents
- **[`pkg/store/README.md`](pkg/store/README.md)** - Store API reference and usage guide
- **[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)** - Authoritative Store + Buffer semantics spec
- **[`docs/CONSISTENCY_MODELS.md`](docs/CONSISTENCY_MODELS.md)** - Domain-specific consistency models explained
- **[`docs/DOMAIN_SEMANTICS_IMPLEMENTATION.md`](docs/DOMAIN_SEMANTICS_IMPLEMENTATION.md)** - Implementation details

**Quick start**: Read this README for overview, then [`pkg/store/README.md`](pkg/store/README.md) for API usage.

**Deep dive**: See [`docs/README.md`](docs/README.md) for a complete documentation guide.

## License

LGPL-2.1-or-later

## Summary

Sylos-DB prioritizes:
- ✅ Correctness, consistency, and safety
- ✅ Explicit primitives over hidden abstractions
- ✅ Domain-focused verbs over generic CRUD
- ✅ Internal mechanics over exposed controls

Sylos-DB avoids:
- ❌ Fake abstractions
- ❌ Hidden intent
- ❌ Generic DB interfaces
- ❌ ORM-style opacity

This structure prioritizes **correctness, debuggability, and long-term sanity** over short-term convenience.

