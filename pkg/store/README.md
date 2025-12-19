# Sylos-DB Store API

## Overview

The Store API is a domain-focused interface for migration state management. It consolidates multi-step database operations (node insertion, status transitions, index maintenance, stats updates) into single atomic methods, providing read-your-writes consistency and semantic barriers for coordination points.

**Key Philosophy**: Store owns mechanics, not meaning. It guarantees correctness and consistency while keeping storage strategy internal.

## Core Principles

1. **No Storage Primitives Exposed**: Never import `go.etcd.io/bbolt` in Migration-Engine
2. **Read-Your-Writes Consistency**: Reads always observe prior writes, even with internal buffering
3. **Automatic Conflict Detection**: Buffer flushes automatically before reads that depend on buffered state
4. **Semantic Barriers**: Declare intent at coordination points (round completion, phase transitions)
5. **Domain Verbs**: Methods reflect business operations, not database primitives

## Usage

### Opening a Store

```go
import "github.com/Project-Sylos/Sylos-DB/pkg/store"

s, err := store.Open("/path/to/database.db")
if err != nil {
    return err
}
defer s.Close()
```

### Node Lifecycle Operations

#### Register a Node

Atomically inserts node with all indexes and stats updates:

```go
state := &bolt.NodeState{
    ID:              nodeID,
    ServiceID:       "service-123",
    ParentID:        parentID,
    Name:            "myfile.txt",
    Path:            "/path/to/myfile.txt",
    Type:            "file",
    Depth:           5,
    TraversalStatus: bolt.StatusPending,
}

err := s.RegisterNode("SRC", level, bolt.StatusPending, state)
```

**What it does internally:**
- Writes to nodes bucket
- Adds to status bucket (pending/successful/failed)
- Updates status-lookup index
- Updates children index (if has parent)
- Updates stats (nodes count, status count)
- Handles SRC↔DST join mappings (if applicable)

#### Transition Node Status

```go
err := s.TransitionNodeStatus("SRC", level, bolt.StatusPending, bolt.StatusSuccessful, nodeID)
```

**What it does internally:**
- Updates node state
- Moves from old status bucket to new status bucket
- Updates status-lookup index
- Updates stats (decrement old, increment new)

#### Update Copy Status

```go
err := s.UpdateNodeCopyStatus(nodeID, bolt.CopyStatusSuccessful)
```

Only updates the node's copy status field (no status bucket changes).

#### Delete Node

```go
err := s.DeleteNode("SRC", nodeID)
```

Removes node and all associated indexes and stats.

### Query Operations

All queries automatically flush buffer if needed (read-your-writes).

#### Get Node by ID

```go
node, err := s.GetNode(nodeID)
```

#### Get Node by Path

```go
node, err := s.GetNodeByPath("SRC", "/path/to/file")
```

Uses path-to-ULID lookup internally.

#### Get Children

```go
childIDs, err := s.GetChildren("SRC", parentID)
```

#### List Pending Nodes

```go
nodes, err := s.ListPendingAtLevel("SRC", level, limit)
```

Set `limit` to 0 for no limit.

#### Check for Pending Nodes

```go
hasPending, err := s.HasPendingAtLevel("SRC", level)
```

Optimized O(1) check using stats bucket.

#### Get All Levels

```go
levels, err := s.GetAllLevels("SRC")
// Returns: [0, 1, 2, 3, ...] - all depth levels that exist
```

#### Get Max Depth

```go
maxDepth, err := s.GetMaxKnownDepth("SRC")
// Returns highest level number, or -1 if no levels exist
```

#### Lease Tasks for Workers

```go
tasks, err := s.LeaseTasksAtLevel("SRC", level, bolt.StatusPending, 100)
// Returns up to 100 tasks with their node states
for _, task := range tasks {
    nodeID := task.Key
    state := task.State
    // Process task...
}
```

### Semantic Barriers

Use barriers at coordination points instead of explicit flush calls.

```go
// Before round completion check
err := s.Barrier(store.BarrierRoundCompletion)
hasPending, err := s.HasPendingAtLevel("SRC", currentLevel)
if !hasPending {
    // Round is complete
}

// Before advancing to next round
err := s.Barrier(store.BarrierRoundAdvance)
currentRound++

// Between major phases
err := s.Barrier(store.BarrierPhaseTransition)
```

**Available Barriers:**
- `BarrierRoundCompletion`: Before checking if round is complete
- `BarrierRoundAdvance`: Before advancing to next round
- `BarrierPhaseTransition`: Between traversal/copy/exclusion phases
- `BarrierCompletionCheck`: Before final "done" determination
- `BarrierModeSwitch`: Switching between parallel/serial modes

**What barriers do:**
- Flush all buffered writes
- Ensure visibility and consistency
- Optional fsync at critical barriers (phase transitions)

### Exclusion Operations

```go
// Mark node as excluded
err := s.MarkExcluded("SRC", nodeID, inherited)

// Perform exclusion sweep at level
err := s.SweepInheritedExclusions("SRC", level)

// Check exclusion status
excluded, inherited, err := s.CheckExclusionStatus("SRC", nodeID)

// Scan exclusion holding bucket
entries, hasMore, err := s.ScanExclusionHoldingAtLevel("SRC", level, limit)
for _, entry := range entries {
    nodeID := entry.NodeID
    depth := entry.Depth
    mode := entry.Mode // "exclude" or "unexclude"
}

// Check if node is in holding
exists, mode, err := s.CheckHoldingEntry("SRC", nodeID)

// Add/remove holding entries
err := s.AddHoldingEntry("SRC", nodeID, depth, "exclude")
err := s.RemoveHoldingEntry("SRC", nodeID, "exclude")
```

### Stats Queries

```go
// Get level progress
pending, completed, failed, err := s.GetLevelProgress("SRC", level)

// Get total queue depth
totalPending, err := s.GetQueueDepth("SRC")

// Count specific status at level
count, err := s.CountStatusAtLevel("SRC", level, bolt.StatusNotOnSrc)

// Count total nodes in queue
totalNodes, err := s.CountNodes("SRC")
```

### Logging

```go
// Record log
err := s.RecordLog("info", "worker", workerID, "Task completed successfully")

// Query logs
logs, err := s.QueryLogs("error", 100) // last 100 error logs
```

## Migration Guide: From Direct Bolt to Store API

### Before (Direct bolt usage)

```go
// ❌ Old way - scattered operations
err := db.Update(func(tx *bolt.Tx) error {
    // Insert node
    nodesBucket := getBucket(tx, GetNodesBucketPath("SRC"))
    nodeData, _ := state.Serialize()
    nodesBucket.Put(nodeID, nodeData)
    
    // Add to status bucket
    statusBucket, _ := GetOrCreateStatusBucket(tx, "SRC", level, "pending")
    statusBucket.Put(nodeID, []byte{})
    
    // Update status-lookup
    UpdateStatusLookup(tx, "SRC", level, nodeID, "pending")
    
    // Update children index
    // ... more manual operations
    
    return nil
})

// Manually flush buffer
if opts.FlushBuffer {
    outputBuffer.Flush()
}

// Check for pending
hasPending := db.HasStatusBucketItems("SRC", level, "pending")
```

### After (Store API)

```go
// ✅ New way - single operation
err := s.RegisterNode("SRC", level, bolt.StatusPending, state)

// Declare intent at coordination point
s.Barrier(store.BarrierRoundCompletion)

// Check for pending (automatic read-your-writes)
hasPending, err := s.HasPendingAtLevel("SRC", level)
```

## Key Differences

| Aspect | Direct Bolt | Store API |
|--------|-------------|-----------|
| **Imports** | `go.etcd.io/bbolt` | `github.com/Project-Sylos/Sylos-DB/pkg/store` |
| **Operations** | Manual multi-step | Single method call |
| **Stats Updates** | Manual separate calls | Automatic |
| **Buffering** | Explicit `Flush()` calls | Automatic + barriers |
| **Consistency** | Manual coordination | Automatic read-your-writes |
| **Error-Prone** | Easy to forget steps | Consolidated invariants |

## What Store Does NOT Do

- ❌ Decide when/why operations happen (business logic)
- ❌ Control workflow orchestration
- ❌ Expose transactions, buckets, or buffer controls
- ❌ Make decisions about filtering, searching, or UI logic

**Store provides:** Correct, consistent state transitions
**Engine provides:** When and why those transitions happen

## Performance Characteristics

- **Buffering**: Writes are buffered internally (default: 1000 entries or 2 seconds)
- **Auto-Flush**: Triggered by read conflicts, size threshold, or time threshold
- **Barriers**: Explicit consistency checkpoints at coordination points
- **O(1) Stats**: Status checks use pre-computed stats bucket (fast)

## Complete API Reference

### Node Operations
- `RegisterNode(queueType, level, status, state) error`
- `TransitionNodeStatus(queueType, level, oldStatus, newStatus, nodeID) error`
- `UpdateNodeCopyStatus(nodeID, newCopyStatus) error`
- `DeleteNode(queueType, nodeID) error`

### Queries
- `GetNode(nodeID) (*bolt.NodeState, error)`
- `GetNodeByPath(queueType, path) (*bolt.NodeState, error)`
- `GetChildren(queueType, parentID) ([]string, error)`
- `ListPendingAtLevel(queueType, level, limit) ([]*bolt.NodeState, error)`
- `HasPendingAtLevel(queueType, level) (bool, error)`
- `GetAllLevels(queueType) ([]int, error)` - Get all depth levels that exist
- `GetMaxKnownDepth(queueType) (int, error)` - Get highest level number
- `LeaseTasksAtLevel(queueType, level, status, limit) ([]LeaseResult, error)` - Lease tasks for workers

### Exclusions
- `MarkExcluded(queueType, nodeID, inherited) error`
- `SweepInheritedExclusions(queueType, level) error`
- `CheckExclusionStatus(queueType, nodeID) (excluded, inherited bool, err error)`
- `ScanExclusionHoldingAtLevel(queueType, level, limit) ([]ExclusionEntry, bool, error)`
- `CheckHoldingEntry(queueType, nodeID) (exists, mode string, err error)`
- `AddHoldingEntry(queueType, nodeID, depth, mode) error`
- `RemoveHoldingEntry(queueType, nodeID, mode) error`

### Stats
- `GetLevelProgress(queueType, level) (pending, completed, failed int, err error)`
- `GetQueueDepth(queueType) (int, error)`
- `CountStatusAtLevel(queueType, level, status) (int, error)` - Count nodes at level with specific status
- `CountNodes(queueType) (int, error)` - Count total nodes in queue

### Coordination
- `Barrier(reason BarrierReason) error`

### Logging
- `RecordLog(level, entity, entityID, message) error`
- `QueryLogs(level, limit) ([]*bolt.LogEntry, error)`

### Lifecycle
- `Open(dbPath) (*Store, error)`
- `Close() error`

## Next Steps for Migration-Engine

1. Replace `import "go.etcd.io/bbolt"` with `import "github.com/Project-Sylos/Sylos-DB/pkg/store"`
2. Replace `db *bolt.DB` with `store *store.Store`
3. Replace direct CRUD operations with Store methods
4. Replace explicit `outputBuffer.Flush()` calls with semantic barriers
5. Remove manual stats update calls (now automatic)
6. Test with same workloads to verify correctness

## Debugging Tips

- **Read returns stale data**: Missing barrier before critical read
- **Stats incorrect**: Missing barrier at coordination point
- **Performance regression**: Too many barriers (use only at coordination points)
- **Deadlock**: Nested store operations (avoid calling Store from within Store methods)

## Questions?

This API is designed for the specific needs of the Migration-Engine's traversal and copy phases. If something feels awkward or requires workarounds, that's a signal the API needs adjustment.

