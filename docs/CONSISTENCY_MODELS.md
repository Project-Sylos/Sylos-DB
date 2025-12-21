# Consistency Models in Sylos-DB

## Overview

Sylos-DB implements **domain-specific consistency models** to optimize performance while maintaining correctness where it matters.

## Two Consistency Models

### 1. Strict Consistency (Operational Data)

**Domains:**
- Nodes (node state, registration, updates)
- Status (pending, successful, failed, excluded)
- Stats (bucket counts, level progress)
- Exclusion (exclusion status, holding buckets)
- Lookups (path-to-ULID, SRC↔DST join mappings)

**Behavior:**
- Reads **always trigger flush** if conflicts detected
- Guarantees read-your-writes consistency
- Required for migration logic correctness

**Why Strict:**
These domains control the operational state machine of the migration. Any inconsistency could lead to:
- Duplicate processing
- Missed nodes
- Incorrect completion detection
- Data corruption

### 2. Eventually Consistent (Observability Data)

**Domains:**
- Logs (trace, debug, info, warning, error, critical)
- Queue Stats (queue observer metrics for UI/API)

**Behavior:**
- Reads **never trigger flush**
- Tolerate delays (buffer flushes every 2 seconds by default)
- Only flushed at semantic barriers for durability

**Why Eventually Consistent:**
These domains are used for observability and monitoring:
- UI polls logs every 200ms
- API polls queue stats every 200ms
- Slight delays (up to 2 seconds) are acceptable
- Flushing on every poll would destroy performance

## Performance Impact

### Without Domain-Specific Models (All Strict)

```
UI polls logs at 200ms interval
→ Triggers flush every 200ms
→ Blocks all writes during flush
→ Destroys throughput
```

### With Domain-Specific Models (Mixed)

```
UI polls logs at 200ms interval
→ Reads from DB without flush
→ Gets logs up to 2 seconds old
→ Zero impact on operational writes
→ Logs eventually consistent via background flush
```

## Barrier Semantics

Semantic barriers flush **all domains** (including eventually consistent ones):

```go
store.Barrier(store.BarrierPhaseTransition)
```

This ensures:
- All logs are persisted before phase change
- All queue stats are persisted before shutdown
- Durability at critical coordination points

## Example Usage

### Operational Read (Strict)

```go
// Check if there are pending tasks
hasPending, err := store.HasPendingAtLevel("SRC", level)
// ✅ Flushes buffer if conflicts detected
// ✅ Always sees latest writes
```

### Observability Read (Eventually Consistent)

```go
// Poll logs for UI (every 200ms)
logs, err := store.QueryLogs("error", 1000)
// ✅ Never flushes buffer
// ✅ May be up to 2 seconds stale
// ✅ Zero performance impact
```

### Barrier (Flushes Everything)

```go
// Before shutdown
err := store.Barrier(store.BarrierPhaseTransition)
// ✅ Flushes all domains (operational + observability)
// ✅ Guarantees durability
```

## Implementation Details

### Operational Data Reads

```go
func (s *Store) HasPendingAtLevel(queueType string, level int) (bool, error) {
    qr := QueueRound{QueueType: queueType, Level: level}
    
    // Check conflicts and flush if needed
    if err := s.checkDomainConflict(DomainDependency{
        Pending: []QueueRound{qr},
        Stats:   true,
    }); err != nil {
        return false, err
    }
    
    return s.db.HasStatusBucketItems(queueType, level, bolt.StatusPending)
}
```

### Observability Data Reads

```go
func (s *Store) QueryLogs(level string, limit int) ([]*bolt.LogEntry, error) {
    s.mu.RLock()
    defer s.mu.RUnlock()
    
    // Logs are eventually consistent - no conflict check
    // UI polling can tolerate slight delays
    
    if level != "" {
        return bolt.GetLogsByLevel(s.db, level)
    }
    return bolt.GetAllLogs(s.db)
}
```

## Design Rationale

This design follows the principle:

> **Flush only when correctness requires it.**

- Operational state machine: Correctness is critical → strict consistency
- Observability/monitoring: Freshness is nice-to-have → eventual consistency
- Coordination points: Durability is critical → barriers flush everything

This allows:
- High-frequency UI polling without performance degradation
- Strict correctness for migration logic
- Durability guarantees at semantic boundaries

## Future Considerations

If eventually consistent reads become problematic (e.g., users confused by stale logs), we can:

1. **Reduce buffer flush interval** (e.g., 500ms instead of 2s)
2. **Add explicit flush methods** (e.g., `FlushLogs()` for critical log queries)
3. **Implement read-through cache** (check buffer before reading DB)

But the current model is correct, simple, and performant.

