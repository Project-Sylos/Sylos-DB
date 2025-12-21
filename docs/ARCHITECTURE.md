# Sylos-DB Store + Buffer Semantics (Authoritative Spec)

## Purpose

Sylos-DB provides the **authoritative persistence layer and state machine** for Sylos.
Migration-Engine **must not** reason about database mechanics, buffering, or flushing.

Migration-Engine describes **domain intent** (queue, round, status, mode).
Sylos-DB guarantees **correctness, visibility, and durability**.

---

## Core Principles

1. **Domain verbs only**

   * No raw CRUD exposed
   * No bucket access
   * No DB handles exposed

2. **Buffered writes are internal**

   * Callers never request buffering
   * Callers never request flushing

3. **Reads must be correct**

   * Any read must observe all causally relevant writes
   * Store decides whether a flush is required

4. **No buffer scanning**

   * Buffer maintains O(1) metadata describing *what domains are dirty*
   * Reads consult metadata, not buffered ops

---

## Store API Shape (Public, Stable)

The Store API exposes **domain-level operations only**.

### Example Read APIs

```go
FetchPendingTraversalTasks(queueType QueueType, round int, limit int) ([]Task, error)

HasPendingTasks(queueType QueueType, round int) (bool, error)

GetNodeState(queueType QueueType, nodeID string) (*NodeState, error)
```

### Example Write APIs

```go
RegisterChildren(parent NodeID, children []NodeSpec) error

TransitionNodeStatus(queueType QueueType, round int, nodeID string, from Status, to Status) error

RecordTaskFailure(queueType QueueType, round int, nodeID string, reason string) error
```

### Semantic Barriers (NOT flush)

```go
Barrier(reason BarrierReason) error
```

Examples:

* `BarrierRoundAdvance`
* `BarrierCompletionCheck`
* `BarrierPhaseTransition`

> Barriers declare **semantic boundaries**, not persistence mechanics.

---

## Internal Buffer Design (Sylos-DB only)

### Buffer Tracks *Dirty Domains*, Not Ops

The buffer maintains **O(1) metadata** describing which *domain slices* are affected by unflushed writes.

Example internal structure:

```go
type DirtyDomains struct {
    pendingByRound map[QueueRound]bool
    statusByRound  map[QueueRound]bool
    nodesByRound   map[QueueRound]bool
    lookups        bool
    stats          bool
    logs           bool
    exclusion      bool
}
```

### When Writes Are Buffered

Every buffered write **declares its domain impact** immediately.

Example:

```go
buffer.Add(StatusUpdate{
    QueueType: SRC,
    Round: 5,
})
```

Marks internally:

```
pendingByRound[(SRC,5)] = true
statusByRound[(SRC,5)]  = true
nodesByRound[(SRC,5)]   = true
stats                   = true
```

⚠️ No scanning, no heuristics, no key inspection.

---

## Read-Before-Flush Logic (Critical)

Every Store **read method** defines the domain slices it depends on.

### Example: Fetch traversal tasks

```go
FetchPendingTraversalTasks(queueType, round)
```

Internally depends on:

```
PendingIndex(queueType, round)
```

Store logic:

```go
if buffer.AffectsPending(queueType, round) {
    buffer.Flush()
}
return boltRead(...)
```

### Example: Completion checks

Depends on:

* pendingByRound
* stats

Store logic:

```go
if buffer.AffectsAny(
    PendingIndex(queueType, round),
    Stats(queueType),
) {
    buffer.Flush()
}
```

✔ Flush happens **only if required**
✔ Flush happens **once per read**
✔ Engine never participates

---

## Barriers (Flush by Meaning, Not Command)

Barriers exist for **semantic safety points**, not as a persistence API.

```go
store.Barrier(BarrierRoundAdvance)
```

Internally:

* Flushes buffer
* Optionally fsync / checkpoint
* Clears dirty metadata

Engine never calls `Flush()`.

---

## Engine Responsibilities (Explicitly Allowed)

Migration-Engine **may**:

* Track current round
* Track queue mode
* Ask for tasks at a specific level
* Ask domain questions ("are there pending tasks at round N?")

Migration-Engine **must not**:

* Check buffer state
* Request flushes
* Reason about persistence timing
* Know whether buffering exists

---

## Explicit Non-Goals

Sylos-DB is **not**:

* A generic DB SDK
* An ORM
* A pluggable backend framework
* MVCC / snapshot isolation

This is **domain-specific correctness enforcement**, nothing more.

---

## Invariant Guarantee

> **If a Store read returns data, it is guaranteed to reflect all logically prior writes that could affect it.**

This invariant is non-negotiable.

---

## Implementation Guidance for Cursor

* Implement Store APIs first
* Preserve existing Bolt schema internally
* Move existing DB logic behind Store calls
* Centralize flush logic inside Store
* Do not expose buffer or DB primitives
* Favor correctness over minimal flushing
* Over-flush is acceptable; under-flush is a bug

---

## Final Contract Sentence (Do Not Violate)

**Migration-Engine describes *what slice of state it wants*.
Sylos-DB decides *how to make that slice correct*.**

---

## Domain Slice Reference

### Defined Domain Slices

| Domain | Key | Affected By | Consistency Model |
|--------|-----|-------------|-------------------|
| **Pending Index** | `(queueType, level)` | Node status transitions to/from pending | **Strict** (read-your-writes) |
| **Status Index** | `(queueType, level, status)` | Any status transition at level | **Strict** (read-your-writes) |
| **Nodes** | `(queueType, level)` | Node registration, updates, deletes | **Strict** (read-your-writes) |
| **Lookups** | `(queueType)` | Path-to-ULID, SRC↔DST join mappings | **Strict** (read-your-writes) |
| **Stats** | `(queueType)` | Any write that updates bucket counts | **Strict** (read-your-writes) |
| **Logs** | `(logLevel)` | Log writes | **Eventually consistent** (UI polling) |
| **Exclusion** | `(queueType, level)` | Exclusion status changes, holding bucket ops | **Strict** (read-your-writes) |
| **Queue Stats** | `(all)` | Queue observer metrics | **Eventually consistent** (API polling) |

### Domain Dependency Matrix

| Read Operation | Depends On | Flushes Buffer? |
|----------------|------------|-----------------|
| `GetNode` | Nodes(queueType, level) | **Yes** (strict) |
| `GetNodeByPath` | Lookups(queueType), Nodes(queueType, level) | **Yes** (strict) |
| `GetChildren` | Nodes(queueType, level), Lookups(queueType) | **Yes** (strict) |
| `ListPendingAtLevel` | Pending(queueType, level), Nodes(queueType, level) | **Yes** (strict) |
| `HasPendingAtLevel` | Pending(queueType, level), Stats(queueType) | **Yes** (strict) |
| `GetLevelProgress` | Stats(queueType) | **Yes** (strict) |
| `GetQueueDepth` | Stats(queueType) | **Yes** (strict) |
| `CheckExclusionStatus` | Exclusion(queueType, level) | **Yes** (strict) |
| `QueryLogs` | Logs(level) | **No** (eventually consistent) |
| `GetQueueStats` | QueueStats | **No** (eventually consistent) |

---

## Correctness Over Performance

When in doubt:

* Flush more, not less
* Declare wider domain impact, not narrower
* Check more domain slices, not fewer

Performance optimization comes after correctness is proven.

### Domain-Specific Consistency Models

Not all domains require strict read-your-writes consistency:

**Strict Consistency (Operational Data)**
- Nodes, Status, Pending, Stats, Exclusion, Lookups
- Reads **always flush** if conflicts detected
- Required for correctness of migration logic

**Eventually Consistent (Observability Data)**
- Logs, QueueStats
- Reads **never flush** (tolerate delays)
- Used for UI/API polling (200ms intervals)
- Flushed at barriers for durability only

This distinction allows high-frequency polling without performance impact while maintaining strict correctness for operational state.

