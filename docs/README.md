# Sylos-DB Documentation

This directory contains detailed documentation for the Sylos-DB repository. This README serves as a table of contents and guide to help you find the information you need.

## 📚 Documentation Index

### Core Architecture & Design

#### [ARCHITECTURE.md](./ARCHITECTURE.md)
**Authoritative specification of Store + Buffer semantics**

Essential reading for understanding the core design principles and implementation details of Sylos-DB. This document defines:
- Domain-based buffer tracking (O(1) metadata)
- Store API shape and domain verbs
- Read-before-flush logic and conflict detection
- Semantic barriers for coordination points
- Domain slice reference and dependency matrix

**Read this if you want to:**
- Understand how the Store API works internally
- Learn about domain-based consistency guarantees
- See the authoritative spec for buffer semantics
- Understand the contract between Migration-Engine and Sylos-DB

---

#### [DOMAIN_SEMANTICS_IMPLEMENTATION.md](./DOMAIN_SEMANTICS_IMPLEMENTATION.md)
**Implementation summary of domain semantics**

Details on how the domain semantics from ARCHITECTURE.md are implemented in code. Includes:
- What changed from the previous implementation
- Key implementation details (domain slice tracking, write/read declarations)
- Files modified
- Correctness guarantees
- Migration-Engine integration notes

**Read this if you want to:**
- Understand the implementation details
- See what changed and why
- Learn how to integrate with the Store API
- Review correctness guarantees

---

#### [CONSISTENCY_MODELS.md](./CONSISTENCY_MODELS.md)
**Domain-specific consistency models explained**

Explains the two consistency models used in Sylos-DB:
- **Strict Consistency**: Operational data (nodes, status, stats, exclusion, lookups)
- **Eventually Consistent**: Observability data (logs, queue stats)

Includes:
- Why different domains use different models
- Performance impact analysis
- Barrier semantics
- Example usage patterns

**Read this if you want to:**
- Understand why logs don't trigger flushes
- Learn about the performance optimizations
- See examples of strict vs eventually consistent reads
- Understand barrier behavior

---

### Package Documentation

#### [`../pkg/store/README.md`](../pkg/store/README.md)
**Store API reference and usage guide**

Complete API reference for the `pkg/store` package, including:
- All public methods with signatures
- Usage examples
- Migration guide from raw BoltDB usage
- Design principles and guarantees

**Read this if you want to:**
- Use the Store API in your code
- Migrate from direct BoltDB access
- See code examples
- Understand API method semantics

---

### Repository Documentation

#### [`../README.md`](../README.md)
**Repository overview and high-level introduction**

High-level overview of the repository, including:
- Purpose and design philosophy
- Package structure
- Quick start guide
- Links to detailed documentation

**Read this if you want to:**
- Get a high-level understanding of Sylos-DB
- Learn the design philosophy
- See the package structure
- Find links to detailed docs

---

## 🎯 Quick Navigation by Task

### I want to...

**...use the Store API in my code**
→ Start with [`../pkg/store/README.md`](../pkg/store/README.md) for API reference and examples

**...understand how buffering and flushing works**
→ Read [ARCHITECTURE.md](./ARCHITECTURE.md) (sections on buffer design and read-before-flush logic)

**...understand why logs don't flush on read**
→ Read [CONSISTENCY_MODELS.md](./CONSISTENCY_MODELS.md) (eventually consistent domains)

**...see what changed in the implementation**
→ Read [DOMAIN_SEMANTICS_IMPLEMENTATION.md](./DOMAIN_SEMANTICS_IMPLEMENTATION.md)

**...integrate with Migration-Engine**
→ Read [`../pkg/store/README.md`](../pkg/store/README.md) (migration guide section) and [ARCHITECTURE.md](./ARCHITECTURE.md) (engine responsibilities)

**...understand the overall architecture**
→ Read [`../README.md`](../README.md) for overview, then [ARCHITECTURE.md](./ARCHITECTURE.md) for details

**...contribute or modify the codebase**
→ Read [ARCHITECTURE.md](./ARCHITECTURE.md) (implementation guidance) and [`../pkg/store/README.md`](../pkg/store/README.md) (API design principles)

---

## 📖 Reading Order Recommendations

### For Users (Migration-Engine developers)

1. [`../README.md`](../README.md) - Get oriented
2. [`../pkg/store/README.md`](../pkg/store/README.md) - Learn the API
3. [CONSISTENCY_MODELS.md](./CONSISTENCY_MODELS.md) - Understand performance characteristics (optional)

### For Maintainers (Sylos-DB developers)

1. [`../README.md`](../README.md) - Get oriented
2. [ARCHITECTURE.md](./ARCHITECTURE.md) - Understand the spec
3. [DOMAIN_SEMANTICS_IMPLEMENTATION.md](./DOMAIN_SEMANTICS_IMPLEMENTATION.md) - See the implementation
4. [CONSISTENCY_MODELS.md](./CONSISTENCY_MODELS.md) - Understand the consistency models

### For Contributors (New developers)

1. [`../README.md`](../README.md) - Get oriented
2. [`../pkg/store/README.md`](../pkg/store/README.md) - Understand the public API
3. [ARCHITECTURE.md](./ARCHITECTURE.md) - Understand the internal design
4. [CONSISTENCY_MODELS.md](./CONSISTENCY_MODELS.md) - Understand performance optimizations

---

## 🔍 Document Descriptions

### ARCHITECTURE.md
- **Type**: Authoritative Specification
- **Audience**: Maintainers, Contributors
- **Purpose**: Define the contract and implementation details
- **Status**: Authoritative (do not violate)

### DOMAIN_SEMANTICS_IMPLEMENTATION.md
- **Type**: Implementation Summary
- **Audience**: Maintainers, Contributors
- **Purpose**: Document what changed and how it's implemented
- **Status**: Historical reference, implementation notes

### CONSISTENCY_MODELS.md
- **Type**: Design Explanation
- **Audience**: All developers
- **Purpose**: Explain performance optimizations and consistency trade-offs
- **Status**: Reference documentation

### pkg/store/README.md
- **Type**: API Reference & Usage Guide
- **Audience**: Users, API consumers
- **Purpose**: Help developers use the Store API
- **Status**: User-facing documentation

### README.md (root)
- **Type**: Repository Overview
- **Audience**: Everyone
- **Purpose**: High-level introduction and navigation
- **Status**: Entry point

---

## 📝 Document Maintenance

When adding new documentation:

1. **Architecture/Spec changes** → Update `ARCHITECTURE.md`
2. **Implementation details** → Update `DOMAIN_SEMANTICS_IMPLEMENTATION.md` or create new doc
3. **API changes** → Update `pkg/store/README.md`
4. **New patterns/designs** → Create new doc in `docs/` and add to this manifest
5. **User-facing changes** → Update root `README.md`

When updating this manifest:

- Add new documents to the appropriate section
- Update the "Quick Navigation by Task" section if needed
- Keep the reading order recommendations up to date

---

## 🆘 Need Help?

- **API questions** → See [`../pkg/store/README.md`](../pkg/store/README.md)
- **Design questions** → See [ARCHITECTURE.md](./ARCHITECTURE.md)
- **Performance questions** → See [CONSISTENCY_MODELS.md](./CONSISTENCY_MODELS.md)
- **Implementation questions** → See [DOMAIN_SEMANTICS_IMPLEMENTATION.md](./DOMAIN_SEMANTICS_IMPLEMENTATION.md)

