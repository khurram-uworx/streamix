# Streamix Concurrency Control: Explicit Semantics for Ordered/Unordered Operations

## Overview

Currently, Streamix provides concurrency control through scattered operators with inconsistent naming:
- `ParallelMap` / `ParallelMapOrdered` (1→1 transforms)
- `FlatMap` / `FlatMapMany` (1→N transforms)

The **ordering semantics are implicit and naming is inconsistent**. Users can't easily discover or understand whether results are:

- **Unordered** (fastest, results emit as soon as they complete)
- **Sequential** (ordered, but single-threaded with no concurrency)
- **Ordered concurrent** (concurrent execution with order-preserving output)

This ambiguity creates a **production concern**: developers must guess which semantic applies, potentially missing performance optimizations or ending up with unexpected result ordering.

## Proposal: Unified Concurrency API (Option C)

We will introduce a **unified, symmetric API** that makes the concurrency contract clear and discoverable:

```csharp
// Single-value transforms (1→1)
stream.Map(selector)                                    // unordered, unbounded
stream.MapOrdered(selector, maxConcurrency: 10)       // ordered, configurable concurrency

// Multi-value transforms (1→N flattening)
stream.FlatMap(selector)                               // unordered, unbounded
stream.FlatMapOrdered(selector, maxConcurrency: 10)   // ordered, configurable concurrency

// Sequential (single-threaded, always ordered)
stream.ConcatMap(selector)                            // sequential, strict order
```

### Semantic Comparison

| Operator | Concurrency | Ordering | Use Case | Performance |
|----------|-------------|----------|----------|-------------|
| `Map()` | Unbounded | Unordered | Fire-and-forget, fastest transformation | ⭐⭐⭐ |
| `MapOrdered()` | Configurable N | Ordered (reordered) | Transform with order preservation | ⭐⭐ |
| `FlatMap()` | Unbounded | Unordered | Fire-and-forget, fastest pipeline | ⭐⭐⭐ |
| `FlatMapOrdered()` | Configurable N | Ordered (reordered) | Flatten with order preservation | ⭐⭐ |
| `ConcatMap()` | 1 | Ordered (sequential) | Strict ordering, side effects that need order | ⭐ |

### Breaking Changes (0.x Release Cycle)

Since we're in a 0.x release cycle, we'll do a clean break:
- **Remove** `ParallelMap()` and `ParallelMapOrdered()` — replace with `Map()` and `MapOrdered()`
- **Remove** `FlatMapMany()` and `FlatMapManyAwait()` — replace with `FlatMap()`, `ConcatMap()`, and `FlatMapOrdered()`
- **No deprecation warnings** — clean API, no legacy baggage
- Update all internal usage to use new names

### Design Principles

1. **Symmetric naming** - `Map` ↔ `MapOrdered` and `FlatMap` ↔ `FlatMapOrdered` for easy discovery
2. **Unordered by default** - Methods without "Ordered" suffix are fastest (unbounded concurrency)
3. **Configurable concurrency** - Ordered variants accept `maxConcurrency` parameter for tuning
4. **Consistent with industry standards** - Aligns with RxJS, Rx.NET, and Project Reactor semantics

## Implementation Strategy

**Clean break, no deprecation** — We're in 0.x, so we'll remove old names entirely and implement fresh.

1. **Phase 1**: Remove old names (`ParallelMap`, `ParallelMapOrdered`, `FlatMapMany`, `FlatMapManyAwait`) from IStream
2. **Phase 2**: Add new signatures (`Map`, `MapOrdered`, `FlatMap`, `ConcatMap`, `FlatMapOrdered`)
3. **Phase 3**: Implement all five operators in Stream.cs and ConnectableStream.cs
4. **Phase 4**: Update all internal code and tests to use new names
5. **Phase 5**: Write comprehensive concurrency tests
6. **Phase 6**: Update README and documentation

## Files Affected

- `src/Streamix/IStream.cs` - Interface definitions
- `src/Streamix/Implementations/Stream.cs` - Implementation
- `src/Streamix/Implementations/ConnectableStream.cs` - Core logic for concurrent operations
- `src/Streamix.Tests/ConcurrencyTests.cs` - Concurrency validation tests
- `README.md` - Update examples and operator reference
- `docs/CONCURRENCY.md` - This file (design rationale)

---
