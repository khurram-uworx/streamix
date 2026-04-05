# Streamix Boundary Variety Release Tasks

## Purpose

This document turns the release-targeted slice of `docs/VARIETY.md` into concrete, assignable tasks for coding agents.

This backlog is intentionally narrow. It focuses only on the work needed to make the next Streamix release boundary-complete enough for:

- terminal variety at the system edge
- the first reusable sink abstraction
- truthful documentation for the shipped contract

## Suggested Execution Order

1. Task 1: Harden current terminal semantics and tests
2. Task 2: Add missing high-value terminals
3. Task 3: Add comparer-aware extremum and keyed materializer polish
4. Task 4: Introduce the core sink abstraction
5. Task 5: Rework channel output onto the sink path
6. Task 6: Add the delegate sink adapter
7. Task 7: Update README and examples for the shipped variety surface

## Coordination Notes

- Task 1 should happen first because it locks down current semantics before more API is added.
- Task 4 is the main API-shape task for the release. Tasks 5 and 6 depend on it.
- Task 2 and Task 3 can proceed in parallel once Task 1 has clarified the current semantics.
- Task 7 should wait until Tasks 2 through 6 settle the release surface.
- Shared files likely to create merge conflicts:
  - `src/Streamix/Extensions/TerminalExtensions.cs`
  - `src/Streamix/IStream.cs`
  - `src/Streamix/Implementations/Stream.cs`
  - `src/Streamix/Implementations/ConnectableStream.cs`
  - `README.md`

## ✅ Task 1: Harden Current Terminal Semantics And Tests

### Priority

High

### Goal

Lock down the current boundary behavior before adding the remaining release APIs.

### Why this exists

Several planned terminals are already implemented, but their test coverage is lighter than the boundary-quality bar described in `docs/VARIETY.md`.

### Scope

- Review current tests for:
  - `ToDictionaryAsync(...)`
  - `ElementAtAsync(...)`
  - `ElementAtOrDefaultAsync(...)`
  - `MinByAsync(...)`
  - `MaxByAsync(...)`
  - `DrainAsync(...)`
  - `ToChannel(...)`
- Add missing tests for:
  - empty-stream behavior where relevant
  - cancellation during enumeration
  - upstream exception propagation
  - sink/action exception propagation where relevant
- Keep the existing duplicate-key throwing behavior for `ToDictionaryAsync(...)`

### Constraints

- Do not change already-shipped semantics unless a bug is clearly identified.
- Treat `docs/VARIETY.md` as the target semantic direction.

### Acceptance criteria

- The currently shipped variety-related terminals have explicit tests for success, cancellation, and upstream failure where relevant.
- `ToDictionaryAsync(...)` duplicate-key behavior is clearly locked down by tests.
- `ToChannel(...)` tests still cover completion ownership and backpressure.

### Files likely involved

- `src/Streamix.Tests/TerminalExtensionsTests.cs`
- `src/Streamix.Tests/StreamTests.cs`
- `src/Streamix/Extensions/TerminalExtensions.cs`

## ✅ Task 2: Add Missing High-Value Terminals

### Priority

High

### Goal

Complete the core release slice by adding the two planned terminals that are still missing: membership and grouped materialization.

### Why this exists

The release target in `docs/VARIETY.md` is still incomplete without `ContainsAsync(...)` and `ToLookupAsync(...)`.

### Scope

- Add `ContainsAsync(T value, CancellationToken cancellationToken = default)`
- Add `ContainsAsync(T value, IEqualityComparer<T>? comparer, CancellationToken cancellationToken = default)`
- Add `ToLookupAsync<TKey>(Func<T, TKey> keySelector, CancellationToken cancellationToken = default)`
- Add `ToLookupAsync<TKey>(Func<T, TKey> keySelector, IEqualityComparer<TKey>? comparer, CancellationToken cancellationToken = default)`
- Add `ToLookupAsync<TKey, TValue>(Func<T, TKey> keySelector, Func<T, TValue> valueSelector, CancellationToken cancellationToken = default)`
- Add `ToLookupAsync<TKey, TValue>(Func<T, TKey> keySelector, Func<T, TValue> valueSelector, IEqualityComparer<TKey>? comparer, CancellationToken cancellationToken = default)`
- Add focused tests for success, empty input, cancellation, and upstream exception behavior

### Constraints

- Follow .NET-idiomatic semantics for `Contains` and `ToLookup`.
- Keep `ToLookupAsync(...)` as a materializer, not a sink API.

### Acceptance criteria

- Callers can perform a short-circuit membership check with `ContainsAsync(...)`.
- Callers can materialize grouped results with `ToLookupAsync(...)`.
- Comparer overloads behave as expected for case-insensitive or custom key scenarios.

### Files likely involved

- `src/Streamix/Extensions/TerminalExtensions.cs`
- `src/Streamix.Tests/TerminalExtensionsTests.cs`
- `README.md`

## ✅ Task 3: Add Comparer-Aware Boundary Polish

### Priority

Medium

### Goal

Close the highest-value API-shape gaps in the already-implemented terminal set.

### Why this exists

The current repo already has `MinByAsync(...)` and `MaxByAsync(...)`, but not the comparer-aware overloads described in `docs/VARIETY.md`.

### Scope

- Add `MinByAsync<TKey>(..., IComparer<TKey>? comparer, ...)`
- Add `MaxByAsync<TKey>(..., IComparer<TKey>? comparer, ...)`
- Ensure the new `ToLookupAsync(...)` and existing dictionary/hash-set APIs present a coherent comparer story
- Add focused tests for custom comparer behavior and empty-stream behavior

### Constraints

- Preserve the current simple overloads for convenience.
- Do not introduce a large overload matrix beyond the release need.

### Acceptance criteria

- `MinByAsync(...)` and `MaxByAsync(...)` support custom ordering through `IComparer<TKey>?`.
- The keyed terminal family has a consistent comparer-aware shape.

### Files likely involved

- `src/Streamix/Extensions/TerminalExtensions.cs`
- `src/Streamix.Tests/TerminalExtensionsTests.cs`
- `README.md`

## ✅ Task 4: Introduce The Core Sink Abstraction

### Priority

High

### Goal

Create the reusable sink contract that the rest of the boundary variety plan depends on.

### Why this exists

`docs/VARIETY.md` explicitly recommends pivoting away from one-off sink APIs and toward a small reusable abstraction.

### Scope

- Add `IAsyncSink<in T>`
- Add `SinkCompletionMode`
- Add `ToSinkAsync(...)` as the general sink terminal
- Define explicit success, error, cancellation, and completion-ownership semantics
- Add tests for:
  - successful copying
  - upstream failure
  - sink write failure
  - completion behavior
  - leave-open behavior

### Decision required

Confirm the exact core completion contract:

- whether upstream cancellation should call `CompleteAsync(...)` or just stop writing
- whether sink completion on upstream error should pass the original exception through `CompleteAsync(error)`

### Constraints

- Keep the abstraction minimal.
- Do not add ecosystem-specific sink types in this task.

### Acceptance criteria

- The repo has a reusable sink interface and one general sink terminal.
- Sink completion ownership is explicit and test-covered.
- The API is small enough to support future adapters without multiplying stream methods.

### Files likely involved

- `src/Streamix/IStream.cs`
- `src/Streamix/Extensions/TerminalExtensions.cs`
- `src/Streamix/Implementations/Stream.cs`
- `src/Streamix/Implementations/ConnectableStream.cs`
- `src/Streamix.Tests/TerminalExtensionsTests.cs`

## ✅ Task 5: Rework Channel Output Onto The Shared Sink Path

### Priority

High

### Goal

Make channel output an adapter over the new sink abstraction instead of a special-case copy path.

### Why this exists

Channels are the current sink baseline. The release is not complete until that behavior is expressed through the reusable sink path.

### Scope

- Implement a `ChannelWriter<T>` sink adapter
- Rework `ToChannel(ChannelWriter<T> writer, ...)` to use `ToSinkAsync(...)` internally
- Keep `ToChannel(int? capacity = null, ...)` working as today
- Preserve current success, backpressure, error propagation, and leave-open behavior

### Constraints

- Do not regress the current channel semantics already covered in tests.
- Keep the public `ToChannel(...)` API for compatibility.

### Acceptance criteria

- Existing channel tests still pass with the sink-backed implementation.
- `ToChannel(...)` becomes an adapter over shared sink logic rather than its own special-case pipeline.

### Files likely involved

- `src/Streamix/IStream.cs`
- `src/Streamix/Extensions/TerminalExtensions.cs`
- `src/Streamix/Implementations/Stream.cs`
- `src/Streamix/Implementations/ConnectableStream.cs`
- `src/Streamix.Tests/StreamTests.cs`
- `src/Streamix.Tests/TerminalExtensionsTests.cs`

## ✅ Task 6: Add The Delegate Sink Adapter

### Priority

High

### Goal

Provide the smallest useful sink adapter for app-specific boundaries without requiring custom sink classes.

### Why this exists

`docs/VARIETY.md` explicitly calls out a delegate-based sink as the first practical adapter after the core sink abstraction.

### Scope

- Add an adapter over `Func<T, CancellationToken, ValueTask>`
- Decide whether the adapter also accepts optional completion/error callbacks or whether that remains a custom sink concern for now
- Add tests for:
  - successful writes
  - awaited backpressure-style writes
  - callback exceptions
  - interaction with `SinkCompletionMode`

### Constraints

- Keep the API narrow.
- Avoid inventing a large overload family in the first release pass.

### Acceptance criteria

- Users can connect a stream to a delegate-based boundary through the sink abstraction.
- Delegate sink behavior is test-covered for success and failure.

### Files likely involved

- `src/Streamix/Extensions/TerminalExtensions.cs`
- `src/Streamix.Tests/TerminalExtensionsTests.cs`
- `README.md`

## ✅ Task 7: Update README And Examples For The Shipped Variety Surface

### Priority

Medium

### Goal

Make the release contract truthful once the API surface is settled.

### Why this exists

README currently lags some already-shipped boundary APIs and does not yet describe the sink abstraction this release is supposed to introduce.

### Scope

- Update the terminal section in `README.md`
- Document the final `ToDictionaryAsync(...)` duplicate-key semantics
- Document `ContainsAsync(...)`, `ToLookupAsync(...)`, `DrainAsync(...)`, and the comparer-aware terminal shape that actually ships
- Document the sink abstraction and the intended completion-ownership semantics
- Keep examples truthful and limited to shipped APIs

### Acceptance criteria

- README reflects the exact variety APIs that ship in the release.
- Boundary semantics are documented clearly enough for users to reason about completion, error propagation, and ownership.

### Files likely involved

- `README.md`
- `src/Streamix.Tests/ExampleTests.cs`

## Suggested Agent Handout Batches

### Batch A: lock current behavior and finish core terminals

- Task 1
- Task 2
- Task 3

### Batch B: sink pivot

- Task 4
- Task 5
- Task 6

### Batch C: public contract

- Task 7

## Final Checklist

- every task has a clear owner-sized scope
- every task has acceptance criteria
- decision-gate tasks are clearly marked
- likely files are listed to reduce agent search time
- execution order reflects real dependencies
