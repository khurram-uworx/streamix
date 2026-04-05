# Streamix Creation Operators Next Tasks

## Purpose

This document breaks the remaining creation-operator roadmap from `docs/CREATION.md` into concrete, assignable tasks for the next release after the current creation slice.

The current release-targeted slice appears complete enough to move toward release planning. These tasks focus on the remaining phase 2 and phase 3 work rather than release-blocking fixes.

## Current Status

All six tasks in this document are now complete in the repository.

## Suggested Execution Order

1. Task 1: Add collection and lazy-enumerable stream factories
2. Task 2: Add `ValueTask`-based `Single` factories
3. Task 3: Add foundational time primitives (`Never`, `Timer`) and decide on `Poll`
4. Task 4: Add resource-scoped creation with `Using(...)`
5. Task 5: Add event/callback helper(s) built on `Create(...)`
6. Task 6: Finish README and test coverage for the expanded creation story

## Coordination Notes

- Task 3 is a decision gate because `Poll(...)` may stay as documentation built from `Interval(...)` instead of becoming a first-class API.
- Task 4 should not begin until the public `Using(...)` shape is agreed.
- Task 5 should build on the existing `Create(...)` contract rather than bypassing it.
- Task 6 depends on the public API decisions in Tasks 3, 4, and 5.
- Shared files likely to create merge conflicts:
  - `src/Streamix/Stream.cs`
  - `src/Streamix/Single.cs`
  - `src/Streamix/Implementations/Stream.cs`
  - `README.md`

## ✅ Task 1: Add Collection And Lazy-Enumerable Stream Factories

### Priority

High

### Goal

Add the remaining practical `Stream<T>` input bridges for in-memory collections and lazy async-enumerable factories.

### Why this exists

`docs/CREATION.md` identifies these as the next most practical source boundaries after the current release slice. They improve docs readability, test ergonomics, and integration with existing .NET code.

### Scope

- Add `Stream.From(IEnumerable<T>)`
- Add `Stream.From(params T[] items)`
- Add `Stream.From(Func<CancellationToken, IAsyncEnumerable<T>>)`
- Keep all three factories cold and cancellation-aware where applicable
- Add focused tests for success, laziness, cancellation, exception propagation, and repeated subscription

### Constraints

- Preserve Streamix's cold-by-default semantics
- Do not add overlapping aliases unless they materially improve clarity

### Acceptance criteria

- Callers can create streams directly from `IEnumerable<T>`, params arrays, and lazy `IAsyncEnumerable<T>` factories
- The lazy `IAsyncEnumerable<T>` factory is invoked once per subscription
- Cancellation and exception behavior are covered in `src/Streamix.Tests`

### Files likely involved

- `src/Streamix/Stream.cs`
- `src/Streamix/Implementations/Stream.cs`
- `src/Streamix.Tests/CreateTests.cs`
- `README.md`

## ✅ Task 2: Add `ValueTask`-Based `Single` Factories

### Priority

High

### Goal

Extend `Single<T>` creation to support modern `ValueTask<T>`-based APIs without forcing task allocation.

### Why this exists

`docs/CREATION.md` calls this out as the next natural step for async-first .NET integration after `Task<T>` support.

### Scope

- Add `Single.From(ValueTask<T>)`
- Add `Single.From(Func<ValueTask<T>>)`
- Add `Single.From(Func<CancellationToken, ValueTask<T>>)`
- Decide whether matching `Stream.From(...)` overloads should also be added in the same slice or deferred
- Add tests for eager vs lazy behavior, token propagation, cancellation, exception propagation, and repeated subscription

### Decision required

Decide whether `Stream` should mirror every new `Single` factory immediately or continue to flow through `Single.From(...)` only.

### Acceptance criteria

- `Single<T>` supports `ValueTask<T>`-based eager and lazy creation
- Lazy factories are invoked once per subscription
- Cancellation and exception semantics match the existing `Task<T>` overload family

### Files likely involved

- `src/Streamix/Single.cs`
- `src/Streamix/Stream.cs`
- `src/Streamix/Implementations/Single.cs`
- `src/Streamix.Tests/SingleFactoryTests.cs`
- `README.md`

## ✅ Task 3: Add Time-Primitives And Decide `Poll(...)`

### Priority

Medium

### Goal

Close the next time-based creation gaps with small primitives and an explicit product decision on polling.

### Why this exists

The plan identifies `Never<T>()`, `Timer(TimeSpan)`, and possibly `Poll(...)` as the next meaningful building blocks after `Interval(...)`.

### Decision

`Poll(...)` is a first-class core API in this slice.

### Scope

- Add `Stream.Never<T>()`
- Add `Stream.Timer(TimeSpan)`
- Add `Stream.Poll<T>(TimeSpan interval, Func<CancellationToken, ValueTask<T>> poll)`
- Add tests for timing semantics, cancellation, completion, and non-accumulating behavior where relevant

### Constraints

- Reuse the clock abstraction for deterministic tests
- Keep timer/poll semantics cold by default

### Acceptance criteria

- `Never<T>()` never emits and never completes unless cancelled
- `Timer(TimeSpan)` emits a single `0L` after the delay and then completes
- `Poll(...)` has explicit docs and tests for cancellation and repeated subscription behavior

### Files likely involved

- `src/Streamix/Stream.cs`
- `src/Streamix/Implementations/Stream.cs`
- `src/Streamix.Tests/TimeBasedOperatorTests.cs`
- `README.md`

## ✅ Task 4: Add Resource-Scoped Creation With `Using(...)`

### Priority

Medium

### Goal

Provide a first-class resource-lifetime creation API for sources that require setup and teardown.

### Why this exists

The plan explicitly calls out sockets, subscriptions, timers, handles, and readers as real integration boundaries that need structured cleanup.

### Decision required

Confirm the initial surface:

- `IAsyncDisposable` only, or
- both `IAsyncDisposable` and `IDisposable` overloads

### Scope

- Add `Stream.Using<TResource, T>(...)` with the agreed public shape
- Ensure disposal happens on normal completion, failure, and cancellation
- Decide and document whether disposal exceptions replace upstream exceptions or are secondary
- Add tests for success, failure, cancellation, and disposal ordering

### Constraints

- Keep the first slice small; avoid a large overload family until semantics are settled

### Acceptance criteria

- Resources are created per subscription and always disposed
- Disposal behavior is deterministic across success, failure, and cancellation paths
- The public contract is documented in README and tests

### Files likely involved

- `src/Streamix/Stream.cs`
- `src/Streamix/Implementations/Stream.cs`
- `src/Streamix.Tests/ResourceSafetyTests.cs`
- `README.md`

## ✅ Task 5: Add Event Or Callback Helper(s) On Top Of `Create(...)`

### Priority

Medium

### Goal

Offer at least one ergonomic adapter for common callback or event-driven boundaries without expanding the core API too aggressively.

### Why this exists

The plan explicitly recommends building boundary-specialized helpers on top of `Create(...)` once the primitive exists and its semantics are stable.

### Scope

- Pick one narrowly scoped helper family:
  - `FromEvent(...)`, or
  - `FromCallback(...)`
- Implement the helper on top of `Create(...)`
- Make subscription and unsubscription lifetime explicit
- Add tests for event delivery, cancellation, teardown, and repeated subscription

### Constraints

- Do not add many event-specific overloads in the first pass
- Prefer one helper pattern that proves the model

### Suggested implementation path

- Start with the smallest useful delegate shape
- Keep the helper internal-to-public implementation path thin by delegating to `Create(...)`

### Acceptance criteria

- Callers can bridge at least one common callback/event pattern without manually writing `Create(...)`
- Subscription teardown is correct on completion and cancellation
- The helper does not weaken the `Create(...)` backpressure and terminal-state contract

### Files likely involved

- `src/Streamix/Stream.cs`
- `src/Streamix/Implementations/Stream.cs`
- `src/Streamix.Tests/CreateTests.cs`
- `README.md`

## ✅ Task 6: Expand README And Example Coverage For The Next Creation Slice

### Priority

Low

### Goal

Keep the public contract truthful and make the next generation of creation operators visible through executable examples.

### Why this exists

Creation operators are adoption-facing APIs. If they are not visible and clearly explained, a large part of their value is lost.

### Scope

- Update the creation section in `README.md`
- Add or update example-oriented tests where practical
- Document eager vs lazy semantics for any new factory families
- Document cancellation and lifetime semantics for `Using(...)`, time primitives, and any event/callback helper

### Acceptance criteria

- README reflects only the APIs that actually ship
- Examples are truthful and match current semantics
- New creation APIs are represented by tests or executable snippets where practical

### Files likely involved

- `README.md`
- `src/Streamix.Tests/ExampleTests.cs`
- `src/Streamix.Tests/CreateTests.cs`
- `src/Streamix.Tests/SingleFactoryTests.cs`

## Suggested Agent Handout Batches

### Batch A: core factories

- Task 1
- Task 2

### Batch B: time and lifecycle

- Task 3
- Task 4

### Batch C: ergonomic adapters and docs

- Task 5
- Task 6

## Final Checklist

- every task has a clear owner-sized scope
- every task has acceptance criteria
- decision-gate tasks are clearly marked
- likely files are listed to reduce agent search time
- execution order reflects real dependencies
