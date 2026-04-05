# Streamix Creation Operators Plan

## Goal

Streamix needs a stronger source story.

Today the library composes well once data is already inside `IAsyncEnumerable<T>`, but real systems often begin at boundaries that are:

- task-based
- callback-based
- event-driven
- polling-based
- time-based
- resource-scoped

This document focuses only on creation operators that help callers enter Streamix cleanly and idiomatically.

## Current State

Current built-in factories are narrow.

`Stream` currently exposes:

- `From(IAsyncEnumerable<T>)`
- `From(ISingle<T>)`
- `From(T)`
- `Empty<T>()`
- `Error<T>(Exception)`
- `Range(int start, int count)`
- `FromChannel(ChannelReader<T>)`
- `FromChannel(Channel<T>)`

`Single` currently exposes:

- `From(IAsyncEnumerable<T>)`
- `From(T)`
- `From(Task<T>)`
- `Empty<T>()`
- `Error<T>(Exception)`

That means Streamix can consume async streams and some task/value sources, but it does not yet offer a rich set of source factories for common app boundaries.

## Design Direction

Creation operators should follow these rules:

1. Prefer cold-by-default behavior unless the operator is explicitly hot or callback-driven.
2. Make cancellation, completion, and error propagation explicit.
3. Do not force users through `Channel<T>` just to bridge callback or event sources.
4. Keep the core API small, but cover the most common real-world entry points.
5. Distinguish clearly between `Stream<T>` factories and `Single<T>` factories.
6. Preserve .NET idioms instead of mechanically copying Reactor/Rx names where a clearer name exists.

## Highest-Priority Gap

The biggest missing capability is boundary composition.

Without richer creation operators, callers must hand-roll adapters from:

- `Task<T>`
- `Func<Task<T>>`
- callbacks
- timers
- polling loops
- event emitters
- resource-scoped async iterators

That increases friction exactly where adoption starts.

## Recommended Operator Set

### Phase 1: immediate boundary coverage

These are the first operators to add.

#### `Single.From(Func<Task<T>>)`

Why:

- defers task creation until subscription
- avoids eager side effects
- matches how callers naturally represent one-shot async work

Recommended shape:

```csharp
public static ISingle<T> From<T>(Func<Task<T>> factory)
public static ISingle<T> From<T>(Func<CancellationToken, Task<T>> factory)
```

Notes:

- the `CancellationToken` overload is the better long-term shape
- the non-token overload is still pragmatic and easy to adopt

#### `Stream.Defer(...)`

Why:

- lazy source creation is foundational
- lets each subscriber get a fresh source
- composes well with retry/repeat-style behavior later

Recommended shape:

```csharp
public static IStream<T> Defer<T>(Func<IStream<T>> factory)
public static IStream<T> Defer<T>(Func<CancellationToken, IStream<T>> factory)
```

Also add:

```csharp
public static ISingle<T> Defer<T>(Func<ISingle<T>> factory)
public static ISingle<T> Defer<T>(Func<CancellationToken, ISingle<T>> factory)
```

Important semantic rule:

- factory invocation happens on enumeration/subscription, not at method call time

#### `Stream.Create(...)`

Why:

- this is the main escape hatch for callback-based and event-driven integration
- it gives Streamix a native way to model push-style producers
- it is the operator that unlocks adapters the fastest

Recommended shape:

```csharp
public static IStream<T> Create<T>(
    Func<IStreamEmitter<T>, CancellationToken, ValueTask> producer)
```

Emitter responsibilities should include:

- `EmitAsync(T item, CancellationToken cancellationToken = default)`
- `Complete()`
- `Fail(Exception exception)`

Design notes:

- `EmitAsync` should honor backpressure
- completion and failure should be idempotent
- emissions after terminal state should fail predictably
- `Create` should be cold by default: each subscriber gets a new producer run

This is the most important addition in the whole plan.

#### `Stream.Generate(...)`

Why:

- covers pull-friendly stateful generation without forcing `Create`
- useful for counters, cursors, pagination, and finite state machines

Recommended first shape:

```csharp
public static IStream<T> Generate<TState, T>(
    TState initialState,
    Func<TState, (bool HasValue, T Value, TState NextState)> generator)
```

Better async-friendly long-term shape:

```csharp
public static IStream<T> Generate<TState, T>(
    TState initialState,
    Func<TState, CancellationToken, ValueTask<GenerationResult<TState, T>>> generator)
```

Where `GenerationResult` carries:

- whether to emit
- the emitted value
- the next state
- whether generation is complete

This avoids abusing exceptions or sentinel values to stop generation.

#### `Stream.Interval(TimeSpan)`

Why:

- common source for polling, heartbeats, retries, and timers
- needed before many time-based composition patterns feel complete

Recommended shape:

```csharp
public static IStream<long> Interval(TimeSpan period)
public static IStream<long> Interval(TimeSpan dueTime, TimeSpan period)
```

Semantic rules:

- emits monotonically increasing `long` values starting at `0`
- respects cancellation promptly
- uses the library clock abstraction where possible
- does not accumulate unbounded timer backlog if the consumer is slow

### Phase 2: practical utility factories

These are strong follow-ons once the core set exists.

#### `Stream.From(IEnumerable<T>)`

Why:

- very common boundary
- avoids forcing callers through `.ToAsyncEnumerable()` or custom helpers

#### `Stream.From(params T[] items)`

Why:

- improves readability in tests and examples
- useful for docs and operator tests

#### `Stream.From(Func<CancellationToken, IAsyncEnumerable<T>>)`

Why:

- gives a lightweight lazy async-enumerable bridge without full `Defer`
- useful when the caller already thinks in `IAsyncEnumerable<T>`

#### `Single.From(ValueTask<T>)`

Why:

- aligns with async-first APIs in modern .NET
- avoids task allocation in some sources

#### `Single.From(Func<ValueTask<T>>)` and token overload

Why:

- same reason as `Func<Task<T>>`, but more idiomatic for new APIs

### Phase 3: resource-scoped and boundary-specialized creation

These matter for robust real-world integration.

#### `Using(...)`

Why:

- lets a source own setup/teardown cleanly
- useful for sockets, subscriptions, timers, readers, and handles

Recommended direction:

```csharp
public static IStream<T> Using<TResource, T>(
    Func<CancellationToken, ValueTask<TResource>> resourceFactory,
    Func<TResource, CancellationToken, IStream<T>> streamFactory)
    where TResource : IAsyncDisposable
```

Potential overloads can support `IDisposable`.

#### Polling helper

Why:

- polling is one of the explicit target scenarios
- users will otherwise rebuild it repeatedly from `Interval + SelectMany`

Recommended direction:

```csharp
public static IStream<T> Poll<T>(
    TimeSpan interval,
    Func<CancellationToken, ValueTask<T>> poll)
```

Semantics:

- cold by default
- first poll occurs after the interval elapses
- passes the subscriber token into the poll callback
- does not accumulate timer backlog when the consumer is slow

#### Event and callback helpers

Why:

- these are a major adoption boundary in UI, messaging, and legacy APIs

Recommended direction:

Prefer building these on top of `Create(...)` first rather than adding many direct factories immediately.

Possible later helpers:

```csharp
public static IStream<T> FromEvent<TDelegate, T>(...)
public static IStream<T> FromCallback<T>(...)
```

But these should come after `Create`, not before.

## Additional Operators Worth Considering

Beyond the set you listed, these are the next best candidates.

### `Stream.Never<T>()`

Why:

- useful in tests and control-flow composition
- standard reactive primitive

Semantics:

- never emits
- never completes
- only stops on cancellation

### `Stream.Timer(TimeSpan)`

Why:

- clearer than `Interval` when only one delayed tick is needed
- often pairs naturally with timeout, delay, or retry orchestration

Semantics:

- emits a single `0L` after the due time, then completes

### `Stream.Repeat(T value)` or `Repeat(Func<T>)`

Why:

- occasionally useful, but lower priority
- can often wait until `Generate` exists

### `Stream.Unfold(...)`

Why:

- often a clearer functional name than a very general `Generate`
- could remain internal naming even if the public API stays `Generate`

### `Single.Defer(...)`

Why:

- as important for one-shot work as `Stream.Defer(...)` is for multi-item work
- especially important if `Single.From(Task<T>)` remains eager

## Operators To Avoid In The First Pass

Do not expand the first implementation slice with:

- many event-specific helpers
- observer-pattern specific factories in core
- transport-specific sources
- hot-source abstractions with complicated lifetime policies
- overlapping aliases that differ only in naming

The first goal is to cover the common boundary shapes with a small set of primitives.

## Proposed API Priorities

If we want the smallest high-value slice, the order should be:

1. `Single.From(Func<Task<T>>)` and token overload
2. `Single.Defer(...)`
3. `Stream.Defer(...)`
4. `Stream.Create(...)`
5. `Stream.Generate(...)`
6. `Stream.Interval(...)`

That set covers:

- lazy single-shot work
- lazy stream construction
- callback/event bridging
- stateful generation
- time-based source generation

## Semantics To Lock Down Before Implementation

These need explicit decisions before coding.

### Cold vs hot

Recommendation:

- `Defer`, `From(Func<...>)`, `Generate`, and `Interval` should be cold
- each subscriber gets a fresh execution
- sharing should remain an explicit opt-in via `Publish`, `Replay`, or `RefCount`

### Cancellation

Recommendation:

- every async factory should accept or internally honor `CancellationToken`
- cancellation should stop upstream work promptly
- cancellation should not be translated into successful completion

### Backpressure in `Create`

Recommendation:

- `EmitAsync` must await when downstream is not ready
- bounded buffering should be explicit, not accidental
- if a buffer is introduced, it must have defined size and ownership

### Terminal signaling in `Create`

Recommendation:

- producer may call either `Complete()` or `Fail(...)` once
- producer exceptions should fail the stream if no terminal signal has already been sent
- late calls after termination should not corrupt state

### `Single` cardinality

Recommendation:

- `Single` creation helpers should preserve the 0..1 contract
- if `Single.From(IAsyncEnumerable<T>)` remains permissive, document current behavior clearly
- for new `Single` factories, prefer shapes that naturally produce only one value

## Testing Plan

Each new creation operator should cover:

- success behavior
- lazy execution
- cancellation behavior
- exception propagation
- repeated subscription behavior
- ordering semantics where relevant
- completion semantics

Additional operator-specific tests:

### `Create`

- emitter completes successfully
- emitter fails
- producer throws before completion
- producer throws after terminal signal
- downstream cancellation stops producer
- backpressure is respected under bounded consumption

### `Generate`

- finite generation
- empty generation
- cancellation mid-generation
- exception thrown by generator

### `Interval`

- first tick timing semantics
- cancellation during wait
- no extra ticks after cancellation
- slow consumer behavior

### `Defer`

- factory not invoked until enumeration
- factory invoked once per subscription
- factory exception propagates correctly

## Suggested First Implementation Slice

If the next coding pass should stay tight and critical-path focused, implement only:

1. `Single.From(Func<Task<T>>)` plus `Func<CancellationToken, Task<T>>`
2. `Single.Defer(...)`
3. `Stream.Defer(...)`
4. `Stream.Create(...)`
5. `Stream.Interval(...)`

Then add `Generate(...)` in the next slice.

Reason:

- `Defer` and lazy `Single` factories fix eager-boundary issues immediately
- `Create` unlocks callbacks and events
- `Interval` unlocks polling and timer-driven flows
- `Generate` is valuable, but less urgent than the boundary adapters above

## Recommended README Follow-up

Once the first slice lands, update `README.md` with a dedicated "Creation" section that shows:

- `Single.From(Func<Task<T>>)`
- `Stream.Defer(...)`
- `Stream.Create(...)`
- `Stream.Interval(...)`
- one callback or polling example built from these primitives

That will make the new source story visible, which matters as much as the implementation itself.
