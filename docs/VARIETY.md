# Streamix Boundary Variety Plan

## Goal

Streamix already composes well inside a pipeline. The next step is boundary composition:

- more ways to finish a stream and extract a result
- more ways to push a stream into external systems safely

This document focuses on two areas only:

- terminal variety
- sink variety

## Current State

Today the main boundary surface looks like this:

- built-in `IStream<T>` terminals: `ForEachAsync(...)`, `ToChannel(...)`
- built-in `ISingle<T>` terminal: `ToTask()`
- extension-based terminals in `src/Streamix/Extensions/TerminalExtensions.cs`
- LINQ-style query extensions in `src/Streamix/Extensions/LinqExtensions.cs`

That is a good start, but the surface is still mostly “enumerate to collection” plus LINQ-style element reducers. For system boundaries, we need a clearer split between:

- materializers that return in-memory results
- sinks that copy the stream into another destination with explicit completion/error ownership

## Design Direction

The next additions should follow these rules:

1. Prefer boundary usefulness over LINQ parity for its own sake.
2. Keep the core package .NET-idiomatic and small.
3. Make cancellation, error propagation, completion ownership, and ordering explicit.
4. Avoid adding many ad hoc `ToXxxAsync` APIs when a general sink abstraction would cover the same ground.
5. Keep optional ecosystem-specific sinks in separate extension packages when they pull the API away from the minimal core.

## Terminal Variety

### What to add next in core

These are the most useful next terminals for `IStream<T>`.

#### 1. Element and membership terminals

- `ContainsAsync(T value, IEqualityComparer<T>? comparer = null, ...)`
- `ElementAtAsync(int index, ...)`
- `ElementAtOrDefaultAsync(int index, ...)`

Why:

- these are common boundary checks
- they short-circuit cleanly
- they add more value than cloning a long tail of obscure LINQ terminals

#### 2. Keyed and grouped materializers

- `ToLookupAsync<TKey>(Func<T, TKey> keySelector, ...)`
- `ToLookupAsync<TKey, TValue>(Func<T, TKey> keySelector, Func<T, TValue> valueSelector, ...)`
- comparer overloads for `ToDictionaryAsync(...)`, `ToHashSetAsync(...)`, and `ToLookupAsync(...)`

Why:

- grouped output is a real system-boundary shape
- comparer support is expected in .NET collection APIs

Important note:

- current `ToDictionaryAsync(...)` overwrites duplicate keys
- LINQ `ToDictionary(...)` throws on duplicates

Before adding more dictionary-style APIs, decide the contract:

- either align `ToDictionaryAsync(...)` with LINQ and throw on duplicate keys
- or keep last-write-wins semantics and expose that explicitly with a distinct name or option

This is the biggest terminal-semantic issue in the current implementation.

#### 3. Extremum-by-selector terminals

- `MinByAsync<TKey>(Func<T, TKey> keySelector, IComparer<TKey>? comparer = null, ...)`
- `MaxByAsync<TKey>(Func<T, TKey> keySelector, IComparer<TKey>? comparer = null, ...)`

Why:

- often needed at boundaries
- avoids forcing callers to project away the original element just to compute an extremum

#### 4. Completion-only terminal

- `DrainAsync(...)` or `IgnoreElementsAsync(...)`

Why:

- sometimes callers only need completion/error semantics
- `ForEachAsync(_ => { })` works but is not an intentional API

Prefer one name only. `DrainAsync` is shorter; `IgnoreElementsAsync` is more explicit.

#### 5. Async-predicate overloads for existing terminals

- `AnyAsync(Func<T, ValueTask<bool>> predicate, ...)`
- `AllAsync(Func<T, ValueTask<bool>> predicate, ...)`
- `CountAsync(Func<T, ValueTask<bool>> predicate, ...)`

Why:

- the library is async-first
- terminal APIs should not force sync predicates where the pipeline already supports async selectors/filters

### Good candidates, but lower priority

- `ToFrozenSetAsync(...)`
- `ToFrozenDictionaryAsync(...)`
- `ToQueueAsync(...)`
- `ToStackAsync(...)`
- selector overloads for `SumAsync(...)` / `AverageAsync(...)`
- broader numeric coverage if needed later

These are useful, but they should come after the semantic cleanup and the first sink pass.

### What not to prioritize yet

- cloning the full LINQ terminal surface
- highly specialized collection terminals with weak boundary value
- many aliases for the same behavior

The core risk here is surface-area sprawl without meaningfully improving system-boundary composition.

## Sink Variety

### Main recommendation

Do not keep adding one-off sink methods directly on `IStream<T>` one destination at a time.

Introduce a small sink abstraction first, then adapt concrete destinations to it.

### Proposed core abstraction

```csharp
public interface IAsyncSink<in T>
{
    ValueTask WriteAsync(T item, CancellationToken cancellationToken = default);
    ValueTask CompleteAsync(Exception? error = null, CancellationToken cancellationToken = default);
}
```

Then add one general terminal:

```csharp
public static Task ToSinkAsync<T>(
    this IStream<T> source,
    IAsyncSink<T> sink,
    SinkCompletionMode completionMode = SinkCompletionMode.CompleteSink,
    CancellationToken cancellationToken = default)
```

And define explicit completion ownership:

```csharp
public enum SinkCompletionMode
{
    CompleteSink,
    LeaveSinkOpen
}
```

### Why this is the right pivot

- keeps the stream side small
- makes sink semantics reusable
- allows adapters instead of duplicating copy logic
- supports backpressure naturally when `WriteAsync` awaits
- gives a consistent place for completion/error policy

### First adapters to build

#### 1. Channel adapter

Map existing `ToChannel(...)` onto the sink abstraction internally.

This preserves today’s API while making it an adapter, not a special case.

#### 2. Delegate-based sink

Add a lightweight adapter for:

```csharp
Func<T, CancellationToken, ValueTask>
```

This gives users an immediate way to connect Streamix to app-specific boundaries without creating custom sink classes.

#### 3. Collection sink for existing mutable collections

Prefer a copy-style API over more materializer proliferation:

- `CopyToAsync(ICollection<T> destination, ...)`
- optionally `CopyToAsync(ISet<T> destination, ...)`

This is useful when the caller already owns the destination collection and wants Streamix to fill it.

#### 4. Text sink

For boundary-heavy apps, a text writer sink is high value:

- `WriteLinesAsync(TextWriter writer, Func<T, string>? formatter = null, bool leaveOpen = false, ...)`

This should likely be an extension method backed by a sink adapter, not a new core interface member.

### Sinks to keep out of the first pass

- `Stream` / `PipeWriter` byte sinks unless Streamix also defines a clean byte-stream story
- `IObserver<T>` in core
- transport-specific sinks like Kafka, SignalR, gRPC, HTTP response bodies

Those should live in dedicated extension packages once the generic sink contract is in place.

## API Shape Guidance

### Keep materializers and copy-sinks separate

Use different names for different ownership models:

- materialize and return: `ToListAsync`, `ToDictionaryAsync`, `ToLookupAsync`
- copy into caller-owned destination: `CopyToAsync`, `WriteLinesAsync`, `ToSinkAsync`

This distinction matters. A method that allocates and returns a collection is not the same kind of boundary as a method that writes into an external destination.

### Do not overfit `ISingle<T>` yet

For now:

- keep `ISingle<T>.ToTask()` as the main terminal
- add `IStream<T>` terminals first
- only extend `ISingle<T>` when a boundary need clearly differs from `Task<T>`

## Suggested Execution Order

### Phase 1: terminal semantic cleanup

- decide duplicate-key behavior for `ToDictionaryAsync(...)`
- add comparer overloads where appropriate
- add tests that lock down cancellation and exception behavior for current terminals

### Phase 2: high-value terminals

- `ContainsAsync`
- `ElementAtAsync` / `ElementAtOrDefaultAsync`
- `ToLookupAsync`
- `MinByAsync` / `MaxByAsync`
- `DrainAsync` or `IgnoreElementsAsync`
- async-predicate overloads for `AnyAsync`, `AllAsync`, `CountAsync`

### Phase 3: sink abstraction

- add `IAsyncSink<T>`
- add `ToSinkAsync(...)`
- implement `ChannelWriter<T>` adapter
- make existing `ToChannel(...)` delegate to the shared sink path

### Phase 4: first sink adapters

- delegate-based sink adapter
- collection copy sink
- `TextWriter` line sink

### Phase 5: optional expansion

- frozen collection terminals
- ecosystem adapters in extension packages
- domain-specific sinks once real usage patterns appear

## Testing Plan

Every new terminal or sink should cover:

- success behavior
- empty-stream behavior where relevant
- cancellation during enumeration
- exception propagation from upstream
- exception propagation from the sink/action itself
- ordering guarantees where relevant
- completion ownership behavior for sinks
- duplicate-key behavior for keyed terminals

Additional sink-specific tests:

- bounded-channel backpressure
- sink completion on successful end
- sink completion on upstream error
- leave-open behavior

## Recommended Scope For The Next Implementation Slice

If we want the next slice to stay tight and high value, implement only this:

1. Lock down `ToDictionaryAsync(...)` semantics.
2. Add `ContainsAsync`, `ElementAtAsync`, `ElementAtOrDefaultAsync`, and `ToLookupAsync`.
3. Add one completion-only terminal: `DrainAsync`.
4. Introduce `IAsyncSink<T>` plus `ToSinkAsync(...)`.
5. Rework `ToChannel(...)` to use the sink abstraction.
6. Add a delegate-based sink adapter.

That gives Streamix a real boundary story without turning the core library into a grab bag of unrelated `ToXxxAsync` methods.
