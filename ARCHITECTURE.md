# Streamix Architecture

This document holds the design intent and implementation-oriented semantics for Streamix.

## Core Model

Modern .NET has `IAsyncEnumerable<T>` and channels, but Streamix intentionally provides a unified composable abstraction on top:

- `Stream<T>` for 0..N item streams
- `Single<T>` for 0..1 item streams

`Single.From(...)` supports values, `Task<T>`, `ValueTask<T>`, and `IAsyncEnumerable<T>` sources.

The default mental model is:

- cold, pull-based streams built on `IAsyncEnumerable<T>`
- channels only when coordination or fan-out is needed
- explicit async composition, cancellation, ordering, and error propagation

## Ordering and Concurrency Semantics

Ordered operators have explicit runtime semantics:

- `MapOrdered` and `FlatMapOrdered` preserve source order even when later work finishes first.
- Later ordered results or failures are not observed until earlier ordered work has been drained.
- `FlatMapOrdered` may buffer later inner items up to `maxBufferedItemsPerInner` while waiting for earlier inners.
- Cancelling enumeration stops waiting and propagates cancellation into the ordered operator's in-flight work.

## Structured Concurrency and Supervision

Streamix uses a unified supervision model for both structured concurrency (`Stream.ScopedAsync`) and concurrent operators (`FlatMap`, `RunOnChannel`, etc.).

- **Deterministic Settlement**: A supervision boundary does not complete until the parent body has finished AND all registered child tasks have reached a terminal state.
- **Fail-Fast**: The first observed non-cancellation fault triggers boundary-wide cancellation via a linked token.
- **Exception Propagation**: After all children settle, the first encountered non-cancellation exception is propagated. Subsequent faults are suppressed to ensure deterministic error handling.
- **Boundary Integration**: Channel-backed boundaries (`PipeThroughChannel`, `RunOnChannel`) are fully integrated into the supervision model, ensuring that worker tasks are properly tracked and settled.

## Design Principles

- Async-first (`IAsyncEnumerable<T>` + Channels)
- Small, .NET-idiomatic API surface
- Minimal, .NET-idiomatic operators
- Pull by default, channel-backed coordination when needed
- Explicit behavior around concurrency, cancellation, and error propagation
- Optional interop with AsyncRx.NET through a separate package
- Optional EF integration through `Streamix.Extensions` (not in core `Streamix`)

## Extension Package Boundaries

- `Streamix` (core) has no `Microsoft.EntityFrameworkCore` dependency.
- `Streamix.Extensions` hosts optional integrations with heavier transitive graphs (for example AsyncRx and EF Core).
- Consumers that only need core stream operators should reference `Streamix` directly.

## Entity Framework Stream Semantics

`EfStream` in `Streamix.Extensions` adapts EF queries to `IStream<T>` with explicit lifetime and materialization behavior:

- Public buffered entry points are `EfStream.From(...)` and `Func<DbContext>.ToStream(...)`.
- Public streamed entry points are `EfStream.FromStreamed(...)` and `Func<DbContext>.ToStreamed(...)`.
- Factory-based usage creates one `DbContext` per subscription and disposes it on completion, error, or cancellation.
- Query builder delegates are executed against the same context instance that performs query execution.
- Buffered execution uses `ToListAsync(cancellationToken)`, then emits each item to downstream operators.
- Streamed execution uses `AsAsyncEnumerable()` and emits items as EF async enumeration advances.
- Buffered materialization remains the default on existing APIs for backward-compatible semantics.
- Streamed execution can short-circuit earlier under downstream operators such as `Take`, but it keeps the `DbContext` alive for the duration of enumeration.
- Streamed execution is provider-sensitive: if ordering matters, it must be expressed in the query; cancellation may be observed at different points by different providers; and failures can surface after partial emission rather than strictly before the first item.
- Caller-owned `DbContext` overloads are intentionally excluded so that subscription ownership, disposal, and same-context execution rules stay explicit and safe.
- EF-specific batching or paging helpers are intentionally deferred until real usage shows a gap that the buffered-versus-streamed choice plus existing Streamix operators does not address well.

## Implementation Notes

- Channels for flow control
- Lightweight operator chaining
- No reflection or heavy runtime magic
- Fully compatible with async streams

## Performance Guardrails and Characteristics

Streamix is designed for high-performance asynchronous streaming with the following characteristics:

- Backpressure by Design: Concurrent operators like `FlatMap`, `FlatMapOrdered`, `Merge`, and the task-returning concurrent `Map` overload utilize bounded `System.Threading.Channels`. This ensures that if a consumer is slower than the producer, the producer is naturally paused once the internal buffers are full, preventing unbounded memory growth.
- Zero-Allocation Sequential Operators: Basic operators like `Map`, `Filter`, `Take`, and `Skip` are implemented as thin wrappers over `IAsyncEnumerable<T>` using async iterators. They introduce minimal overhead and do not involve intermediate buffering.
- Bounded Concurrency: All flattening and parallel operators accept a `maxConcurrency` parameter, allowing you to strictly control the number of simultaneous asynchronous operations.
- Materialization Awareness: Operators that require state across multiple items, such as `Buffer(count)`, `Window(count)`, or `Replay(bufferSize)`, involve allocations proportional to their requested size. These should be used with appropriate bounds to manage memory usage.
- Watermark-Aware Windowing: Supports bounded out-of-order data processing by deriving a monotonic watermark (`maxObservedEventTimestamp - outOfOrderness`). Late events (timestamp <= watermark) are dropped, and windows are finalized once the watermark reaches the window's end.
- Hot Stream Efficiency: `ConnectableStream<T>` (via `Publish()` or `Replay()`) manages a single underlying subscription for multiple downstream consumers, reducing redundant upstream work and resource consumption.

## Boundary Semantics

- `ToDictionaryAsync(...)` follows .NET `Dictionary` semantics and throws on duplicate keys.
- `ToLookupAsync(...)` materializes grouped output and supports comparer overloads for key handling.
- `ContainsAsync(...)` short-circuits as soon as a matching value is found.
- `MinByAsync(...)` and `MaxByAsync(...)` support comparer overloads when default key ordering is not the desired ordering.
- `DrainAsync(...)` is the explicit completion-only terminal when you care about completion, cancellation, or failure but not emitted items.

Sink completion semantics are explicit:

- on successful completion, `CompleteAsync(null)` is called when completion is owned by the terminal
- on upstream or sink write failure, `CompleteAsync(error)` is called and the original exception is still propagated to the caller
- on cancellation, Streamix stops writing and does not complete the sink

`ToChannel(...)` is implemented as an adapter over the same sink path, so channel writes and custom sinks follow the same completion and error rules.
