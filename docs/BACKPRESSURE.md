# Backpressure Strategies in Streamix

## Overview

Streamix currently supports backpressure through its `Channel<T>`-based internal implementation, providing implicit flow control via await semantics. However, backpressure handling remains **implicit and opaque** to developers.

This document outlines explicit backpressure strategies to give developers **clear, composable, and idiomatic control** over how streams handle slow consumers.

## Motivation

### The Problem

- **Rx.NET**: Weak backpressure story; mixing concerns
- **Raw `Channel<T>`**: Developers must manually handle overflow
- **`IAsyncEnumerable`**: Implicit backpressure, no control
- **.NET in general**: No standardized, clean backpressure pattern

Streamix can fill this gap by providing **explicit, opt-in strategies** inspired by Project Reactor but tailored to .NET idioms and Streamix's channel-based architecture.

### Why Backpressure Matters

When a producer emits faster than a consumer can handle:
- **Without strategy**: Items are dropped, buffered indefinitely, or cause exceptions
- **With strategy**: Developers know exactly what happens and can reason about resource usage

## Proposed API

```csharp
/// <summary>
/// Buffers items up to <paramref name="capacity"/> when downstream is slow.
/// Throws BackpressureException if buffer overflows.
/// </summary>
/// <param name="capacity">Maximum number of items to buffer.</param>
/// <returns>A stream with backpressure buffering strategy applied.</returns>
IStream<T> OnBackpressureBuffer(int capacity);

/// <summary>
/// Drops items when downstream cannot keep pace.
/// The most recent item is always emitted when the consumer catches up.
/// </summary>
/// <returns>A stream with backpressure drop strategy applied.</returns>
IStream<T> OnBackpressureDrop();

/// <summary>
/// Keeps only the latest item when downstream is slow.
/// Older items in the buffer are discarded in favor of newer ones.
/// </summary>
/// <returns>A stream with backpressure latest strategy applied.</returns>
IStream<T> OnBackpressureLatest();

/// <summary>
/// Throws a BackpressureException when downstream cannot keep pace.
/// Signals immediate failure rather than buffering or dropping items.
/// </summary>
/// <returns>A stream with backpressure error strategy applied.</returns>
IStream<T> OnBackpressureError();
```

## Strategy Semantics

### `OnBackpressureBuffer(capacity)`

**Behavior:**
- Items are buffered in a bounded queue up to `capacity`
- If the buffer is full when a new item arrives, a `BackpressureException` is thrown
- Suitable for: Scenarios where temporary queuing is acceptable but overflow must fail fast

**Example:**
```csharp
stream
    .OnBackpressureBuffer(100)
    .ForEachAsync(async item => await ProcessSlowly(item));
```

### `OnBackpressureDrop()`

**Behavior:**
- Items are dropped when the internal buffer is full (i.e., new items are discarded)
- The items currently in the buffer are preserved
- Suitable for: Scenarios where you want to keep existing work and discard any new work that arrives while the consumer is busy

**Example:**
```csharp
var metrics = Stream.Poll(TimeSpan.FromMilliseconds(10), async ct => 
    await GetMetricAsync(ct));

metrics
    .OnBackpressureDrop()
    .ForEachAsync(async m => await LogToSlowBackend(m));
```

### `OnBackpressureLatest()`

**Behavior:**
- Keeps only the latest item in the buffer when downstream is slow
- Intermediate items are discarded
- Suitable for: State-like streams where only the newest value is relevant (UI updates, configuration changes)

**Example:**
```csharp
configUpdates
    .OnBackpressureLatest()
    .ForEachAsync(async cfg => await ReloadConfig(cfg));
```

### `OnBackpressureError()`

**Behavior:**
- Immediately throws `BackpressureException` when the producer outpaces the consumer
- No buffering, no dropping—immediate failure
- Suitable for: Strict scenarios where overflow indicates a design problem that must be surfaced immediately

**Example:**
```csharp
stream
    .OnBackpressureError()
    .ForEachAsync(async item => ProcessItem(item))
    .ContinueWith(t => 
    {
        if (t.IsFaulted && t.Exception?.InnerException is BackpressureException)
            logger.Error("Consumer too slow");
    });
```

## Design Principles

1. **Explicit Over Implicit**: Developers must opt-in; no hidden behavior changes
2. **Fail Fast When Needed**: `Buffer` and `Error` strategies alert developers to design issues
3. **Idiomatic to .NET**: Naming conventions match Streamix patterns (`OnError*`, `DoOn*`); exceptions for errors
4. **Channel-Native**: Strategies map directly to how `Channel<T>` works internally
5. **Composability**: Strategies are mutually exclusive (last one wins) to avoid confusion
6. **Documentation First**: Each strategy must document what happens to items and when exceptions occur

## Implementation Notes

### Internal Representation

- **`OnBackpressureBuffer`**: Bounded `Channel<T>` with capacity; throws on overflow
- **`OnBackpressureDrop`**: Bounded channel with `ChannelFullMode.DropWrite` (drop newest by default, or keep oldest)
- **`OnBackpressureLatest`**: Bounded channel with custom logic to retain only the latest item
- **`OnBackpressureError`**: Bounded channel that immediately errors on full

### Exception Handling

All strategies use a common `BackpressureException` type:

```csharp
public sealed class BackpressureException : InvalidOperationException
{
    public BackpressureException(string message) : base(message) { }
}
```

Thrown when:
- `OnBackpressureBuffer` buffer overflows
- `OnBackpressureError` is triggered
- Optionally, when `OnBackpressureLatest` or `OnBackpressureDrop` detect capacity exhaustion (configurable)

### Operator Precedence

If multiple backpressure strategies are chained, the **last one wins**:

```csharp
stream
    .OnBackpressureBuffer(100)
    .OnBackpressureDrop()  // This overrides Buffer
    .ForEachAsync(...);
```

This avoids confusion and aligns with reactive library conventions.

## Testing Strategy

- **Unit tests**: Each strategy in isolation
- **Integration tests**: Combining backpressure strategies with operators like `Map`, `Filter`, `FlatMap`
- **Load tests**: Producer/consumer mismatch scenarios (fast producer, slow consumer)
- **Exception tests**: Verify correct exception types and messages

## Common Patterns

### Metrics & Logging (Drop Strategy)
When collecting metrics or logging events, it's often better to drop some data than to crash the application or slow down the main processing logic.
```csharp
var metrics = Stream.Poll(TimeSpan.FromMilliseconds(10), GetMetricAsync);

await metrics
    .OnBackpressureDrop()
    .ForEachAsync(async m => await LogToSlowBackend(m));
```

### UI & State Updates (Latest Strategy)
For UI updates or configuration changes, only the most recent value is usually relevant. If the consumer is slow, intermediate values can safely be skipped.
```csharp
configUpdates
    .OnBackpressureLatest()
    .ForEachAsync(async cfg => await ReloadConfig(cfg));
```

### Strict Validation (Error/Buffer Strategy)
In scenarios where data loss is unacceptable and overflow indicates a critical system bottleneck, use `OnBackpressureError` or a fixed-size `OnBackpressureBuffer`.
```csharp
// Fail fast if processing cannot keep up with incoming orders
await orders
    .OnBackpressureError()
    .ForEachAsync(ProcessOrderAsync);
```

## Future Considerations

1. **`OnBackpressureSkipDuplicate()`**: Skip consecutive duplicate items (useful for state streams)
2. **Metrics**: Expose backpressure statistics (items dropped, buffered, errors)
3. **Adaptive strategies**: Auto-tune buffer size based on producer/consumer rates
4. **Backpressure feedback**: Signal upstream producers to slow down (Reactive Streams style `request(n)`)

## References

- **Project Reactor**: [BackpressureHandling](https://projectreactor.io/docs/core/release/reference/#advanced-backpressure-handling)
- **Reactive Streams**: [Backpressure Protocol](http://www.reactive-streams.org/)
- **.NET Channels**: [System.Threading.Channels Documentation](https://docs.microsoft.com/en-us/dotnet/api/system.threading.channels)
