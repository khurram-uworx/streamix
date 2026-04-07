# Streamix — Idiomatic Reactive Streams for .NET

> A lightweight, fluent, async-first streaming library for .NET.  
> Inspired by Project Reactor, designed natively for C#.

---

## ✨ Why Streamix?

Modern .NET has `IAsyncEnumerable<T>` and Channels, but we lack a **unified, composable streaming abstraction** that:

- Supports **0..N item streams** (`Stream<T>`) and **0..1 item streams** (`Single<T>`)  
- Handles **backpressure naturally**  
- Provides **declarative, chainable operators** like Reactor  
- LINQ/query syntax support
- Hot-stream primitives (`Publish`, `RefCount`, `Replay`)
- Integrates with **AsyncRx.NET**, **Channel** or raw `IAsyncEnumerable<T>`  

**Streamix bridges that gap** — without Rx-style complexity.

The default mental model is:

- cold, pull-based streams built on `IAsyncEnumerable<T>`
- channels only when coordination or fan-out is needed
- explicit async composition, cancellation, ordering, and error propagation

### `Stream<T>`
* Represents a stream of 0..N values.

```csharp
IStream<int> numbers = Stream.Range(1, 10);
```

### `Single<T>`
* Represents a stream of 0..1 values.

```csharp
ISingle<User> user = Single.From(GetUser(id));
```

`Single.From(...)` supports values, `Task<T>`, `ValueTask<T>`, and `IAsyncEnumerable<T>` sources.

---

## 🚀 Examples

### Basic Pipeline

```csharp
await Stream.Range(1, 10)
    .Filter(x => x % 2 == 0)
    .Map(x => x * 10)
    .ForEachAsync(Console.WriteLine);
```

### Async Composition

```csharp
var orders =
    GetUser(id)                           // Single<User>
    .FlatMap(user => GetOrders(user))     // Stream<Order>
    .Map(o => o.Product);                 // Stream<string>
```

Available patterns include:

* `Map` / `MapAwait` - 1:1 transforms, sequential and ordered
* `Map(Func<T, Task<TResult>>, maxConcurrency)` - 1:1 transform, concurrent and unordered
* `MapOrdered` - 1:1 transform, concurrent and ordered
* `Filter` / `FilterAwait`
* `FlatMap` / `FlatMapAwait` — 1:1 or 1:N transforms, unordered concurrent by default
* `ConcatMap` — 1:N transform, sequential and ordered
* `FlatMapOrdered` — 1:N transform, concurrent and ordered

---

## ⚙️ Concurrency & Backpressure

Streamix provides explicit control over concurrency and ordering.

- `Map(Func<T, TResult>)` and `MapAwait(Func<T, ValueTask<TResult>>)` are sequential and ordered.
- `Map(Func<T, Task<TResult>>, int maxConcurrency = int.MaxValue)` is concurrent and unordered.
- `MapOrdered(Func<T, Task<TResult>>, int maxConcurrency)` is concurrent and ordered.
- `FlatMap` and `FlatMapAwait` are the unordered concurrent flattening operators by default.

```csharp
await stream
    .Map(async x => await ProcessAsync(x), maxConcurrency: 5) // task-returning overload
    .ForEachAsync(Console.WriteLine);
```

### Concurrency Semantics

| Operator | Concurrency | Ordering | Use Case | Performance |
|----------|-------------|----------|----------|-------------|
| `Map(Func<T, TResult>)` | 1 | Ordered | Synchronous projection with minimal overhead | ⭐ |
| `MapAwait(Func<T, ValueTask<TResult>>)` | 1 | Ordered | Async projection when each item must complete before the next advances | ⭐ |
| `Map(Func<T, Task<TResult>>, ...)` | Configurable N, default unbounded | Unordered | Highest-throughput async 1:1 transform | ⭐⭐⭐ |
| `MapOrdered()` | Configurable N | Ordered | Async transform while preserving source order | ⭐⭐ |
| `FlatMap()` | Unbounded | Unordered | Fire-and-forget, fastest 1:N expansion | ⭐⭐⭐ |
| `FlatMapOrdered()` | Configurable N | Ordered | Expand while preserving source order with explicit per-inner buffering | ⭐⭐ |
| `ConcatMap()` | 1 (Sequential) | Ordered | Strict sequential processing | ⭐ |

When Streamix uses bounded channels internally, producers pause when buffers are full instead of unboundedly accumulating work.

### Backpressure Strategies

While Streamix provides implicit backpressure, you can explicitly control how to handle overflow when a producer outpaces a consumer.

*   **`OnBackpressureBuffer(capacity)`**: Buffers items up to capacity; throws `BackpressureException` on overflow.
*   **`OnBackpressureDrop()`**: Discards new items when the consumer is busy.
*   **`OnBackpressureLatest()`**: Keeps only the most recent item, discarding intermediate ones.
*   **`OnBackpressureError()`**: Immediately fails with `BackpressureException` if the consumer cannot keep up.

```csharp
// Buffer up to 100 items before failing
await stream.OnBackpressureBuffer(100).ForEachAsync(ProcessAsync);

// Drop metrics if the logging backend is slow
await metrics.OnBackpressureDrop().ForEachAsync(LogAsync);

// Keep only the latest state update
await state.OnBackpressureLatest().ForEachAsync(UpdateUIAsync);

// Fail immediately if consumer falls behind
await stream.OnBackpressureError().ForEachAsync(CriticalProcessAsync);
```

See [docs/BACKPRESSURE.md](docs/BACKPRESSURE.md) for more details.

Ordered operators have explicit runtime semantics:

- `MapOrdered` and `FlatMapOrdered` preserve source order even when later work finishes first.
- Later ordered results or failures are not observed until earlier ordered work has been drained.
- `FlatMapOrdered` may buffer later inner items up to `maxBufferedItemsPerInner` while waiting for earlier inners.
- Cancelling enumeration stops waiting and propagates cancellation into the ordered operator's in-flight work.

---

## 🧩 Hot vs Cold Streams

Streams are cold by default: each subscriber re-enumerates the source. Publish() turns a cold stream into a connectable shared stream.

```csharp
var cold = Stream.Range(1,3);  // cold by default
var hot = source.Publish();

using var connection = hot.Connect();
await hot.ForEachAsync(Console.WriteLine);
```

Use RefCount() to Auto-connects on the first subscriber and disconnects when the last subscriber leaves.

```csharp
var shared = Stream.Range(1, 3).Publish().RefCount()
```

Use Replay(.) to share the source and replay the most recent items to late subscribers.

```csharp
var replayed = Stream.Range(1, 3).Replay(2);
```

---

## 📦 Operators

* `Map` / `MapAwait` / `MapOrdered`
* `Filter` / `FilterAwait`
* `FlatMap` / `FlatMapAwait`
* `ConcatMap` / `FlatMapOrdered`
* `Generate`
* `Take` / `Skip`
* `Merge` / `Zip`
* `Buffer` / `Window`
* `Never` / `Timer` / `Poll`
* `Throttle` / `Delay`
* `Retry` / `Retry(..., backoffStrategy)` / `Timeout`
* `OnErrorResume` / `OnErrorReturn` / `OnErrorMap`
* `Publish` / `Replay` / `RefCount`
* `RunOn`
* `DoOnNext`, `Do`, `Tap`, `DoOnError`, `DoOnComplete`, `DoOnTerminate`

`IStream<T>` includes `ForEachAsync(...)`, sink output, and channel output. Additional terminal operators are available through extension methods:

* `ToListAsync`, `ToArrayAsync`, `ToHashSetAsync`, `ToDictionaryAsync`, `ToLookupAsync`
* `FirstAsync` / `LastAsync` (and `OrDefault` variants)
* `ElementAtAsync` / `ElementAtOrDefaultAsync`
* `ContainsAsync`
* `SingleAsync` (and `OrDefault` variant)
* `AggregateAsync` / `CountAsync` / `AnyAsync` / `AllAsync`
* `MinAsync` / `MaxAsync`
* `MinByAsync` / `MaxByAsync` (with comparer overloads)
* `SumAsync` / `AverageAsync`
* `DrainAsync` / `ToSinkAsync`

`ISingle<T>` also supports `ToTask()`.

### Boundary Semantics

* `ToDictionaryAsync(...)` follows .NET `Dictionary` semantics and throws on duplicate keys.
* `ToLookupAsync(...)` materializes grouped output and supports comparer overloads for key handling.
* `ContainsAsync(...)` short-circuits as soon as a matching value is found.
* `MinByAsync(...)` and `MaxByAsync(...)` support comparer overloads when default key ordering is not the desired ordering.
* `DrainAsync(...)` is the explicit completion-only terminal when you care about completion, cancellation, or failure but not emitted items.

---

## 🔗 LINQ & Query Syntax Support

Streamix supports both **fluent and query comprehension syntax**:

For now, LINQ is a convenience layer rather than the full concurrency-control surface:

- `Where` / `Select` and their async counterparts stay sequential and ordered.
- `SelectMany` / `SelectManyAsync` are the unordered flattening helpers.
- Use fluent operators such as `FlatMap`, `ConcatMap`, and `FlatMapOrdered` when you need explicit unordered, sequential, or ordered flattening control.

```csharp
// Query syntax (from...where...select)
var result = await (
    from x in Stream.Range(1, 10)
    where x % 2 == 0
    select x * 10
).ToListAsync();

// Fluent syntax
var result = await Stream.Range(1, 10)
    .Where(x => x % 2 == 0)
    .Select(x => x * 10)
    .ToListAsync();

// Async with ValueTask<T>
var result = await Stream.Range(1, 10)
    .WhereAsync(async x => await ValidateAsync(x))
    .SelectAsync(async x => await FetchAsync(x))
    .SelectManyAsync(async x => await GetStream(x), maxConcurrency: 3) // unordered flattening
    .ToListAsync();

var ordered = await Stream.Range(1, 10)
    .FlatMapOrdered(x => GetStream(x), maxConcurrency: 3)
    .ToListAsync();
```

**Available:** `Where`, `Select`, `SelectMany` (sync) and `WhereAsync`, `SelectAsync`, `SelectManyAsync` (async)

---

## 🏗️ Creation Operators

Streamix provides a rich set of operators to create streams from various sources.

### `Stream.Create<T>`
For complex sources, callbacks, or event-driven systems.
```csharp
var stream = Stream.Create<int>(async emitter => {
    await emitter.EmitAsync(1);
    if (someCondition) {
        emitter.Fail(new Exception("Oops"));
    } else {
        emitter.Complete();
    }
});
```
*   **Backpressure**: `EmitAsync` awaits if the downstream consumer is slow.
*   **Cancellation**: Check `emitter.CancellationToken` to stop producing. `EmitAsync` will throw an `OperationCanceledException` if the subscriber cancels or if the stream has already reached a terminal state (`Complete` or `Fail`).

### `Stream.FromEvent<T>`
For async callback or event sources that can await item delivery and return an `IDisposable` subscription.
```csharp
var source = new PriceFeed();

var prices = Stream.FromEvent<decimal>(handler => source.Subscribe(handler));

var latest = await prices.Take(2).ToListAsync();
```
*   **Shape**: `FromEvent(...)` is intentionally narrow in the first pass. It expects a subscription function that accepts an async handler (`Func<T, ValueTask>`) and returns an `IDisposable` used for teardown.
*   **Backpressure**: When the source awaits the handler, `FromEvent(...)` preserves the same backpressure contract as `Create(...)`.
*   **Lifetime**: Each subscriber gets its own registration. Cancelling or disposing the subscription always disposes the returned registration.

### `Stream.Defer<T>` / `Single.Defer<T>`
Lazy creation: the factory is called once per subscriber.
```csharp
var stream = Stream.Defer(() => Stream.From(DateTime.Now.Ticks));
```

### `Stream.Generate<TState, T>`
Stateful generation of sequences.
```csharp
var stream = Stream.Generate(0, state =>
    state < 10 ? GenerationResult<int, int>.Emit(state, state + 1)
               : GenerationResult<int, int>.Complete());
```

### `Stream.Interval`
Periodic emissions based on time.
```csharp
var stream = Stream.Interval(TimeSpan.FromSeconds(1));
```
*   **Backpressure**: Does not accumulate ticks. If a consumer is slow, the next interval starts only after the consumer is ready.

### `Stream.Never`
Non-terminating stream primitive.
```csharp
var stream = Stream.Never<int>();
```
*   **Semantics**: Never emits and never completes unless the subscriber cancels.

### `Stream.Timer`
Single delayed emission.
```csharp
var stream = Stream.Timer(TimeSpan.FromSeconds(5));
```
*   **Semantics**: Emits a single `0L` after the due time, then completes.

### `Stream.Poll`
Periodic async polling.
```csharp
var polled = Stream.Poll(
    TimeSpan.FromSeconds(1),
    async ct => await PollOnceAsync(ct));
```
*   **Semantics**: Cold by default, passes the subscriber cancellation token into the poll callback, and waits for each poll result before scheduling the next interval.

### `Stream.Using<TResource, T>`
Manages the lifetime of a resource (e.g., sockets, readers, subscriptions) per subscriber.
```csharp
var stream = Stream.Using(
    () => new StreamReader("data.txt"),
    reader => Stream.Create<string>(async emitter => {
        while (!reader.EndOfStream) {
            await emitter.EmitAsync(await reader.ReadLineAsync());
        }
    })
);
```
*   **Disposal**: The resource is guaranteed to be disposed (via `Dispose` or `DisposeAsync`) when the stream completes, fails, or the subscription is cancelled.
*   **Exceptions**: Standard C# semantics apply; if both the stream and the disposal throw, the disposal exception is propagated.

### `Stream.From` / `Single.From` / `Just`
Shorthands for values, Tasks, and Async Enumerables.

*   **Eager vs. Lazy**:
    *   `From(Task<T>)` and `From(ValueTask<T>)` wrap existing, already-started work (**eager**). If the task is already completed, the stream will emit the result immediately upon subscription.
    *   `From(Func<Task<T>>)`, `From(Func<ValueTask<T>>)`, and their `CancellationToken` overloads defer the creation and start of the task until the stream is subscribed to (**lazy**). The factory is called once per subscriber.
    *   `Stream.Defer(...)` and `Single.Defer(...)` are always **lazy** and call the factory once per subscriber to create the entire stream instance.
*   **Single Cardinality**:
    *   `Single.From(IAsyncEnumerable<T>)` strictly enforces a **0..1 cardinality**. If the source enumerable produces more than one item, an `InvalidOperationException` is thrown during enumeration.

```csharp
Stream.Just(42);
Stream.From(new[] { 1, 2, 3 }); // from array
Stream.From(Task.FromResult("hello")); // eager
Stream.From(async ct => await FetchData(ct)); // lazy IAsyncEnumerable factory
Single.From(async ct => await FetchData(ct)); // lazy Task factory
```

---

## 🔌 Interop

### `IAsyncEnumerable<T>`

```csharp
IStream<int> stream = Stream.From(asyncEnumerable);
ISingle<int> single = Single.From(task);
```

### Channels

```csharp
using System.Threading.Channels;

var channel = Channel.CreateUnbounded<int>();
IStream<int> fromChannel = Stream.FromChannel(channel);

await Stream.Range(1, 3).ToChannel(channel.Writer, completeWriter: true);
```

### Sinks

Streamix also exposes a small reusable sink abstraction for boundary writes:

```csharp
var output = new List<int>();

await Stream.Range(1, 3).ToSinkAsync(
    (item, ct) =>
    {
        output.Add(item);
        return ValueTask.CompletedTask;
    });
```

The core contract is:

```csharp
public interface IAsyncSink<in T>
{
    ValueTask WriteAsync(T item, CancellationToken cancellationToken = default);
    ValueTask CompleteAsync(Exception? error = null, CancellationToken cancellationToken = default);
}
```

Use `SinkCompletionMode.CompleteSink` to let the terminal own sink completion, or `SinkCompletionMode.LeaveSinkOpen` when the caller owns the destination lifetime.

Sink completion semantics are explicit:

* on successful completion, `CompleteAsync(null)` is called when completion is owned by the terminal
* on upstream or sink write failure, `CompleteAsync(error)` is called and the original exception is still propagated to the caller
* on cancellation, Streamix stops writing and does not complete the sink

`ToChannel(...)` is implemented as an adapter over the same sink path, so channel writes and custom sinks follow the same completion and error rules.

### AsyncRx.NET

AsyncRx interop lives in the separate `Streamix.Extensions` project so the core package does not take a dependency on `System.Reactive.Async`.

```csharp
using Streamix.Extensions;

IStream<int> stream = asyncObservable.ToStream();
IAsyncObservable<int> observable = stream.ToAsyncObservable();
```

The extensions project also supports `ISingle<T>` interop.

---

## 🧵 Execution

* Runs on caller context by default
* `RunOn(TaskScheduler)` moves upstream execution onto a chosen scheduler.

```csharp
stream.RunOn(TaskScheduler.Default);
```

---

## ⚠️ Error Handling

```csharp
var recovered = stream
    .Map(...)
    .OnErrorResume(ex => Stream.Empty<int>());

var retried = stream
    .Retry(retryCount: 3,
    backoffStrategy: (attempt, ex) => TimeSpan.FromMilliseconds(attempt * 100));
```

---

## 📌 Design Principles

* Async-first (`IAsyncEnumerable<T>` + Channels)
* Small, .NET-idiomatic API surface
* Minimal, .NET-idiomatic operators
* Pull by default, channel-backed coordination when needed
* Explicit behavior around concurrency, cancellation, and error propagation
* Optional interop with AsyncRx.NET through a separate package

---

## 🛠️ Implementation Notes

* Channels for flow control
* Lightweight operator chaining
* No reflection or heavy runtime magic
* Fully compatible with async streams

---

## 🚀 Performance Guardrails & Characteristics

Streamix is designed for high-performance asynchronous streaming with the following characteristics:

- **Backpressure by Design**: Concurrent operators like `FlatMap`, `FlatMapOrdered`, `Merge`, and the task-returning concurrent `Map` overload utilize bounded `System.Threading.Channels`. This ensures that if a consumer is slower than the producer, the producer is naturally paused once the internal buffers are full, preventing unbounded memory growth.
- **Zero-Allocation Sequential Operators**: Basic operators like `Map`, `Filter`, `Take`, and `Skip` are implemented as thin wrappers over `IAsyncEnumerable<T>` using async iterators. They introduce minimal overhead and do not involve intermediate buffering.
- **Bounded Concurrency**: All flattening and parallel operators accept a `maxConcurrency` parameter, allowing you to strictly control the number of simultaneous asynchronous operations.
- **Materialization Awareness**: Operators that require state across multiple items, such as `Buffer(count)`, `Window(count)`, or `Replay(bufferSize)`, involve allocations proportional to their requested size. These should be used with appropriate bounds to manage memory usage.
- **Hot Stream Efficiency**: `ConnectableStream<T>` (via `Publish()` or `Replay()`) manages a single underlying subscription for multiple downstream consumers, reducing redundant upstream work and resource consumption.

## 🎯 When to Use

* High-throughput async pipelines
* Composable stream processing
* Reactive-style data transformations
* Situations where Reactor-style API helps readability & maintainability

---

## 🚫 When Not to Use

* Simple sequential async calls (`await` is enough)
* CPU-bound work (use Parallel / PLINQ)
* Legacy Rx-only pipelines

---

## 🧭 Roadmap

* Structured concurrency support
* ASP.NET Core integration for reactive endpoints
* Additional time-based operators
* Source generators for optimized pipelines

---

## 🤝 Contributing

* Keep API fluent & minimal
* Focus on async-first idioms
* Backpressure awareness is required for stream operators

---

## 📜 License

MIT
