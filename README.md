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

---

## 🧩 Core Types

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

`Single.From(...)` supports values, `Task<T>`, and `IAsyncEnumerable<T>` sources.

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
    .FlatMapMany(user => GetOrders(user)) // Stream<Order>
    .Map(o => o.Product);                 // Stream<string>
```

Available patterns include:

* `Map` / `MapAwait`
* `Filter` / `FilterAwait`
* `FlatMap` for 1:1 async project → like `SelectMany`
* `FlatMapMany` for 1:N expansion → flatten multiple sequences
* `FlatMapAwait` / `FlatMapManyAwait` for async selector functions

---

## ⚙️ Concurrency & Backpressure

```csharp
await stream
    .ParallelMap(async x => await ProcessAsync(x), maxConcurrency: 5)
    .ForEachAsync(Console.WriteLine);
```

Use:

- `ParallelMap(...)` when completion order can vary
- `ParallelMapOrdered(...)` when upstream order must be preserved
- `FlatMap(..., maxConcurrency: n)` and `FlatMapMany(..., maxConcurrency: n)` for concurrent flattening

When Streamix uses bounded channels internally, producers pause when buffers are full instead of unboundedly accumulating work.

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

* `Map` / `MapAwait`
* `Filter` / `FilterAwait`
* `FlatMap` / `FlatMapAwait`
* `Generate`
* `FlatMapMany` / `FlatMapManyAwait`
* `ParallelMap`, `ParallelMapOrdered`
* `Take` / `Skip`
* `Merge` / `Zip`
* `Buffer` / `Window`
* `Throttle` / `Delay`
* `Retry` / `Retry(..., backoffStrategy)` / `Timeout`
* `OnErrorResume` / `OnErrorReturn` / `OnErrorMap`
* `Publish` / `Replay` / `RefCount`
* `RunOn`
* `DoOnNext`, `Do`, `Tap`, `DoOnError`, `DoOnComplete`, `DoOnTerminate`

`IStream<T>` includes `ForEachAsync(...)` and channel output. Additional terminal operators are available through extension methods:

* `ToListAsync`, `ToArrayAsync`, `ToHashSetAsync`, `ToDictionaryAsync`
* `FirstAsync` / `LastAsync` (and `OrDefault` variants)
* `SingleAsync` (and `OrDefault` variant)
* `AggregateAsync` / `CountAsync` / `AnyAsync` / `AllAsync`
* `MinAsync` / `MaxAsync`
* `SumAsync` / `AverageAsync`

`ISingle<T>` also supports `ToTask()`.

---

## 🔗 LINQ & Query Syntax Support

Streamix supports both **fluent and query comprehension syntax**:

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
    .SelectManyAsync(async x => await GetStream(x), maxConcurrency: 3)
    .ToListAsync();
```

**Available:** `Where`, `Select`, `SelectMany` (sync) and `WhereAsync`, `SelectAsync`, `SelectManyAsync` (async)

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

### `Stream.From` / `Single.From` / `Just`
Shorthands for values, Tasks, and Async Enumerables.

*   **Eager vs. Lazy**:
    *   `From(Task<T>)` wraps existing, already-started work (**eager**). If the task is already completed, the stream will emit the result immediately upon subscription.
    *   `From(Func<Task<T>>)` and `From(Func<CancellationToken, Task<T>>)` defer the creation and start of the task until the stream is subscribed to (**lazy**). The factory is called once per subscriber.
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

- **Backpressure by Design**: Concurrent operators like `FlatMap`, `Merge`, and `ParallelMap` utilize bounded `System.Threading.Channels`. This ensures that if a consumer is slower than the producer, the producer is naturally paused once the internal buffers are full, preventing unbounded memory growth.
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
