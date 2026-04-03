# Streamix — Idiomatic Reactive Streams for .NET

> A lightweight, fluent, async-first streaming library for .NET.  
> Inspired by Project Reactor, designed natively for C#.

---

## ✨ Why Streamix?

Modern .NET has `IAsyncEnumerable<T>` and Channels, but we lack a **unified, composable streaming abstraction** that:

- Supports **0..N item streams** (`Stream<T>`) and **0..1 item streams** (`Single<T>`)  
- Handles **backpressure naturally**  
- Provides **declarative, chainable operators** like Reactor  
- Integrates with **AsyncRx.NET** or raw `IAsyncEnumerable<T>`  

**Streamix bridges that gap** — without Rx-style complexity.

---

## 🧩 Core Types

### `Stream<T>`
- 0..N items
- Built on `IAsyncEnumerable<T>` internally
- Async push/pull via `Channel<T>` when needed

```csharp
Stream<int> numbers = Stream.Range(1, 10);
````

### `Single<T>`

* 0..1 item
* Wraps `Task<T>` or a single async result

```csharp
Single<User> user = GetUser(id);
```

---

## 🚀 Example: Basic Pipeline

```csharp
await Stream.Range(1, 10)
    .Filter(x => x % 2 == 0)
    .Map(x => x * 10)
    .ForEachAsync(Console.WriteLine);
```

---

## 🔄 Async Composition

```csharp
var orders =
    GetUser(id)                           // Single<User>
    .FlatMapMany(user => GetOrders(user)) // Stream<Order>
    .Map(o => o.Product);                 // Stream<string>
```

* `FlatMap` → like `SelectMany`
* `FlatMapMany` → flatten multiple sequences

---

## ⚙️ Concurrency & Backpressure

```csharp
await stream
    .FlatMap(async x => await Process(x), maxConcurrency: 5)
    .ForEachAsync(Console.WriteLine);
```

* `maxConcurrency` controls simultaneous processing
* Backpressure is automatic via bounded Channels. When the consumer is slow, the producer is paused once the `maxConcurrency` buffer is full.

---

## 🧩 Hot vs Cold Streams

```csharp
var cold = Stream.Range(1,3);  // cold by default
var hot  = cold.Publish().RefCount(); // shared hot stream
```

---

## 📦 Core Operators

* `Map` / `Select` / `MapAwait`
* `Filter` / `Where` / `FilterAwait`
* `FlatMap` / `SelectMany` / `FlatMapAwait`
* `FlatMapMany` / `FlatMapManyAwait`
* `Merge` / `Zip`
* `Buffer` / `Window`
* `Throttle` / `Delay`
* `Retry` / `Timeout`
* `Take` / `Skip`
* `FirstAsync` / `LastAsync` / `SingleAsync` (and `OrDefault` variants)
* `AggregateAsync` / `CountAsync` / `AnyAsync` / `AllAsync`

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

### From AsyncRx.NET

```csharp
using Streamix.Extensions;

IStream<int> stream = asyncObservable.ToStream();
```

### To AsyncRx.NET

```csharp
using Streamix.Extensions;

IAsyncObservable<int> obs = stream.ToAsyncObservable();
```

### From `IAsyncEnumerable<T>`

```csharp
Stream<int> stream = Stream.From(asyncEnumerable);
```

---

## 🧵 Execution

* Runs on caller context by default
* Optional scheduler control:

```csharp
stream.RunOn(TaskScheduler.Default);
```

---

## ⚠️ Error Handling

```csharp
stream
    .Map(...)
    .OnErrorResume(ex => Stream.Empty<int>());
```

---

## 📌 Design Principles

1. Async-first (`IAsyncEnumerable<T>` + Channels)
2. Pull by default, push when needed
3. Declarative & fluent API
4. Interoperable with AsyncRx.NET
5. Minimal, .NET-idiomatic operators
6. Backpressure-aware automatically

---

## 🛠️ Implementation Notes

* Channels for flow control
* Lightweight operator chaining
* No reflection or heavy runtime magic
* Fully compatible with async streams

---

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
