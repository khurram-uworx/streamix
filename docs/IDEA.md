# Streamix — Reactive Streams for .NET (Without the Rx Baggage)

> A minimal, idiomatic .NET library for composable, backpressure-aware async streams.
> Inspired by Project Reactor, built on `IAsyncEnumerable<T>` and `Channel<T>`.

---

## ✨ Why Streamix?

Modern .NET gives us powerful primitives:

* `async/await`
* `IAsyncEnumerable<T>`
* `Channel<T>`

But what's missing is a **unified, composable streaming abstraction** that provides:

* Declarative pipelines (like Reactor / Rx)
* Built-in backpressure
* Controlled concurrency
* End-to-end async flow composition

**Streamix fills that gap — without abandoning .NET idioms.**

---

## 🧠 Design Principles

* ✅ Async-first (`async/await` everywhere)
* ✅ Pull-based by default (`IAsyncEnumerable<T>`)
* ✅ Push when needed (`Channel<T>`)
* ✅ Backpressure-aware
* ✅ Minimal API surface
* ❌ No observer pattern
* ❌ No dual-type confusion (`Observable` vs `Flowable`)
* ❌ No scheduler obsession

---

## 📦 Core Types

### `Stream<T>`

Represents a stream of 0..N values.

```csharp
Stream<int> stream = Stream.Range(1, 10);
```

Backed by `IAsyncEnumerable<T>`.

---

### `Single<T>`

Represents 0..1 value (like `Task<T>` or Mono).

```csharp
Single<User> user = GetUser(id);
```

---

## 🚀 Quick Examples

### Transformations

```csharp
var result =
    Stream.Range(1, 10)
        .Filter(x => x % 2 == 0)
        .Map(x => x * 10);

await result.ForEachAsync(Console.WriteLine);
```

---

### Async Composition

```csharp
var result =
    GetUser(id)
        .FlatMap(user => GetOrders(user))
        .FlatMapMany(orders => Stream.From(orders));
```

---

### Concurrency Control

```csharp
await stream
    .FlatMap(async x => await Process(x), maxConcurrency: 10)
    .ForEachAsync(Console.WriteLine);
```

---

### Backpressure (Implicit)

```csharp
var stream = Stream.Create<int>(async writer =>
{
    for (int i = 0; i < 1_000_000; i++)
    {
        await writer.WriteAsync(i); // respects downstream pressure
    }
});
```

---

### Buffering

```csharp
await stream
    .Buffer(100)
    .ForEachAsync(batch => ProcessBatch(batch));
```

---

### Merging Streams

```csharp
var merged = Stream.Merge(stream1, stream2);
```

---

### Time-based Operators

```csharp
await stream
    .Throttle(TimeSpan.FromMilliseconds(200))
    .ForEachAsync(Console.WriteLine);
```

---

## 🔁 Hot vs Cold Streams

### Cold (default)

```csharp
var stream = Stream.Range(1, 3);
```

Each consumer gets full sequence.

---

### Hot (shared)

```csharp
var hot = stream.Publish().RefCount();
```

Shared execution across subscribers.

---

## ⚙️ Backpressure Model

Streamix uses:

* `Channel<T>` internally (bounded by default)
* Await-based flow control
* No explicit `request(n)` — demand flows naturally via awaits

---

## 🧵 Threading Model

* No implicit thread switching
* Runs on caller context unless specified

```csharp
stream.RunOn(TaskScheduler.Default);
```

---

## ❗ Error Handling

```csharp
stream
    .Map(...)
    .OnErrorResume(ex => Stream.Empty<int>());
```

---

## 🔌 Interop

### From `IAsyncEnumerable<T>`

```csharp
Stream.From(asyncEnumerable);
```

### To `IAsyncEnumerable<T>`

```csharp
await foreach (var item in stream)
```

---

## 🧩 Operator Set (Core)

* `Map`
* `Filter`
* `FlatMap`
* `Merge`
* `Zip`
* `Buffer`
* `Window`
* `Retry`
* `Timeout`
* `Throttle`
* `Take`
* `Skip`

---

## 🆚 Comparison

| Feature      | Streamix | Rx.NET | Raw .NET |
| ------------ | -------- | ------ | -------- |
| Async-first  | ✅        | ❌      | ✅        |
| Backpressure | ✅        | ⚠️     | ⚠️       |
| Simplicity   | ✅        | ❌      | ✅        |
| Composition  | ✅        | ✅      | ❌        |
| Ecosystem    | 🚧       | Mature | Native   |

---

## 🎯 When to Use

Use Streamix when:

* You need high-throughput pipelines
* You want declarative async flows
* You need concurrency control
* You're building streaming systems

---

## 🚫 When NOT to Use

* Simple request/response logic → use `async/await`
* CPU-bound loops → use `Parallel` / PLINQ

---

## 🛠️ Implementation Notes

* Built on `IAsyncEnumerable<T>`
* Channels for coordination
* Minimal allocations
* No reflection / no magic

---

## 🧭 Roadmap

* [ ] Structured concurrency integration
* [ ] Source generators for pipelines
* [ ] ASP.NET Core integration
* [ ] Diagnostic hooks

---

## 🤝 Contributing

PRs welcome — keep it small, composable, and idiomatic.

---

## 📜 License

MIT
