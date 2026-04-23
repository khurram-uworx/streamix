# Streamix

> Idiomatic reactive streams for .NET.
> Fluent, async-first, and built around `IAsyncEnumerable<T>` rather than around framework magic.

Streamix brings a composable stream model to modern .NET with explicit semantics for concurrency, ordering, cancellation, errors, and backpressure. It is inspired by Reactor, but the shape is deliberately .NET-native.

## Why It Exists

Modern .NET gives us `IAsyncEnumerable<T>` and channels, but it still leaves a gap between low-level primitives and a fluent stream abstraction.

Streamix fills that gap with:

- `Stream<T>` for 0..N values
- `Single<T>` for 0..1 values
- declarative operators for mapping, filtering, flattening, timing, retries, and recovery
- hot-stream primitives such as `Publish`, `Replay`, and `RefCount`
- interop with `IAsyncEnumerable<T>`, channels, AsyncRx.NET, and ASP.NET Core streaming

The default mental model is simple:

- cold, pull-based streams built on `IAsyncEnumerable<T>`
- channels only when coordination or fan-out is needed
- explicit async composition, cancellation, ordering, and error propagation

## Quick Taste

```csharp
await Stream.Range(1, 10)
    .Named("MyStream")
    .Log()
    .Filter(x => x % 2 == 0)
    .Map(x => x * 10)
    .ForEachAsync(Console.WriteLine);
```

```csharp
var products =
    GetUser(id)                       // Single<User>
    .FlatMap(user => GetOrders(user)) // Stream<Order>
    .Map(o => o.Product);             // Stream<string>
```

Streamix provides several operators to help you observe and debug your reactive pipelines.

```csharp
await Stream.Range(1, 100)
    .Named("Orders")
    .Trace()
    .Checkpoint("ProcessStart")
    .Map(async x => await ProcessAsync(x), maxConcurrency: 5)
    .Checkpoint("ProcessEnd")
    .ForEachAsync(Console.WriteLine);
```

Streamix supports both fluent and query comprehension syntax.

```csharp
var result = await (
    from x in Stream.Range(1, 10)
    where x % 2 == 0
    select x * 10
).ToListAsync();
```

Streamix supports event-time windowing with tumbling and sliding windows.

```csharp
await sensorStream
    .MapWithTimestamp(s => s.ObservedAt)
    .WindowByTime(
        duration: TimeSpan.FromMinutes(5),
        slide: TimeSpan.FromMinutes(1),
        outOfOrderness: TimeSpan.FromSeconds(30))
    .FlatMap(window => window.MaxAsync(s => s.Value))
    .ForEachAsync(Console.WriteLine);
```

Streamix provides a first-class structured concurrency model via `Stream.ScopedAsync`. It ensures that concurrent tasks have well-defined lifetimes, clear parent-child relationships, and predictable fail-fast semantics.

```csharp
await Stream.ScopedAsync(async scope =>
{
    scope.Run(async ct =>
    {
        await Task.Delay(100, ct);
        // Concurrent work...
    });

    // The scope waits for all registered tasks to settle
    // and propagates the first non-cancellation exception.
});
```

Entity Framework integration is provided by `Streamix.Extensions` via `EfStream`.

```csharp
await EfStream.From(
        ctx => ctx.Set<Customer>().Where(c => c.IsActive),
        () => new AppDbContext())
    .ForEachAsync(customer => Console.WriteLine(customer.Name));
```

`EfStream.From(...)` and `ToStream(...)` keep the buffered default contract. `EfStream.FromStreamed(...)` and `ToStreamed(...)` provide explicit opt-in streamed enumeration when you want row-by-row EF async enumeration semantics.

For streamed EF queries, ordering, cancellation timing, and error timing remain provider-sensitive; the package README and getting-started guide document those caveats explicitly.

The EF integration remains factory-based; caller-owned `DbContext` overloads are intentionally not part of the public API.

EF-specific batching or paging helpers are not currently part of the contract; the guidance is to choose buffered versus streamed execution deliberately before adding application-level batching.

## Documentation

- [GETTING-STARTED.md](GETTING-STARTED.md): Hello World, core concepts, feature surface, operators, interop, and package usage
- [ARCHITECTURE.md](ARCHITECTURE.md): design principles, behavioral semantics, implementation notes, and performance characteristics

### Blog Series

- [Streamix: A Stream Library for Modern .NET](https://khurram-uworx.github.io/2026/04/04/Streamix.html)
- [Streamix: The Core Mental Model](https://khurram-uworx.github.io/2026/04/05/Streamix2.html)
- [Hot vs Cold Streams, Ordering, and Async Composition in Streamix](https://khurram-uworx.github.io/2026/04/11/Streamix3.html)
- [Backpressure, Interop, and Streaming ASP.NET Core Responses With Streamix](https://khurram-uworx.github.io/2026/04/12/Streamix4.html)

## Packages

- [`Streamix`](https://www.nuget.org/packages/Streamix): core stream types, operators, terminals, channels, and sinks
- [`Streamix.Extensions`](https://www.nuget.org/packages/Streamix.Extensions): optional integrations (AsyncRx.NET interop and Entity Framework `EfStream`), isolated from the core package
- [`Streamix.AspNetCore`](https://www.nuget.org/packages/Streamix.AspNetCore): SSE, WebSocket, and HTTP response streaming integration for ASP.NET Core

## Status

Streamix is currently in active development. Core features including structured concurrency, time-series windowing, and channel-backed execution boundaries are implemented and verified.

**Roadmap**

- Additional time-based operators
- Source generators for optimized pipelines

## Contributing

- Keep API fluent and minimal
- Focus on async-first idioms
- Backpressure awareness is required for stream operators

## License

MIT
