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

## Packages

- [`Streamix`](https://www.nuget.org/packages/Streamix): core stream types, operators, terminals, channels, and sinks
- [`Streamix.Extensions`](https://www.nuget.org/packages/Streamix.Extensions): AsyncRx.NET interop, isolated from the core package
- [`Streamix.AspNetCore`](https://www.nuget.org/packages/Streamix.AspNetCore): SSE, WebSocket, and HTTP response streaming integration for ASP.NET Core

## Documentation

- [GETTING-STARTED.md](GETTING-STARTED.md): Hello World, core concepts, feature surface, operators, interop, and package usage
- [ARCHITECTURE.md](ARCHITECTURE.md): design principles, behavioral semantics, implementation notes, and performance characteristics

## Blog Series

- [Streamix: A Stream Library for Modern .NET](https://khurram-uworx.github.io/2026/04/04/Streamix.html)
- [Streamix: The Core Mental Model](https://khurram-uworx.github.io/2026/04/05/Streamix2.html)
- [Hot vs Cold Streams, Ordering, and Async Composition in Streamix](https://khurram-uworx.github.io/2026/04/11/Streamix3.html)
- [Backpressure, Interop, and Streaming ASP.NET Core Responses With Streamix](https://khurram-uworx.github.io/2026/04/12/Streamix4.html)

## Status

Streamix is still early-stage. The repository docs describe the intended product contract, but they should not be read as claiming every aspirational roadmap item is already implemented.

## License

MIT
