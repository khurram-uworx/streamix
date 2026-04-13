# Streamix

Idiomatic reactive streams for .NET.

Streamix gives you a fluent, async-first stream model on top of `IAsyncEnumerable<T>` with explicit semantics for composition, concurrency, ordering, cancellation, and backpressure.

## Installation

```bash
dotnet add package Streamix
```

## What You Get

- `Stream<T>` for 0..N values and `Single<T>` for 0..1 values
- Fluent operators for filtering, mapping, flattening, timing, retries, and recovery
- Explicit concurrency and ordering control
- Cold streams by default, with hot-stream primitives such as `Publish`, `Replay`, and `RefCount`
- LINQ/query syntax support for the common sequential surface
- Interop with `IAsyncEnumerable<T>`, channels, and an optional AsyncRx.NET extensions package

## Basic Example

```csharp
await Stream.Range(1, 10)
    .Filter(x => x % 2 == 0)
    .Map(x => x * 10)
    .ForEachAsync(Console.WriteLine);
```

## A Little More

```csharp
var products =
    GetUser(id)                       // Single<User>
    .FlatMap(user => GetOrders(user)) // Stream<Order>
    .Map(o => o.Product);             // Stream<string>
```

Common patterns:

- `Map` / `MapAwait` / `MapOrdered`
- `Filter` / `FilterAwait`
- `FlatMap` / `FlatMapAwait` / `FlatMapOrdered` / `ConcatMap`
- `Publish` / `Replay` / `RefCount`
- `Retry` / `Timeout` / `OnErrorResume`
- `ToListAsync`, `CountAsync`, `FirstAsync`, `SingleAsync`

Streamix keeps concurrency, ordering, hot/cold behavior, and backpressure explicit instead of implicit.

## Learn More

- Overview and package map: [README.md](https://github.com/khurram-uworx/streamix/blob/main/README.md)
- Developer guide: [GETTING-STARTED.md](https://github.com/khurram-uworx/streamix/blob/main/GETTING-STARTED.md)
- Architecture and design notes: [ARCHITECTURE.md](https://github.com/khurram-uworx/streamix/blob/main/ARCHITECTURE.md)
- Repository: [github.com/khurram-uworx/streamix](https://github.com/khurram-uworx/streamix)

## Status

Streamix is still early-stage, but this package README is intended to describe the shipped core surface only.
Use the root repository README for the fuller product contract, roadmap, and release checklist.
