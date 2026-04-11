# Streamix

Streamix is a lightweight, fluent, async-first streaming library for .NET.

It brings a .NET-idiomatic reactive stream model on top of `IAsyncEnumerable<T>` with explicit support for async composition, concurrency, ordering, cancellation, and backpressure-aware operators.

## Installation

```bash
dotnet add package Streamix
```

## Why Streamix?

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

## Async Composition

```csharp
var products =
    GetUser(id)                       // Single<User>
    .FlatMap(user => GetOrders(user)) // Stream<Order>
    .Map(o => o.Product);             // Stream<string>
```

Common patterns include:

- `Map` / `MapAwait` for sequential ordered 1:1 transforms
- `Map(Func<T, Task<TResult>>, maxConcurrency)` for concurrent unordered 1:1 transforms
- `MapOrdered` for concurrent ordered 1:1 transforms
- `Filter` / `FilterAwait`
- `FlatMap` / `FlatMapAwait` for concurrent flattening
- `ConcatMap` for sequential ordered flattening
- `FlatMapOrdered` for concurrent ordered flattening

## Concurrency and Backpressure

Streamix makes concurrency and ordering explicit:

- `Map(Func<T, TResult>)` and `MapAwait(...)` are sequential and ordered
- `Map(Func<T, Task<TResult>>, int maxConcurrency = int.MaxValue)` is concurrent and unordered
- `MapOrdered(...)` is concurrent and ordered
- `FlatMap(...)` is unordered concurrent flattening by default

```csharp
await stream
    .Map(async x => await ProcessAsync(x), maxConcurrency: 5)
    .ForEachAsync(Console.WriteLine);
```

Backpressure strategies are also available when you want explicit overflow handling:

- `OnBackpressureBuffer(capacity)`
- `OnBackpressureDrop()`
- `OnBackpressureLatest()`
- `OnBackpressureError()`

## Hot Streams

Streams are cold by default. Use hot-stream primitives when you want sharing:

```csharp
var shared = Stream.Range(1, 3)
    .Publish()
    .RefCount();
```

## Key Operators

- `Map`, `MapAwait`, `MapOrdered`
- `Filter`, `FilterAwait`
- `FlatMap`, `FlatMapAwait`, `FlatMapOrdered`, `ConcatMap`
- `FromChannel`, `Take`, `Skip`, `Merge`, `MergeChannels`, `Zip`
- `Buffer`, `Window`
- `FromTimer`, `Interval`, `Poll`, `Never`
- `Retry`, `Timeout`, `OnErrorResume`, `OnErrorReturn`, `OnErrorMap`
- `Publish`, `Replay`, `RefCount`
- `ToListAsync`, `ToArrayAsync`, `CountAsync`, `FirstAsync`, `SingleAsync`, `SingleOrDefaultAsync`

## Learn More

- Full documentation and API notes: [README.md](https://github.com/khurram-uworx/streamix/blob/main/README.md)
- Repository: [github.com/khurram-uworx/streamix](https://github.com/khurram-uworx/streamix)

## Status

Streamix is still early-stage, but this package README is intended to describe the shipped core surface only.
Use the root repository README for the fuller product contract, roadmap, and release checklist.
