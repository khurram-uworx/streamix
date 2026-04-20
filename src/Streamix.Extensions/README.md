# Streamix.Extensions

Optional integrations for Streamix.

This package hosts integration features that are intentionally isolated from the core `Streamix` package:

- AsyncRx.NET interop via [AsyncRx.NET](https://github.com/dotnet/reactive)
- Entity Framework Core stream factories via `EfStream`

## Maturity and Dependency Isolation

As AsyncRx.NET (System.Reactive.Async) is still in **experimental preview/alpha** status.

To prevent destabilizing the core Streamix package and to avoid forcing a dependency on a preview library, all AsyncRx-related functionality is isolated within this project.

## Design Decisions

1. **Separate Assembly**: Interop is provided in a separate assembly (`Streamix.Extensions.dll`) so that users only take the dependency if they explicitly need it.
2. **Extension-Based API**: Methods like `ToAsyncObservable()`, `ToStream()`, and `ToSingle()` are implemented as extension methods to maintain a clean separation from the core `IStream<T>` and `ISingle<T>` interfaces.
3. **Push-Pull Bridge**: The bridge uses `System.Threading.Channels` for efficient and backpressure-aware conversion between the pull-based `IAsyncEnumerable<T>` used by Streamix and the push-based `IAsyncObservable<T>` used by AsyncRx.NET.

## Transitive Dependencies

`Streamix.Extensions` intentionally carries a wider dependency graph than core `Streamix`:

- AsyncRx support adds AsyncRx-related dependencies.
- EF stream support adds `Microsoft.EntityFrameworkCore`.

If you need only core stream operators, reference `Streamix` directly.

## Entity Framework Integration

Use `EfStream.From(...)` (or the `ToStream(...)` extension on a `DbContext` factory) to execute EF queries as Streamix streams.

```csharp
await EfStream.From(
        ctx => ctx.Set<Customer>().Where(c => c.IsActive),
        () => new AppDbContext())
    .Take(100)
    .ForEachAsync(customer => Console.WriteLine(customer.Name));
```

Equivalent fluent factory-extension style:

```csharp
await (() => new AppDbContext()).ToStream(
        ctx => ctx.Set<Customer>().Where(c => c.IsActive))
    .ForEachAsync(customer => Console.WriteLine(customer.Name));
```

Important semantics:

- Query construction and execution use the same `DbContext` instance per subscription.
- v1 execution materializes with `ToListAsync` before yielding items downstream.
- `Streamix.Extensions` includes EF Core as a transitive dependency by design.
- The package currently exposes factory-based overloads; caller-owned context overloads are not part of the shipped API.

## Concurrency Integration

Extension-provided streams such as `EfStream` participate in the same core supervision model as standard streams, ensuring consistent resource safety and cancellation behavior when used with `ScopedAsync` or concurrent operators.

## Learn More

- Overview and package map: [README.md](https://github.com/khurram-uworx/streamix/blob/main/README.md)
- Developer guide: [GETTING-STARTED.md](https://github.com/khurram-uworx/streamix/blob/main/GETTING-STARTED.md)
- Architecture and design notes: [ARCHITECTURE.md](https://github.com/khurram-uworx/streamix/blob/main/ARCHITECTURE.md)
- Repository: [github.com/khurram-uworx/streamix](https://github.com/khurram-uworx/streamix)
