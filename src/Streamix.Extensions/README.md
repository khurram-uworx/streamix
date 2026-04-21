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

Explicit streamed enumeration is also available:

```csharp
await EfStream.FromStreamed(
        ctx => ctx.Set<Customer>().Where(c => c.IsActive),
        () => new AppDbContext())
    .Take(100)
    .ForEachAsync(customer => Console.WriteLine(customer.Name));
```

Important semantics:

- Query construction and execution use the same `DbContext` instance per subscription.
- `EfStream.From(...)` and `ToStream(...)` materialize with `ToListAsync` before yielding items downstream.
- `EfStream.FromStreamed(...)` and `ToStreamed(...)` yield items as EF async enumeration advances.
- `Streamix.Extensions` includes EF Core as a transitive dependency by design.
- The package currently exposes factory-based overloads only; caller-owned `DbContext` overloads are intentionally not part of the shipped API.
- Buffered execution remains the default contract on `EfStream.From(...)` and `ToStream(...)`.
- Streamed execution is available only through the separate explicit opt-in entry points `EfStream.FromStreamed(...)` and `ToStreamed(...)`.

### Buffered vs Streamed Guidance

- Prefer buffered mode for ordinary application queries when you want simple timing semantics and deterministic "all query work finishes before first item" behavior.
- Prefer streamed mode when early consumption matters, when downstream operators such as `Take` may stop early, or when you want to avoid full pre-materialization before the first item is observed.
- Both modes create and dispose one `DbContext` per subscription. Streamed mode keeps that context alive until enumeration completes, fails, or is cancelled.

### Provider Caveats For Streamed Execution

Streamed EF execution is provider-sensitive. Streamix does not guarantee identical behavior across all EF providers.

- Ordering: Streamed mode preserves the order produced by the underlying EF query, but not an implied order. If order matters, specify it in the query with `OrderBy(...)`.
- Cancellation timing: Buffered mode observes cancellation during `ToListAsync(...)`. Streamed mode can observe cancellation between rows or async reads, but some providers may only surface cancellation after additional work has already occurred.
- Error timing: Buffered mode usually fails before the first downstream item because materialization happens up front. Streamed mode can fail after emitting a partial prefix of results because errors may surface during enumeration.
- Resource lifetime: Streamed mode keeps the query reader and `DbContext` alive while the consumer is still iterating. Slow consumers therefore hold provider resources longer than buffered mode.

### Usage Guidance

- Use buffered mode for request/response style reads, small-to-medium result sets, and cases where "all-or-fail before first item" is easier to reason about.
- Use streamed mode for large result sets, early-exit consumers, and long-running reads where first-item latency matters more than eager materialization.
- Do not assume provider-uniform cancellation or fault timing in streamed mode. Validate behavior against the provider you plan to ship.
- Streamix does not currently add EF-specific batching, paging, or chunking helpers. Use the existing buffered-versus-streamed choice first, then apply general Streamix operators such as `Buffer(...)` only when application behavior actually justifies it.

### Caller-Owned Context Decision

- Streamix does not provide public overloads that accept a caller-owned `DbContext`.
- The supported contract remains factory-based so that each subscription has clear context ownership, disposal, and same-context query execution semantics.
- Transaction or unit-of-work coordination with a caller-owned context is intentionally left as an external application pattern rather than part of the Streamix EF API surface.

## Concurrency Integration

Extension-provided streams such as `EfStream` participate in the same core supervision model as standard streams, ensuring consistent resource safety and cancellation behavior when used with `ScopedAsync` or concurrent operators.

## Learn More

- Overview and package map: [README.md](https://github.com/khurram-uworx/streamix/blob/main/README.md)
- Developer guide: [GETTING-STARTED.md](https://github.com/khurram-uworx/streamix/blob/main/GETTING-STARTED.md)
- Architecture and design notes: [ARCHITECTURE.md](https://github.com/khurram-uworx/streamix/blob/main/ARCHITECTURE.md)
- Repository: [github.com/khurram-uworx/streamix](https://github.com/khurram-uworx/streamix)
