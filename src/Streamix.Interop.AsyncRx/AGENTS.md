# Streamix.Interop.AsyncRx

This package provides interoperability between Streamix and [AsyncRx.NET](https://github.com/dotnet/reactive).

## Maturity and Dependency Isolation

As of early 2025, AsyncRx.NET (System.Reactive.Async) is still in **experimental preview/alpha** status.

To prevent destabilizing the core Streamix package and to avoid forcing a dependency on a preview library, all AsyncRx-related functionality is isolated within this project.

### Design Decisions

1.  **Separate Assembly**: Interop is provided in a separate assembly (`Streamix.Interop.AsyncRx.dll`) so that users only take the dependency if they explicitly need it.
2.  **Extension-Based API**: Methods like `ToAsyncObservable()`, `ToStream()`, and `ToSingle()` are implemented as extension methods to maintain a clean separation from the core `IStream<T>` and `ISingle<T>` interfaces.
3.  **Push-Pull Bridge**: The bridge uses `System.Threading.Channels` for efficient and backpressure-aware conversion between the pull-based `IAsyncEnumerable<T>` (used by Streamix) and the push-based `IAsyncObservable<T>` (used by AsyncRx.NET).
