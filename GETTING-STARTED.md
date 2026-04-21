# Getting Started with Streamix

Streamix is a lightweight, fluent, async-first streaming library for .NET.

It brings a .NET-idiomatic reactive stream model on top of `IAsyncEnumerable<T>` with explicit support for async composition, concurrency, ordering, cancellation, and backpressure-aware operators.

## Why Streamix?

- `Stream<T>` for 0..N values and `Single<T>` for 0..1 values
- Fluent operators for filtering, mapping, flattening, timing, retries, and recovery
- Explicit concurrency and ordering control
- Cold streams by default, with hot-stream primitives such as `Publish`, `Replay`, and `RefCount`
- LINQ/query syntax support for the common sequential surface
- Interop with `IAsyncEnumerable<T>`, channels, and an optional AsyncRx.NET extensions package

### `Stream<T>`

Represents a stream of 0..N values.

```csharp
IStream<int> numbers = Stream.Range(1, 10);
```

### `Single<T>`

Represents a stream of 0..1 values.

```csharp
ISingle<User> user = Single.From(GetUser(id));
```

`Single.From(...)` supports values, `Task<T>`, `ValueTask<T>`, and `IAsyncEnumerable<T>` sources.

## Hello World

```csharp
await Stream.Range(1, 10)
    .Filter(x => x % 2 == 0)
    .Map(x => x * 10)
    .ForEachAsync(Console.WriteLine);
```

## Async Composition

```csharp
var orders =
    GetUser(id)                           // Single<User>
    .FlatMap(user => GetOrders(user))     // Stream<Order>
    .Map(o => o.Product);                 // Stream<string>
```

Available patterns include:

- `Map` / `MapAwait` - 1:1 transforms, sequential and ordered
- `Map(Func<T, Task<TResult>>, maxConcurrency)` - 1:1 transform, concurrent and unordered
- `MapOrdered` - 1:1 transform, concurrent and ordered
- `Filter` / `FilterAwait`
- `FlatMap` / `FlatMapAwait` - 1:1 or 1:N transforms, unordered concurrent by default
- `ConcatMap` - 1:N transform, sequential and ordered
- `FlatMapOrdered` - 1:N transform, concurrent and ordered

## Concurrency and Backpressure

Streamix provides explicit control over concurrency and ordering.

- `Map(Func<T, TResult>)` and `MapAwait(Func<T, ValueTask<TResult>>)` are sequential and ordered.
- `Map(Func<T, Task<TResult>>, int maxConcurrency = int.MaxValue)` is concurrent and unordered.
- `MapOrdered(Func<T, Task<TResult>>, int maxConcurrency)` is concurrent and ordered.
- `FlatMap` and `FlatMapAwait` are the unordered concurrent flattening operators by default.

```csharp
await stream
    .Map(async x => await ProcessAsync(x), maxConcurrency: 5)
    .ForEachAsync(Console.WriteLine);
```

### Concurrency Semantics

| Operator | Concurrency | Ordering | Use Case | Performance |
|----------|-------------|----------|----------|-------------|
| `Map(Func<T, TResult>)` | 1 | Ordered | Synchronous projection with minimal overhead | ⭐ |
| `MapAwait(Func<T, ValueTask<TResult>>)` | 1 | Ordered | Async projection when each item must complete before the next advances | ⭐ |
| `Map(Func<T, Task<TResult>>, ...)` | Configurable N, default unbounded | Unordered | Highest-throughput async 1:1 transform | ⭐⭐⭐ |
| `MapOrdered()` | Configurable N | Ordered | Async transform while preserving source order | ⭐⭐ |
| `FlatMap()` | Unbounded | Unordered | Fire-and-forget, fastest 1:N expansion | ⭐⭐⭐ |
| `FlatMapOrdered()` | Configurable N | Ordered | Expand while preserving source order with explicit per-inner buffering | ⭐⭐ |
| `ConcatMap()` | 1 (Sequential) | Ordered | Strict sequential processing | ⭐ |

When Streamix uses bounded channels internally, producers pause when buffers are full instead of unboundedly accumulating work.

### Backpressure Strategies

While Streamix provides implicit backpressure, you can explicitly control how to handle overflow when a producer outpaces a consumer.

- `OnBackpressureBuffer(capacity)`: Buffers items up to capacity; throws `BackpressureException` on overflow.
- `OnBackpressureDrop()`: Discards new items when the consumer is busy.
- `OnBackpressureLatest()`: Keeps only the most recent item, discarding intermediate ones.
- `OnBackpressureError()`: Immediately fails with `BackpressureException` if the consumer cannot keep up.

If you chain multiple backpressure operators, they compose as nested stream boundaries. Earlier strategies may already have dropped items or failed before a later strategy sees them.

```csharp
await stream.OnBackpressureBuffer(100).ForEachAsync(ProcessAsync);
await metrics.OnBackpressureDrop().ForEachAsync(LogAsync);
await state.OnBackpressureLatest().ForEachAsync(UpdateUIAsync);
await stream.OnBackpressureError().ForEachAsync(CriticalProcessAsync);
```

## Structured Concurrency

Streamix implements a structured concurrency model that ensures concurrent operations have well-defined lifetimes and predictable failure/cancellation semantics.

### `Stream.ScopedAsync`

The primary entry point for structured concurrency is `Stream.ScopedAsync`. It creates a supervision boundary that waits for all spawned tasks to complete before returning.

```csharp
await Stream.ScopedAsync(async scope =>
{
    scope.Run(async ct =>
    {
        await Task.Delay(100, ct);
        Console.WriteLine("Task 1 done");
    });

    scope.Run(async ct =>
    {
        await Task.Delay(50, ct);
        Console.WriteLine("Task 2 done");
    });
});
// The scope completes only after all tasks settle.
```

### Fail-Fast Semantics

If any task within the scope fails, the entire scope is cancelled (sibling cancellation), but it still waits for all remaining tasks to settle before propagating the first observed non-cancellation exception.

This model is also integrated into concurrent operators like `FlatMap`, `MapOrdered`, and `RunOnChannel`, ensuring that child tasks never "escape" their parent operator's lifetime.

## Hot vs Cold Streams

Streams are cold by default: each subscriber re-enumerates the source. `Publish()` turns a cold stream into a connectable shared stream.

```csharp
var cold = Stream.Range(1, 3);
var hot = source.Publish();

using var connection = hot.Connect();
await hot.ForEachAsync(Console.WriteLine);
```

Use `RefCount()` to auto-connect on the first subscriber and disconnect when the last subscriber leaves.

```csharp
var shared = Stream.Range(1, 3).Publish().RefCount();
```

Use `Replay(...)` to share the source and replay the most recent items to late subscribers.

```csharp
var replayed = Stream.Range(1, 3).Replay(2);
```

## Operators

- `Map` / `MapAwait` / `MapOrdered`
- `Filter` / `FilterAwait`
- `FlatMap` / `FlatMapAwait`
- `ConcatMap` / `FlatMapOrdered`
- `Generate`
- `Take` / `Skip`
- `Merge` / `MergeChannels` / `Zip`
- `Buffer` / `Window`
- `Never` / `FromTimer` / `Poll`
- `Throttle` / `Delay`
- `Retry` / `Retry(..., backoffStrategy)` / `Timeout`
- `OnErrorResume` / `OnErrorReturn` / `OnErrorMap`
- `Publish` / `Replay` / `RefCount`
- `RunOn`
- `Named`, `Log`, `Debug`, `Checkpoint`, `Trace`
- `DoOnNext`, `Do`, `Tap`, `DoOnError`, `DoOnComplete`, `DoOnTerminate`
- `MapWithTimestamp` / `WindowByTime`

`IStream<T>` includes `ForEachAsync(...)`, sink output, and channel output. Additional terminal operators are available through extension methods:

- `ToListAsync`, `ToArrayAsync`, `ToHashSetAsync`, `ToDictionaryAsync`, `ToLookupAsync`
- `FirstAsync` / `LastAsync` (and `OrDefault` variants)
- `ElementAtAsync` / `ElementAtOrDefaultAsync`
- `ContainsAsync`
- `SingleAsync` / `SingleOrDefaultAsync`
- `AggregateAsync` / `CountAsync` / `AnyAsync` / `AllAsync`
- `MinAsync` / `MaxAsync`
- `MinByAsync` / `MaxByAsync` (with comparer overloads)
- `SumAsync` / `AverageAsync`
- `DrainAsync` / `ToSinkAsync`

`ISingle<T>` also supports `ToTask()`.

## LINQ and Query Syntax Support

Streamix supports both fluent and query comprehension syntax.

For now, LINQ is a convenience layer rather than the full concurrency-control surface:

- `Where` / `Select` and their async counterparts stay sequential and ordered.
- `SelectMany` / `SelectManyAsync` are the unordered flattening helpers.
- Use fluent operators such as `FlatMap`, `ConcatMap`, and `FlatMapOrdered` when you need explicit unordered, sequential, or ordered flattening control.

```csharp
var result = await (
    from x in Stream.Range(1, 10)
    where x % 2 == 0
    select x * 10
).ToListAsync();

var fluent = await Stream.Range(1, 10)
    .Where(x => x % 2 == 0)
    .Select(x => x * 10)
    .ToListAsync();

var asyncResult = await Stream.Range(1, 10)
    .WhereAsync(async x => await ValidateAsync(x))
    .SelectAsync(async x => await FetchAsync(x))
    .SelectManyAsync(async x => await GetStream(x), maxConcurrency: 3)
    .ToListAsync();

var ordered = await Stream.Range(1, 10)
    .FlatMapOrdered(x => GetStream(x), maxConcurrency: 3)
    .ToListAsync();
```

Available: `Where`, `Select`, `SelectMany` (sync) and `WhereAsync`, `SelectAsync`, `SelectManyAsync` (async)

## Observability and Debugging

Streamix provides several operators to help you observe and debug your reactive pipelines.

- `Named(string name)`: Tags the stream with a name used by other diagnostic operators.
- `Log()`: Logs items, errors, and completion to standard output. Uses the stream name as a prefix if available.
- `Debug()`: Similar to `Log()` but outputs to `System.Diagnostics.Debug`.
- `Checkpoint(string name)`: Tracks progress through a specific stage of the pipeline with timing information.
- `Trace()`: Provides a comprehensive trace of the current stream lifecycle signals, including `Subscribe`, `Request(1)`, `Next(...)`, `Error(...)`, `Completed`, `Cancelled`, and `Dispose`.

`Trace()` currently includes `Request(1)` in its output to show each downstream pull from the underlying `IAsyncEnumerable<T>` pipeline. Treat that as part of the current emitted trace shape rather than as hidden implementation noise.

```csharp
await Stream.Range(1, 100)
    .Named("Orders")
    .Trace()
    .Checkpoint("ProcessStart")
    .Map(async x => await ProcessAsync(x), maxConcurrency: 5)
    .Checkpoint("ProcessEnd")
    .ForEachAsync(Console.WriteLine);
```

## Time-based Operators

Streamix provides first-class support for time-series processing using event time. Input streams are wrapped in `Timestamped<T>`, and windows are created based on these timestamps.

### `MapWithTimestamp`

Converts a regular stream into a stream of `Timestamped<T>` by extracting a timestamp from each item.

```csharp
var timestamped = source.MapWithTimestamp(x => x.CreatedAt);
```

### `WindowByTime`

Groups elements into tumbling or sliding windows based on their timestamps. Returns a stream of cold, single-consumer streams (`IStream<IStream<Timestamped<T>>>`).

**Tumbling Window:**

```csharp
await temperatureStream
    .WindowByTime(TimeSpan.FromMinutes(30))
    .FlatMap(window => window.MaxAsync(x => x.Value))
    .ForEachAsync(Console.WriteLine);
```

**Sliding Window:**

```csharp
await temperatureStream
    .WindowByTime(
        duration: TimeSpan.FromMinutes(30),
        slide: TimeSpan.FromMinutes(5))
    .FlatMap(window => window.AverageAsync(x => x.Value))
    .ForEachAsync(Console.WriteLine);
```

## Creation Operators

### `Stream.Create<T>`

For complex sources, callbacks, or event-driven systems.

```csharp
var stream = Stream.Create<int>(async emitter =>
{
    await emitter.EmitAsync(1);
    if (someCondition)
    {
        emitter.Fail(new Exception("Oops"));
    }
    else
    {
        emitter.Complete();
    }
});
```

- `EmitAsync` awaits if the downstream consumer is slow.
- Check `emitter.CancellationToken` to stop producing. `EmitAsync` throws `OperationCanceledException` if the subscriber cancels or if the stream has already reached a terminal state.

### `Stream.FromEvent<T>`

For async callback or event sources that can await item delivery and return an `IDisposable` subscription.

```csharp
var source = new PriceFeed();

var prices = Stream.FromEvent<decimal>(handler => source.Subscribe(handler));

var latest = await prices.Take(2).ToListAsync();
```

- `FromEvent(...)` expects a subscription function that accepts an async handler (`Func<T, ValueTask>`) and returns an `IDisposable` used for teardown.
- When the source awaits the handler, `FromEvent(...)` preserves the same backpressure contract as `Create(...)`.
- Each subscriber gets its own registration. Cancelling or disposing the subscription always disposes the returned registration.

### `Stream.FromTimer`

```csharp
var stream = Stream.FromTimer(TimeSpan.FromSeconds(5));
```

Emits a single `0L` after the due time, then completes.

### `Stream.FromChannel<T>`

```csharp
var stream = Stream.FromChannel(channel.Reader);
```

Drains the channel asynchronously and completes when the channel is closed.

### `Stream.FromQueue<T>`

```csharp
var queue = new Queue<int>(new[] { 1, 2, 3 });
var stream = Stream.FromQueue(queue);
```

`FromQueue(...)` is a finite adapter over `Queue<T>`, not a live async subscription source. It dequeues items in FIFO order and completes when the queue is empty. For live asynchronous queue workloads, prefer `Stream.FromChannel(...)`.

### `Stream.Defer<T>` / `Single.Defer<T>`

```csharp
var stream = Stream.Defer(() => Stream.From(DateTime.Now.Ticks));
```

The factory is called once per subscriber.

### `Stream.Generate<TState, T>`

```csharp
var stream = Stream.Generate(0, state =>
    state < 10 ? GenerationResult<int, int>.Emit(state, state + 1)
               : GenerationResult<int, int>.Complete());
```

### `Stream.Interval`

```csharp
var stream = Stream.Interval(TimeSpan.FromSeconds(1));
```

Does not accumulate ticks. If a consumer is slow, the next interval starts only after the consumer is ready.

### `Stream.Never`

```csharp
var stream = Stream.Never<int>();
```

Never emits and never completes unless the subscriber cancels.

### `Stream.Poll`

```csharp
var polled = Stream.Poll(
    TimeSpan.FromSeconds(1),
    async ct => await PollOnceAsync(ct));
```

Cold by default, passes the subscriber cancellation token into the poll callback, and waits for each poll result before scheduling the next interval.

### `Stream.Using<TResource, T>`

```csharp
var stream = Stream.Using(
    () => new StreamReader("data.txt"),
    reader => Stream.Create<string>(async emitter =>
    {
        while (!reader.EndOfStream)
        {
            await emitter.EmitAsync(await reader.ReadLineAsync());
        }
    }));
```

The resource is guaranteed to be disposed when the stream completes, fails, or the subscription is cancelled. If both the stream and the disposal throw, the disposal exception is propagated.

### `Stream.From` / `Single.From` / `Just`

Shorthands for values, tasks, and async enumerables.

- `From(Task<T>)` and `From(ValueTask<T>)` wrap existing, already-started work.
- `From(Func<Task<T>>)`, `From(Func<ValueTask<T>>)`, and their `CancellationToken` overloads defer creation until subscription.
- `Stream.Defer(...)` and `Single.Defer(...)` are always lazy and call the factory once per subscriber to create the entire stream instance.
- `Single.From(IAsyncEnumerable<T>)` strictly enforces 0..1 cardinality and throws `InvalidOperationException` if the source produces more than one item.

```csharp
Stream.Just(42);
Stream.From(new[] { 1, 2, 3 });
Stream.From(Task.FromResult("hello"));
Stream.From(async ct => await FetchData(ct));
Single.From(async ct => await FetchData(ct));
```

## Interop

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
ChannelReader<int> reader = Stream.Range(1, 3).ToChannel(capacity: 10);
```

Use `FromQueue(...)` when you need to drain an in-memory `Queue<T>` once. Use `FromChannel(...)` when you need a live async queue boundary with completion and backpressure semantics.

For bounded channel deployment boundaries, Streamix exposes explicit channel-native execution APIs:

```csharp
using System.Threading.Channels;

var reader = Stream.Range(1, 100)
    .ToChannel(capacity: 32, mode: ChannelBackpressureMode.Wait);

var isolated = Stream.Range(1, 100)
    .PipeThroughChannel(capacity: 64, mode: ChannelBackpressureMode.Fail);

var workerBoundary = Stream.Range(1, 100)
    .RunOnChannel(capacity: 64, degreeOfParallelism: 4);
```

Channel-native backpressure modes:

- `Wait`
- `DropNewest`
- `DropOldest`
- `LatestOnly`
- `Fail`

`PipeThroughChannel(...)` is the explicit execution boundary operator. `RunOnChannel(...)` adds the same boundary plus a worker-pool relay while preserving source order. `TeeToChannel(...)` mirrors items into an existing channel without turning the main pipeline into a terminal.

```csharp
var merged = Stream.MergeChannels(reader1, reader2, reader3);
```

```csharp
var batches = Stream.Range(1, 10)
    .Buffer(count: 3, capacity: 16, mode: ChannelBackpressureMode.Wait);

var windows = Stream.Range(1, 10)
    .Window(count: 3, capacity: 16, mode: ChannelBackpressureMode.Fail);
```

### Sinks

```csharp
var output = new List<int>();

await Stream.Range(1, 3).ToSinkAsync(
    (item, ct) =>
    {
        output.Add(item);
        return ValueTask.CompletedTask;
    });
```

```csharp
public interface IAsyncSink<in T>
{
    ValueTask WriteAsync(T item, CancellationToken cancellationToken = default);
    ValueTask CompleteAsync(Exception? error = null, CancellationToken cancellationToken = default);
}
```

Use `SinkCompletionMode.CompleteSink` to let the terminal own sink completion, or `SinkCompletionMode.LeaveSinkOpen` when the caller owns the destination lifetime.

### AsyncRx.NET

AsyncRx interop lives in the separate `Streamix.Extensions` project so the core package does not take a dependency on `System.Reactive.Async`.

```csharp
using Streamix.Extensions;

IStream<int> stream = asyncObservable.ToStream();
IAsyncObservable<int> observable = stream.ToAsyncObservable();
```

The extensions project also supports `ISingle<T>` interop.

### Entity Framework Core (`EfStream`)

EF integration also lives in `Streamix.Extensions` so the core `Streamix` package stays free of EF dependencies.

```csharp
using Microsoft.EntityFrameworkCore;
using Streamix.Extensions;

await EfStream.From(
        ctx => ctx.Set<Customer>().Where(c => c.IsActive),
        () => new AppDbContext())
    .Take(100)
    .ForEachAsync(customer => Console.WriteLine(customer.Name));
```

Equivalent factory-extension style:

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

EF semantics:

- A new `DbContext` is created per subscription when using the factory overloads.
- Query construction and execution use that same context instance.
- `EfStream.From(...)` and `ToStream(...)` materialize via `ToListAsync`, then emit each item.
- `EfStream.FromStreamed(...)` and `ToStreamed(...)` emit items as EF async enumeration advances.
- Buffered mode pays full per-subscription materialization cost before first emission.
- Streamed mode can stop earlier under downstream short-circuiting operators such as `Take`, but it keeps the `DbContext` alive for the duration of enumeration.
- Caller-owned `DbContext` overloads are intentionally not part of the public Streamix EF API.
- Referencing `Streamix.Extensions` intentionally adds EF Core as a transitive dependency.

Provider-sensitive caveats for streamed mode:

- Add `OrderBy(...)` when result order matters; streamed mode preserves query order, not implied table order.
- Buffered mode usually surfaces query/materialization failures before the first item. Streamed mode can fail after a partial prefix has already been emitted.
- Cancellation timing in streamed mode depends partly on the EF provider and may not be observed at exactly the same point as buffered mode.
- Slow streamed consumers keep the `DbContext` and provider resources alive longer.
- No EF-specific batching helpers are currently provided; prefer choosing buffered versus streamed mode deliberately before introducing application-level batching.

### ASP.NET Core

Streamix integrates with ASP.NET Core for Server-Sent Events, WebSocket streaming, and HTTP response streaming via the `Streamix.AspNetCore` package.

```bash
dotnet add package Streamix.AspNetCore
```

```csharp
using Streamix.AspNetCore;

app.MapGet("/prices", async (HttpResponse response, CancellationToken ct) =>
{
    var priceStream = _priceService.GetPriceUpdates().Publish().RefCount();
    await priceStream.ToSseAsync(response, ct);
});
```

```csharp
[HttpGet("prices")]
public IActionResult GetPrices()
{
    var priceStream = _priceService.GetPriceUpdates().Publish().RefCount();
    return new StreamResult<decimal>(priceStream);
}
```

```csharp
[HttpGet("ws-prices")]
public async Task GetPricesWebSocket()
{
    if (HttpContext.WebSockets.IsWebSocketRequest)
    {
        using var ws = await HttpContext.WebSockets.AcceptWebSocketAsync();
        var stream = _priceService.GetPriceUpdates();
        await stream.ToWebSocketAsync(ws, HttpContext.RequestAborted);
    }
    else
    {
        HttpContext.Response.StatusCode = 400;
    }
}
```

```csharp
[HttpGet("orders")]
public async Task GetOrders(int userId)
{
    var stream = _orderService.GetOrders(userId);
    await stream.ToJsonResponseAsync(HttpContext.Response, HttpContext.RequestAborted);
}
```

Key features:

- Built-in backpressure
- Cancellation support
- Hot stream compatible
- Zero boilerplate
- Custom serialization

## Execution

- Runs on caller context by default
- `RunOn(TaskScheduler)` moves upstream execution onto a chosen scheduler

```csharp
stream.RunOn(TaskScheduler.Default);
```

## Error Handling

```csharp
var recovered = stream
    .Map(...)
    .OnErrorResume(ex => Stream.Empty<int>());

var retried = stream
    .Retry(
        retryCount: 3,
        backoffStrategy: (attempt, ex) => TimeSpan.FromMilliseconds(attempt * 100));
```

## When to Use

- High-throughput async pipelines
- Composable stream processing
- Reactive-style data transformations
- Situations where Reactor-style API helps readability and maintainability

## When Not to Use

- Simple sequential async calls (`await` is enough)
- CPU-bound work (use `Parallel` / `PLINQ`)
- Legacy Rx-only pipelines

## Contributing

- Keep API fluent and minimal
- Focus on async-first idioms
- Backpressure awareness is required for stream operators

## License

MIT
