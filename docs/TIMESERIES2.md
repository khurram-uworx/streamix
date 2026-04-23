# Time Series Follow-Up

## Purpose

This file collects time-series backlog items that have been pushed out of earlier release plans.

Settled public contract and current guidance live in:

- `README.md`
- `GETTING-STARTED.md`
- `ARCHITECTURE.md`

When this packet is prioritized for a release, the sections below describe a sensible implementation order and the main design questions to resolve. Items not taken for that release stay here as future plans.

## Future Follow-Up Candidates

- re-evaluate time-based joins as a separate feature slice after the current release work is complete
- create a dedicated join-planning doc when there is a concrete first join scenario worth designing
- consider further time-based operators only if they have clear semantics and user value

### Planning Notes From Previous Files

- time semantics must stay explicit
- out-of-order and late-data behavior need deliberate design, not default guesses
- cross-stream operators are materially harder than single-stream operators because they force buffering, ordering, and synchronization choices
- we should not stretch channel abstractions into fake database, broker, or persistence abstractions with conflicting semantics

## Long-Horizon Direction

If Streamix ever picks one deliberately ambitious direction in this area, the strongest candidate is:

> make Streamix a serious .NET-native event-time and keyed-stream library on top of `IStream<T>`

This starts from the fact that Streamix already has:

- `MapWithTimestamp(...)`
- `WindowByTime(...)`
- `WindowBySession(...)`
- watermark-style bounded out-of-order handling
- processing-time `Throttle(...)`, `BufferByTime(...)`, and `Sample(...)`

So the "crazy" part is not "add time support". That already exists.

The next jump would be pushing further into:

- keyed streams and partitioned aggregation
- incremental and continuous aggregates
- cross-stream coordination operators
- richer lateness, finalization, and result metadata semantics

### What This Does Not Mean

This direction does not mean:

- a distributed stream processor
- a broker or Kafka replacement
- a channel-first architecture
- EF as a general-purpose state store for the whole library
- a provider-based universal query engine

The point would be to stay single-process, async-first, explicit, and honest about semantics while still going much further than ordinary `IAsyncEnumerable<T>` composition.

## Follow-Up Gaps

### 1. Time-Based Joins

This remains a separate roadmap item and should stay scoped as its own design slice. If it is selected for a release, it should be designed and implemented as a distinct slice rather than blended into the coordination or aggregation work.

Open questions:

- what is the first concrete join scenario worth supporting
- what are the exact semantics for completion, cancellation, watermark advancement, unmatched items, and late data
- how much buffering and reordering are acceptable for a first release

### 2. Keyed Streaming and Partitioned Aggregation

We still do not have a first-class keyed streaming model such as:

- `GroupBy(...)`
- `PartitionBy(...)`
- keyed substreams
- keyed window/session aggregation

Why this is a gap:

- most practical event-time use cases are "per key, per window"
- the old Trill example was fundamentally about keyed streaming
- without a keyed model, users fall back to manual state and materialization

Open questions:

- should grouped output be long-lived substreams, finite grouped windows, or keyed aggregate records
- what ordering guarantees exist within and across keys
- how much fan-out and buffering the library should own

Wireframe example:

These API sketches are intentionally illustrative. They are here to show the expected shape, return style, and metadata flow, not to finalize the surface or enumerate every overload.

Current Streamix shape:

```csharp
await sensorReadings
    .MapWithTimestamp(x => x.ObservedAt)
    .WindowByTime(
        duration: TimeSpan.FromMinutes(5),
        slide: TimeSpan.FromMinutes(1),
        outOfOrderness: TimeSpan.FromSeconds(30))
    .FlatMap(window => window.MaxAsync(x => x.Value.Temperature))
    .ForEachAsync(Console.WriteLine);
```

Possible next shape:

```csharp
await sensorReadings
    .MapWithTimestamp(x => x.ObservedAt)
    .PartitionBy(x => x.DeviceId)
    .WindowByTime(
        duration: TimeSpan.FromMinutes(5),
        slide: TimeSpan.FromMinutes(1),
        outOfOrderness: TimeSpan.FromSeconds(30))
    .Aggregate(window => new
    {
        window.Key,
        window.Start,
        window.End,
        Count = window.Count(),
        Max = window.Max(x => x.Value.Temperature)
    })
    .ForEachAsync(Console.WriteLine);
```

### 3. Incremental and Continuous Aggregates

We should decide whether the next release wants to stay with final-per-window aggregation only, or add more streaming-native aggregate forms.

Candidate areas:

- continuously updated window aggregates
- rolling or sliding aggregate state
- keyed running aggregates
- result shapes that carry explicit window/session metadata

Wireframe example:

Current Streamix shape:

```csharp
await orders
    .MapWithTimestamp(x => x.CreatedAt)
    .WindowBySession(
        gap: TimeSpan.FromMinutes(10),
        outOfOrderness: TimeSpan.FromSeconds(15))
    .FlatMap(window => window.CountAsync())
    .ForEachAsync(Console.WriteLine);
```

Possible next shape:

```csharp
await orders
    .MapWithTimestamp(x => x.CreatedAt)
    .WindowBySession(
        gap: TimeSpan.FromMinutes(10),
        outOfOrderness: TimeSpan.FromSeconds(15))
    .AggregateContinuously(
        seed: SessionTotals.Empty,
        update: (state, order) => state.Add(order))
    .ForEachAsync(Console.WriteLine);
```

### 4. Cross-Stream Coordination Operators Short Of Joins

There is still useful ground between current operators and full event-time joins.

Candidate operators:

- `WithLatestFrom(...)`
- `CombineLatest(...)`
- `ZipLatest(...)` or equivalent latest-state combinators

Why this is a good slice:

- it closes real scenario gaps
- it is often lower risk than full join semantics

Wireframe example:

```csharp
await priceTicks
    .WithLatestFrom(fxRates, (price, rate) => price.Convert(rate))
    .DistinctUntilChanged(x => x.Amount)
    .ForEachAsync(Console.WriteLine);
```

### 5. Stateful Signal-Shaping Operators

The library still appears light on stateful shaping operators such as:

- `DistinctUntilChanged(...)`
- `Debounce(...)`
- `TakeUntil(...)`
- `SkipUntil(...)`

These are not purely time-series features, but they materially affect the usefulness of the overall streaming surface.

### 6. Event-Time Ergonomics and Metadata

Even if the current narrow model stays intact, we should evaluate whether the next release needs better operator outputs and observability.

Candidate areas:

- first-class window metadata instead of only inner streams
- explicit session/window descriptors
- clearer late-event policy controls
- observability for dropped-late events
- a clearer finalization story

Wireframe example:

Current Streamix shape:

```csharp
await sensorReadings
    .MapWithTimestamp(x => x.ObservedAt)
    .WindowByTime(
        duration: TimeSpan.FromMinutes(5),
        outOfOrderness: TimeSpan.FromSeconds(20))
    .FlatMap(window => window.ToListAsync())
    .ForEachAsync(Console.WriteLine);
```

Possible next shape:

```csharp
await sensorReadings
    .MapWithTimestamp(x => x.ObservedAt)
    .WindowByTime(
        duration: TimeSpan.FromMinutes(5),
        outOfOrderness: TimeSpan.FromSeconds(20))
    .SelectWindow(window => new WindowResult<SensorReading>(
        window.Start,
        window.End,
        window.IsFinal,
        window.LateItemCount,
        window.Items))
    .ForEachAsync(Console.WriteLine);
```

### 7. Documentation Consolidation

Follow-up areas:

- make sure public docs describe only current capability
- keep forward-looking gap analysis in planning docs only
- give `WindowBySession(...)` and future time-series follow-up a clear home so design notes do not scatter again

## Suggested Release Framing

If this packet is selected for a release, a practical implementation order is:

1. cross-stream coordination operators short of joins
2. keyed streaming / partitioning model
3. incremental aggregate design
4. event-time metadata and late-data ergonomics
5. time-based joins

If the release scope only absorbs part of this list, the remaining items should stay in this file as future plans.

Longer-horizon join shape reference:

```csharp
await clicks
    .MapWithTimestamp(x => x.OccurredAt)
    .JoinByTime(
        impressions.MapWithTimestamp(x => x.OccurredAt),
        key: x => x.AdId,
        window: TimeSpan.FromMinutes(2),
        outOfOrderness: TimeSpan.FromSeconds(10),
        resultSelector: (click, impression) => new
        {
            click.Value.AdId,
            ClickedAt = click.Timestamp,
            ImpressionAt = impression.Timestamp
        })
    .ForEachAsync(Console.WriteLine);
```
