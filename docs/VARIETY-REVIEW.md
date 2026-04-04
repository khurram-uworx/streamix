# Streamix Boundary Variety Review

## Scope

This review compares `docs/VARIETY.md` against:

- the current boundary APIs in `src/Streamix/IStream.cs` and `src/Streamix/ISingle.cs`
- the current terminal implementation in `src/Streamix/Extensions/TerminalExtensions.cs`
- the current stream/channel behavior in `src/Streamix/Implementations/Stream.cs` and `src/Streamix/Implementations/ConnectableStream.cs`
- the current tests in `src/Streamix.Tests`
- the public contract described in `README.md`

## Executive Summary

The repo is partway through the variety plan, but not at the release target described in `docs/VARIETY.md`.

What is already in place:

- terminal materializers such as `ToListAsync`, `ToArrayAsync`, `ToHashSetAsync`, and `ToDictionaryAsync`
- duplicate-key semantics for `ToDictionaryAsync(...)` now aligned with LINQ-style throwing behavior
- comparer overloads for `ToHashSetAsync(...)` and `ToDictionaryAsync(...)`
- several of the planned “next” terminals already implemented: `ElementAtAsync`, `ElementAtOrDefaultAsync`, `MinByAsync`, `MaxByAsync`, and `DrainAsync`
- the existing `ToChannel(...)` boundary and tests for write, backpressure, error propagation, and leave-open behavior

What is still missing for the intended release slice:

- `ContainsAsync(...)`
- `ToLookupAsync(...)`
- comparer support for `ToLookupAsync(...)` because `ToLookupAsync(...)` does not exist yet
- comparer overloads for `MinByAsync(...)` / `MaxByAsync(...)`
- async-predicate overloads for `AnyAsync(...)`, `AllAsync(...)`, and `CountAsync(...)`
- the sink abstraction pivot: `IAsyncSink<T>`, `SinkCompletionMode`, and `ToSinkAsync(...)`
- adapter-based sink expansion, including a delegate sink and reworking `ToChannel(...)` onto the shared sink path

The main conclusion is straightforward:

- terminal variety is ahead of the original plan in a few spots
- sink variety is still at the pre-pivot stage
- the release should be framed around finishing the missing core boundary pieces, not around inventing more terminal surface

## Delivery Status Against `docs/VARIETY.md`

### Already implemented from the plan

- `ToDictionaryAsync(...)` duplicate keys throw because the implementation uses `Dictionary.Add(...)`, and tests lock that behavior down.
- comparer overloads already exist for:
  - `ToDictionaryAsync(...)`
  - `ToHashSetAsync(...)`
- `ElementAtAsync(...)` is implemented.
- `ElementAtOrDefaultAsync(...)` is implemented.
- `MinByAsync(...)` is implemented.
- `MaxByAsync(...)` is implemented.
- `DrainAsync(...)` is implemented.

### Implemented beyond the plan’s minimum release slice

The terminal surface is broader than `docs/VARIETY.md` assumes in a few areas:

- option-style terminals such as `FirstOrNoneAsync(...)`, `LastOrNoneAsync(...)`, `SingleOrNoneAsync(...)`, and `ElementAtOrNoneAsync(...)`
- subscription-style boundaries such as `SubscribeAsync(...)`
- diagnostics-style terminal `ExecuteAsync(...)`
- blocking bridge `ToEnumerableBlocking(...)`

These are not problems by themselves, but they reinforce the need to keep the next work focused. The repo already has enough terminal surface that sink consolidation matters more than adding more one-off methods.

### Still missing from the release-targeted slice

- `ContainsAsync(...)`
- `ToLookupAsync<TKey>(...)`
- `ToLookupAsync<TKey, TValue>(...)`
- comparer overloads for `ToLookupAsync(...)`
- `IAsyncSink<T>`
- `SinkCompletionMode`
- `ToSinkAsync(...)`
- delegate-based sink adapter
- making `ToChannel(...)` an adapter over the shared sink path

### Still missing from the broader plan

- `CopyToAsync(...)` for caller-owned collections
- `WriteLinesAsync(...)` or equivalent `TextWriter` sink
- lower-priority terminals such as frozen collection materializers
- any ecosystem-specific sink packages

## Findings

### 1. The release target in `docs/VARIETY.md` is not complete yet

The recommended release slice at the end of `docs/VARIETY.md` was:

1. lock down `ToDictionaryAsync(...)` semantics
2. add `ContainsAsync`, `ElementAtAsync`, `ElementAtOrDefaultAsync`, and `ToLookupAsync`
3. add `DrainAsync`
4. introduce `IAsyncSink<T>` plus `ToSinkAsync(...)`
5. rework `ToChannel(...)` to use the sink abstraction
6. add a delegate-based sink adapter

Current state against that list:

- item 1 is done
- item 2 is only partially done
- item 3 is done
- items 4, 5, and 6 are not done

Impact:

- the repo has a stronger terminal story than before
- but it still does not have the reusable sink story that `docs/VARIETY.md` calls the main pivot

Priority: High

### 2. Terminal semantics are better than before, but coverage is uneven relative to the boundary goals

There are focused tests for:

- duplicate-key `ToDictionaryAsync(...)`
- comparer-aware `ToHashSetAsync(...)`
- `ElementAtAsync(...)` and `ElementAtOrDefaultAsync(...)`
- `MinByAsync(...)` and `MaxByAsync(...)`
- `DrainAsync(...)`
- `ToChannel(...)` success, backpressure, error, and leave-open behavior

What is not yet well covered for the current terminal set:

- cancellation behavior for the newer terminal additions
- upstream exception propagation across the newer terminal additions
- empty-stream behavior for every newly added boundary operator
- comparer behavior for every relevant keyed or extremum-style terminal

Impact:

- the implementation is moving in the right direction
- but the “excellence at system boundaries” goal is only partly supported by tests

Priority: High

### 3. `ContainsAsync(...)` is still missing even though it is one of the most practical short-circuit terminals

`docs/VARIETY.md` correctly prioritizes `ContainsAsync(...)` as a boundary-friendly membership check. It is still absent from the implementation and from the tests.

Impact:

- callers still need to spell this as `AnyAsync(x => comparer.Equals(...))` or equivalent
- the current API misses a common short-circuit terminal that belongs in the core set

Priority: High

### 4. `ToLookupAsync(...)` is still missing, so grouped materialization is not part of the boundary story yet

The plan calls grouped materialization a real system-boundary shape. That gap remains open.

Impact:

- Streamix can materialize flat collections and dictionaries
- it still cannot materialize grouped keyed output without forcing callers to write custom aggregation

Priority: High

### 5. Sink variety is still channel-specific rather than abstraction-first

`docs/VARIETY.md` explicitly recommends pivoting away from destination-by-destination sink APIs and toward a small reusable sink contract.

Current state:

- `IStream<T>` still exposes `ToChannel(ChannelWriter<T> writer, bool completeWriter = true, ...)`
- `TerminalExtensions` also exposes `ToChannel(int? capacity = null, ...)`
- there is no `IAsyncSink<T>`
- there is no `ToSinkAsync(...)`
- there is no shared completion policy abstraction such as `SinkCompletionMode`

Impact:

- Streamix still has one special-case sink instead of a reusable sink model
- every future sink would need either duplicate copy logic or another ad hoc API unless this pivot lands first

Priority: High

### 6. `ToChannel(...)` already has useful semantics, but they are not generalized

This is the strongest existing sink boundary today. Tests already cover:

- copying all items
- bounded-channel backpressure
- upstream error propagation
- leave-open behavior

That is good news, because it means the future sink abstraction has a concrete behavioral baseline. The missing step is refactoring the same semantics behind a reusable abstraction instead of leaving channels as a special case.

Priority: Medium

### 7. Async-first predicate support is still inconsistent at the terminal boundary

The repo already supports async predicates and async selectors inside the pipeline, for example via `FilterAwait(...)` and LINQ-style async extensions.

However, the plan’s async terminal overloads are still missing:

- `AnyAsync(Func<T, ValueTask<bool>> ...)`
- `AllAsync(Func<T, ValueTask<bool>> ...)`
- `CountAsync(Func<T, ValueTask<bool>> ...)`

Impact:

- callers can keep async work inside the pipeline, but not at these common short-circuit/reduction terminal boundaries
- that inconsistency is noticeable in an async-first library

Priority: Medium

### 8. `MinByAsync(...)` and `MaxByAsync(...)` shipped with a narrower shape than the plan intends

The current implementation constrains `TKey` to `IComparable<TKey>` and does not accept an `IComparer<TKey>?`.

That works for many cases, but it falls short of the plan’s .NET-idiomatic comparer-aware shape.

Impact:

- callers cannot provide custom ordering without projecting to a comparer-friendly key type
- the API is useful but not yet complete

Priority: Medium

### 9. README is behind the actual current boundary surface

README still describes the terminal story in broad terms, but it does not reflect some already-shipped additions such as:

- `ElementAtAsync(...)`
- `ElementAtOrDefaultAsync(...)`
- `MinByAsync(...)`
- `MaxByAsync(...)`
- `DrainAsync(...)`

That is not the main blocker for the variety release, but the README should be updated once the release scope is settled so the public contract matches reality.

Priority: Medium

## Release Readiness View

### Ready or close enough

- dictionary duplicate-key semantics
- keyed collection comparer overloads that already exist
- element terminals already implemented
- completion-only terminal already implemented
- channel boundary semantics as a baseline

### Not ready for the targeted release

- grouped materializer gap (`ToLookupAsync(...)`)
- membership terminal gap (`ContainsAsync(...)`)
- sink abstraction gap
- reusable sink adapter gap
- async terminal predicate gap
- test hardening for the newer boundary terminals

## Recommended Release Scope

For the next Streamix release, the repo should stay aligned with the narrow implementation slice that `docs/VARIETY.md` already proposed, but updated for current reality:

1. Keep the now-resolved `ToDictionaryAsync(...)` semantics and strengthen related tests where needed.
2. Add the missing high-value terminals:
   - `ContainsAsync(...)`
   - `ToLookupAsync(...)`
3. Harden the already-added terminals with boundary-focused tests:
   - `ElementAtAsync(...)`
   - `ElementAtOrDefaultAsync(...)`
   - `MinByAsync(...)`
   - `MaxByAsync(...)`
   - `DrainAsync(...)`
4. Introduce the sink abstraction:
   - `IAsyncSink<T>`
   - `SinkCompletionMode`
   - `ToSinkAsync(...)`
5. Rework `ToChannel(...)` onto the shared sink path.
6. Add the delegate-based sink adapter.
7. Update README so the release contract is truthful.

## Validation

Code and tests were inspected directly for this review.

I also attempted validation with:

```text
dotnet test --configuration Release
dotnet restore Streamix.slnx
dotnet test src\Streamix.Tests\Streamix.Tests.csproj --configuration Release
```

Those commands did not complete successfully in the current environment because the sandboxed .NET CLI setup/restore path was restricted. This review therefore relies on repository inspection and the existing test suite contents rather than a fresh full test run.

## Bottom Line

Streamix has meaningful terminal variety already, and some of the originally planned additions have already landed.

The missing piece for this release is not “more random terminals.” It is finishing the intended boundary story:

- add the last high-value core terminals that are still absent
- pivot sink behavior onto a reusable abstraction
- tighten tests and docs around the new boundary APIs
