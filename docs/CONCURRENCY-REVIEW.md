# Concurrency Review

Date: 2026-04-06

Status: not ready for the 0.6 concurrency release as currently documented

Scope reviewed:
- `docs/CONCURRENCY.md`
- `README.md`
- `src/Streamix/IStream.cs`
- `src/Streamix/Implementations/Stream.cs`
- `src/Streamix/Implementations/ConnectableStream.cs`
- `src/Streamix/Extensions/LinqExtensions.cs`

Validation note:
- I did not rerun tests in Codex sandbox. This review is based on code and contract inspection, and on the existing green test status reported by the engineering team.

## Findings

### 1. The release contract still makes `Map` concurrency semantics implicit, which undercuts the main 0.6 goal

`docs/CONCURRENCY.md` says the new model makes ordering and concurrency clear and discoverable, and it explicitly defines `Map()` as unordered and unbounded by default. `README.md` repeats that story.

That is not true for the public API as shipped:
- `Map(Func<T, TResult>)` is the normal sequential ordered map in [src/Streamix/IStream.cs](#L27)
- the unordered concurrent variant is a different overload, `Map(Func<T, Task<TResult>>, int maxConcurrency = int.MaxValue)`, in [src/Streamix/IStream.cs](#L36)
- the docs still describe `Map()` generically as unordered and fastest in [docs/CONCURRENCY.md](#L23) and [README.md](#L89)

This means the semantic distinction is still hidden in the delegate shape, not in the operator name. That is exactly the kind of implicitness the 0.6 plan was trying to remove.

Release impact:
- users cannot infer concurrency semantics from the method name alone
- the README currently over-promises behavior
- the API is clearer for `FlatMap`/`ConcatMap`/`FlatMapOrdered` than it is for `Map`

### 2. Query-syntax and LINQ-style composition still expose only the unordered flattening story

The README positions query syntax as a first-class surface in [README.md](#L168). But the LINQ extensions only map `SelectMany` and `SelectManyAsync` to unordered `FlatMap`:
- [src/Streamix/Extensions/LinqExtensions.cs](#L62)
- [src/Streamix/Extensions/LinqExtensions.cs](#L82)

There is no corresponding ordered-concurrent or sequential flattening path on that idiomatic surface.

That leaves the new concurrency model only partially exposed. If 0.6 is supposed to make concurrency control a user-facing superpower, it should not disappear as soon as a consumer uses the LINQ/query syntax that the README actively promotes.

Release impact:
- discoverability is inconsistent across Streamix’s two advertised composition styles
- users can reach unordered concurrency easily, but not the ordered/sequential alternatives, from the LINQ surface

### 3. `FlatMapOrdered` still contains a hidden buffering policy instead of fully exposed control

Both implementations hard-code a per-inner bounded channel size of `16`:
- [src/Streamix/Implementations/Stream.cs](#L687)
- [src/Streamix/Implementations/ConnectableStream.cs](#L493)

This is a real production behavior knob:
- it affects how far faster later inners can run ahead while waiting for ordered emission
- it affects memory, throughput, and stall behavior
- it is not documented in `README.md` or `docs/CONCURRENCY.md`
- it is not user-configurable

For a release framed as “we already have concurrency and backpressure, now we want to expose control”, this still hides an important control point inside the implementation.

Release impact:
- ordered-concurrent behavior is partly defined by an undocumented magic number
- tuning `maxConcurrency` alone does not fully describe runtime behavior

## Secondary concerns

- Ordered operators do not appear to fail fast. `MapOrdered` awaits tasks in source order in [src/Streamix/Implementations/Stream.cs](#L315), and `FlatMapOrdered` drains inner readers in source order in [src/Streamix/Implementations/Stream.cs](#L743). That may be acceptable, but it is a meaningful semantic choice and is not documented.
- The docs are internally consistent with the chosen `FlatMapOrdered` name, but if product messaging still intends a `FlatMapSequential`-style concept, that naming decision needs to be settled explicitly before release.

## Recommendation

Do not declare the concurrency portion of 0.6 complete yet.

Minimum bar before I would call this release-ready:
- fix the README and concurrency doc so they describe `Map` precisely, or rename/split the API so concurrency is explicit from the operator name
- decide whether LINQ/query syntax is part of the concurrency-control story for 0.6; if yes, add the missing ordered/sequential path or clearly document the limitation
- document and justify the `FlatMapOrdered` inner buffering policy, or expose it as an intentional control

As it stands, the implementation contains useful concurrency primitives, but the release claim “make concurrency control explicit and discoverable” is only partially achieved.
