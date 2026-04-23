# Time Series Review

## Purpose

This document reviews the current time-series work for release readiness and breaks any remaining release work into assignable tasks.

The current review conclusion is:

- event-time windowing, watermark-aware behavior, and session windows appear structurally in place
- the newly added processing-time operators need one more release pass before the time-series work should be considered cleanly ready

## Review Summary

The main follow-up is in the new `BufferByTime` and `Sample` implementations.

Two release-relevant gaps remain:

1. The new processing-time operators do not currently preserve the same backpressure discipline expected elsewhere in Streamix.
2. Their current implementation pattern is duplicated and should be refactored once the runtime semantics are corrected.

There is also release-doc cleanup to finish before deleting `docs/TIMESERIES.md`.

## Findings

### 1. `BufferByTime` and `Sample` use an unbounded internal channel and bypass backpressure expectations

Both operators enqueue source items through `Channel.CreateUnbounded<object?>()` and `TryWrite(...)`.

That happens in:

- `BufferByTime` at `src/Streamix/Extensions/TimeseriesExtensions.cs`
- `Sample` at `src/Streamix/Extensions/TimeseriesExtensions.cs`

This means a fast source can keep pushing into an unbounded internal queue even when downstream is slower, which is at odds with the repo's stated backpressure discipline and with the earlier task guidance for additional time-based operators.

### 2. The current implementation can flush buffered state after an upstream fault

In both operators, the source worker completes the internal channel in `finally`, and the outer operator loop may still flush buffered state before scope finalization rethrows the worker exception.

That means the observable behavior can become:

- source faults
- buffered items are still emitted in a final flush
- only then is the error observed

That is weaker than the intended contract for `BufferByTime`, and `Sample` also needs an explicit and documented fault/completion rule.

### 3. The processing-time operator implementation is duplicated in one large file

`BufferByTime` and `Sample` currently duplicate almost the same timer/source/channel/scope orchestration logic inline in `src/Streamix/Extensions/TimeseriesExtensions.cs`.

This is not just stylistic:

- it increases the chance that one operator gets a semantic fix and the other drifts
- it makes the already long `TimeseriesExtensions.cs` harder to maintain
- it obscures the actual operator-specific behavior behind repeated coordination code

### 4. Public docs are not yet aligned with the feature surface that now exists

`README.md` and `ARCHITECTURE.md` do not currently document `BufferByTime` or `Sample`.

`docs/TIMESERIES2.md` has now been reduced to future carry-forward only, but the public release docs still need the same cleanup pass.

## Suggested Execution Order

1. Task 1: correct processing-time operator runtime semantics
2. Task 2: refactor the shared implementation path for processing-time operators
3. Task 3: finish release docs and simplify the carry-forward doc

## Coordination Notes

- Task 1 is the release gate.
- Task 2 should not begin until Task 1 settles the intended runtime behavior.
- Task 3 can start after Task 1 if docs are updated to the final shipped behavior, but it will likely touch the same narrative files as Task 1.
- Shared files likely to create merge conflicts are `src/Streamix/Extensions/TimeseriesExtensions.cs`, `README.md`, and `ARCHITECTURE.md`.

## ✅ Task 1: Fix Processing-Time Operator Runtime Semantics

### Priority

High

### Goal

Bring `BufferByTime` and `Sample` in line with Streamix expectations for backpressure, completion, cancellation, and failure behavior.

### Why this exists

The current implementations use an unbounded internal queue and can flush buffered state after a source fault. That is too weak for a release-quality operator contract.

### Scope

- replace the unbounded internal coordination pattern with one that preserves bounded or naturally propagating backpressure
- ensure upstream failure does not emit a synthetic final buffer or sampled value unless the documented contract explicitly allows it
- define and enforce completion versus failure versus cancellation flush behavior for both operators
- add or update tests that exercise slow-consumer and failure scenarios specifically for these operators

### Constraints

- do not weaken the existing semantics of other time-based operators
- do not introduce a broad scheduler or runtime abstraction expansion as part of this fix
- keep the public API minimal

### Suggested implementation path

- decide the exact runtime contract first for `BufferByTime` and `Sample`
- implement the coordination path so source pressure is not hidden behind an unbounded queue
- add targeted tests for source fault, downstream slowness, cancellation, and completion flush

### Acceptance criteria

- `BufferByTime` and `Sample` no longer rely on unbounded source-side buffering that hides downstream pressure
- upstream failure behavior is explicit and verified by tests
- completion and cancellation behavior are explicit and verified by tests
- the implementations match the documented contract without ambiguity

### Files likely involved

- `src/Streamix/Extensions/TimeseriesExtensions.cs`
- `src/Streamix.Tests/TimeBasedOperatorTests.cs`
- `ARCHITECTURE.md`

## Task 2: Refactor Shared Processing-Time Operator Infrastructure

### Priority

Medium

### Goal

Remove the duplicated timer/source/channel orchestration from `BufferByTime` and `Sample` and replace it with a shared internal helper or internal type.

### Why this exists

The current inline implementations are too repetitive and increase maintenance risk. Any semantic fix to one operator is likely to need the same fix in the other.

### Scope

- extract the shared coordination logic used by `BufferByTime` and `Sample`
- keep the operator-specific projection logic local and easy to read
- reduce the size and branching complexity of `TimeseriesExtensions.cs`

### Constraints

- do not change the public API
- do not perform unrelated refactors across the broader extension surface

### Acceptance criteria

- duplicated timer/source orchestration between `BufferByTime` and `Sample` is removed or substantially reduced
- operator-specific logic is easier to read than the current inline implementations
- the refactor preserves the final semantics established in Task 1

### Files likely involved

- `src/Streamix/Extensions/TimeseriesExtensions.cs`
- `src/Streamix/Implementations`

## Task 3: Align Release Docs And Simplify Next-Release Carry-Forward

### Priority

High

### Goal

Make the public and planning docs match the code that actually ships, then reduce `TIMESERIES2.md` to true next-release carry-forward only.

### Why this exists

The public docs do not yet describe the newly added operators truthfully enough for release handoff.

### Scope

- update `README.md` to mention only the time-based operators that truly ship
- update `ARCHITECTURE.md` with the actual processing-time semantics for `BufferByTime` and `Sample`
- keep `docs/TIMESERIES2.md` small and future-facing

### Acceptance criteria

- public docs describe the actual shipped operator surface truthfully
- the next-release doc is concise and future-facing, similar in style to `docs/EF-STREAM2.md`

### Files likely involved

- `README.md`
- `ARCHITECTURE.md`
- `docs/TIMESERIES2.md`

## Additional Tasks

If time-based joins become active again, create a separate dedicated planning doc rather than reusing release-review notes.

## Suggested Agent Handout Batches

### Batch A: release gate

- Task 1

### Batch B: implementation cleanup

- Task 2

### Batch C: docs

- Task 3

## Final Checklist

- the remaining release work is isolated clearly from already-shipped time-series work
- the release gate is explicit
- each task is assignable without requiring the original planning context
- the next-release carry-forward doc can stay small once this review work is complete
