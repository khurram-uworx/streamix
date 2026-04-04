# Streamix Boundary Variety Next Tasks

## Purpose

This document captures the work that should follow after the release-targeted slice in `docs/VARIETY-TASKS.md`.

These tasks are not required to ship the next release. They exist to complete the broader boundary variety plan from `docs/VARIETY.md` without overloading the immediate release.

## Suggested Execution Order

1. Task 1: Add async-predicate terminal overloads
2. Task 2: Add caller-owned collection copy sinks
3. Task 3: Add `TextWriter` line sink support
4. Task 4: Review and trim terminal surface overlap
5. Task 5: Add frozen-collection materializers if justified
6. Task 6: Design extension-package sink boundaries

## Coordination Notes

- These tasks assume the release has already introduced `IAsyncSink<T>` and `ToSinkAsync(...)`.
- Task 2 and Task 3 should build on the shared sink path rather than creating parallel copy implementations.
- Task 4 is partly a design review and should happen before adding too many more terminals.
- Task 6 should stay design-oriented unless there is a real consumer for a specific ecosystem adapter.
- Shared files likely to create merge conflicts:
  - `src/Streamix/Extensions/TerminalExtensions.cs`
  - `README.md`

## Task 1: Add Async-Predicate Terminal Overloads

### Priority

High

### Goal

Make the terminal boundary as async-friendly as the rest of the pipeline.

### Why this exists

`docs/VARIETY.md` explicitly calls out async-predicate overloads for `AnyAsync(...)`, `AllAsync(...)`, and `CountAsync(...)` as important follow-on work.

### Scope

- Add `AnyAsync(Func<T, ValueTask<bool>> predicate, ...)`
- Add `AllAsync(Func<T, ValueTask<bool>> predicate, ...)`
- Add `CountAsync(Func<T, ValueTask<bool>> predicate, ...)`
- Add tests for short-circuit behavior, cancellation, upstream failure, and predicate failure

### Constraints

- Preserve the existing sync overloads.
- Keep short-circuit behavior explicit and tested.

### Acceptance criteria

- Callers can use async membership and reduction checks without inserting extra pipeline operators just to bridge async work.
- The async overloads match the behavior of the sync overloads aside from awaiting the predicate.

### Files likely involved

- `src/Streamix/Extensions/TerminalExtensions.cs`
- `src/Streamix.Tests/TerminalExtensionsTests.cs`
- `README.md`

## Task 2: Add Caller-Owned Collection Copy Sinks

### Priority

High

### Goal

Support boundary writes into existing mutable collections without proliferating more materializer return types.

### Why this exists

The plan recommends a clear distinction between:

- methods that allocate and return a collection
- methods that copy into a caller-owned destination

### Scope

- Add `CopyToAsync(ICollection<T> destination, ...)`
- Decide whether a separate `ISet<T>` overload adds enough value to justify itself
- Route the implementation through the sink abstraction where practical
- Add tests for success, cancellation, ordering, and destination mutation on failure

### Decision required

Decide whether partial writes on upstream failure are acceptable and how that is documented.

### Acceptance criteria

- Users can copy a stream into an existing collection they own.
- The ownership and partial-write semantics are documented and tested.

### Files likely involved

- `src/Streamix/Extensions/TerminalExtensions.cs`
- `src/Streamix.Tests/TerminalExtensionsTests.cs`
- `README.md`

## Task 3: Add `TextWriter` Line Sink Support

### Priority

Medium

### Goal

Provide a high-value text boundary for logging, files, diagnostics, and simple export scenarios.

### Why this exists

`docs/VARIETY.md` identifies `TextWriter` as a strong first-pass sink for boundary-heavy applications.

### Scope

- Add `WriteLinesAsync(TextWriter writer, ...)`
- Support a formatter overload or formatter callback
- Decide whether `leaveOpen` belongs on the public API or is implicit in the supplied writer ownership model
- Add tests for formatting, cancellation, sink failure, and completion ownership

### Constraints

- Keep this as an extension-style sink API backed by the shared sink logic.
- Do not add broader stream/byte/pipe abstractions in the same task.

### Acceptance criteria

- Callers can stream line-oriented output into a `TextWriter`.
- Writer ownership and failure semantics are explicit and tested.

### Files likely involved

- `src/Streamix/Extensions/TerminalExtensions.cs`
- `src/Streamix.Tests/TerminalExtensionsTests.cs`
- `README.md`

## Task 4: Review And Trim Terminal Surface Overlap

### Priority

Medium

### Goal

Keep the boundary API disciplined as the terminal set grows.

### Why this exists

The repo already includes several extra terminal shapes beyond the original plan. Before adding more, the team should confirm that the surface remains intentional rather than accretive.

### Scope

- Review overlap across:
  - throwing terminals
  - default-returning terminals
  - option-returning terminals
  - diagnostic/subscribe-style boundaries
- Identify candidates that should remain core versus candidates that should stay undocumented or move to a different package later
- Produce a short design note or `WORK.md` entry if the review changes sequencing or public-surface guidance

### Acceptance criteria

- The team has a written decision on which terminal families are part of the intended long-term core boundary story.
- Future terminal additions can reference that decision instead of repeating the same API debate.

### Files likely involved

- `README.md`
- `WORK.md`
- `docs/VARIETY.md`

## Task 5: Add Frozen-Collection Materializers If Justified

### Priority

Low

### Goal

Add frozen collection terminals only if real usage justifies them after the release.

### Why this exists

`docs/VARIETY.md` lists frozen collections as useful but clearly lower priority than the initial sink pass.

### Scope

- Evaluate `ToFrozenSetAsync(...)`
- Evaluate `ToFrozenDictionaryAsync(...)`
- Add only the APIs that are justified by target framework support and real usage
- Add tests for duplicate-key and comparer semantics as needed

### Constraints

- Do not add frozen collection APIs by default just for LINQ parity.
- Keep the surface small.

### Acceptance criteria

- Any frozen collection APIs that land have a clear usage case and consistent semantics with the existing keyed terminals.

### Files likely involved

- `src/Streamix/Extensions/TerminalExtensions.cs`
- `src/Streamix.Tests/TerminalExtensionsTests.cs`
- `README.md`

## Task 6: Design Extension-Package Sink Boundaries

### Priority

Low

### Goal

Prepare the repo for ecosystem-specific sinks without pulling those dependencies into the core package.

### Why this exists

The plan explicitly says transport-specific or ecosystem-specific sinks should live in separate extension packages once the generic sink contract exists.

### Scope

- Define criteria for what belongs in core versus an extension package
- Identify the first likely external sink candidates based on actual usage pressure
- Decide whether any sink-specific adapters belong in `Streamix.Extensions` or a new package
- Capture the decision in docs rather than shipping speculative code

### Acceptance criteria

- The repo has a documented boundary for future sink packages.
- Future integrations can build on `IAsyncSink<T>` without reopening core-package scope questions.

### Files likely involved

- `docs/VARIETY.md`
- `README.md`
- `src/Streamix.Extensions/README.md`
- `WORK.md`

## Suggested Agent Handout Batches

### Batch A: immediate follow-on boundary APIs

- Task 1
- Task 2
- Task 3

### Batch B: API discipline and optional expansion

- Task 4
- Task 5
- Task 6

## Final Checklist

- every task has a clear owner-sized scope
- every task has acceptance criteria
- decision-gate tasks are clearly marked
- likely files are listed to reduce agent search time
- execution order reflects real dependencies
