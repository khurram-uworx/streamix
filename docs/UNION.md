# Union Plan: Channel Phase 4 + Structured Concurrency

## Purpose

This plan merges:

- channel phase-4 goals from `docs/CHANNEL-WORK.md` and `docs/CHANNEL-TASKS.md`
- structured concurrency goals from `docs/STRUCTURED-CONCURRENCY-DESIGN.md` and `docs/STRUCTURED-CONCURRENCY-TASKS.md`

into a single implementation track that avoids duplicated primitives and conflicting lifecycle semantics.

This file is additive. Existing docs remain the source history and decision trail.

## Current State (Observed)

- Channel phase 1-3 features are implemented and tested (`PipeThroughChannel`, `RunOnChannel`, `TeeToChannel`, channel-backed `Buffer`/`Window`, backpressure modes).
- Structured concurrency public surface exists (`Stream.ScopedAsync`, `IStreamScope`, `StreamScope` primitive).
- Structured concurrency is not yet integrated into concurrent operators and channel boundaries.
- There is no unified supervision primitive used by both general concurrency operators and channel boundaries.
- There are no dedicated tests proving `ScopedAsync` behavior contract.

## Union Direction

Use **one supervision/lifetime model** for both tracks:

- `IStream<T>` remains the primary composition model.
- Channels stay explicit execution boundaries, not a competing model.
- Structured concurrency provides parent/child lifetime semantics across:
  - scope entry (`ScopedAsync`)
  - concurrent operators (`FlatMap`, `MapOrdered`, `FlatMapOrdered`, terminal concurrency helpers)
  - channel-backed boundaries (`PipeThroughChannel`, `RunOnChannel`, optionally `TeeToChannel` where meaningful)

Execution-graph diagnostics are optional and deferred unless needed to validate supervision behavior.

## Final Union Contract

The following statements are the implementation contract for this union plan.

### Scope and Child Work

- A supervision boundary **MUST** define explicit child work.
- Child work **MUST** include:
  - work spawned via `IStreamScope.Run(...)`
  - operator-internal concurrent tasks for: `FlatMap`, `MapOrdered`, `FlatMapOrdered`, and `FlatMapAwait`
  - channel-boundary worker tasks for: `PipeThroughChannel`, `RunOnChannel`, and `TeeToChannel`
- Child registration **MUST** occur before child execution can escape supervision.

### Completion Semantics

- A supervised boundary **MUST NOT** complete until:
  - parent body/action has finished, and
  - all registered children have reached a terminal state (success, canceled, or faulted).
- Completion waiting **MUST** be deterministic and race-safe under concurrent child registration/settlement.
- Nested supervised boundaries **MUST** compose: parent completion waits transitively through child boundaries.

### Cancellation Semantics

- Supervision cancellation token **MUST** be linked to parent/outer cancellation.
- When parent cancellation is requested, active children **MUST** observe cancellation via the linked token.
- Cancellation paths **MUST** be idempotent (safe if triggered multiple times).

### Failure Semantics

- The default policy **MUST** be fail-fast:
  - first observed non-cancellation fault triggers boundary cancellation signal to siblings.
- After cancellation is signaled, the boundary **MUST** still wait for all children to settle.
- Exception propagation **MUST** be deterministic and documented:
  - for v1, propagate the first encountered non-cancellation exception after all children settle; subsequent faults are suppressed for propagation but may be logged internally.
- `OperationCanceledException` caused by boundary cancellation **SHOULD** be treated as expected cancellation, not as a new primary fault.

### Ordering and Backpressure Invariants

- Union supervision integration **MUST NOT** weaken existing ordering guarantees of ordered operators.
- Union supervision integration **MUST NOT** weaken existing backpressure behavior of channel boundaries.
- Any behavior change required by supervision **MUST** be covered by tests and explicitly documented.

### Resource Safety

- Supervised completion **MUST** preserve resource-lifetime correctness (`Using`, enumerator disposal, channel completion/teardown).
- Disposal/teardown logic **MUST** remain exception-safe and idempotent.

### API and Surface Constraints

- `IStream<T>` **MUST** remain the primary user model.
- Phase 2/3 channel public API signatures **SHOULD NOT** change unless contract conformance requires it.
- New public surface area **SHOULD** be minimal; reuse existing entry points where feasible.

### Diagnostics Position

- Execution-graph diagnostics are **MAY** for this union cut and are deferred by default.
- If added, diagnostics **MUST NOT** change runtime semantics or become required for supervision correctness.

## Non-Goals (For This Union Cut)

- No broad new channel-first composition API.
- No rework of phase 2/3 API signatures unless a concrete semantic gap is found.
- No advanced policy matrix expansion beyond the existing fail-fast scope behavior unless tests force a change.
- No large public API expansion before integration semantics are proven by tests.

## Proposed Milestones

1. **Contract freeze**: finalize union semantics and boundaries.
2. **Primitive alignment**: harden scope primitive for reuse in operator/channel internals.
3. **Integration pass**: wire supervision into selected concurrent + channel paths.
4. **Behavioral verification**: add missing contract tests and regression matrix.
5. **Docs alignment**: update README/roadmap wording to reflect delivered behavior.

## Task Backlog (Agent-Ready)

## ✅ Task 1: Union Contract Freeze

### Priority

High

### Goal

Document one canonical semantic contract for supervision, cancellation, completion, and failures across structured and channel-backed concurrency.

### Scope

- Define child work for each category:
  - `scope.Run(...)`
  - operator-spawned tasks
  - channel boundary worker tasks
- State completion rules:
  - boundary/scope does not complete before child work settles
- State failure rules:
  - fail-fast cancellation
  - propagated exception shape (first vs aggregate)
- State cancellation rules:
  - linked parent token behavior
- Record explicit non-goals and deferred diagnostics.

### Acceptance Criteria

- A concise union contract section is added to this file.
- Remaining implementation tasks can execute without reopening semantics.

### Files Likely Involved

- `docs/UNION.md`
- `WORK.md` (optional decision log entry)

## ✅ Task 2: Harden Core Supervision Primitive

### Priority

High

### Goal

Ensure the core scope primitive is robust and suitable for shared use by operators and channel boundaries.

### Scope

- Review `StreamScope` for:
  - race safety around registration/disposal
  - deterministic completion waiting
  - exception propagation consistency
  - cancellation idempotency
- Introduce internal abstractions only if required for reuse (keep surface minimal).

### Acceptance Criteria

- Primitive behavior matches union contract under success/cancel/failure.
- Primitive can be consumed by both operator and channel paths.

### Files Likely Involved

- `src/Streamix/Implementations/StreamScope.cs`
- `src/Streamix/Stream.cs`
- `src/Streamix/Interfaces.cs`

## ✅ Task 3: Integrate With Concurrent Operators

### Priority

High

### Goal

Move core concurrent operators from ad hoc task/semaphore lifetime handling toward the union supervision model.

### Scope

- Integrate selected operators:
  - `FlatMap` (task and stream variants)
  - `MapOrdered`
  - `FlatMapOrdered`
  - `FlatMapAwait` (if same pattern)
- Ensure nested concurrent operators compose predictably.
- Preserve ordering/backpressure guarantees.

### Acceptance Criteria

- Concurrent operator child work is explicitly supervised.
- Operator completion does not race ahead of supervised child work.
- Existing behavior (ordering/backpressure) remains intact.

### Files Likely Involved

- `src/Streamix/Extensions/StreamExtensions.cs`
- `src/Streamix/Extensions/TerminalExtensions.cs` (if helper reuse required)

## Task 4: Integrate With Channel Boundaries (Phase 4 Core)

### Priority

High

### Goal

Wire union supervision semantics into channel-backed execution boundaries to satisfy channel phase-4 supervision path.

### Scope

- Integrate with:
  - `ChannelExecution.PipeThroughChannel`
  - `ChannelExecution.RunOnChannel`
  - evaluate `TeeToChannel` participation (mainly completion/teardown semantics)
- Ensure nested boundaries do not double-supervise or leak child tasks.
- Keep phase 2/3 public API shape unchanged unless contract requires otherwise.

### Acceptance Criteria

- Boundary completion respects supervised child lifetime.
- Cancellation/failure behavior is consistent with operator paths.
- No regressions in existing channel behavior tests.

### Files Likely Involved

- `src/Streamix/ChannelExecution.cs`
- `src/Streamix/Extensions/StreamExtensions.cs`

## Task 5: Add Structured + Channel Union Test Matrix

### Priority

High

### Goal

Prove union semantics through tests that distinguish plain bounded concurrency from supervised structured behavior.

### Scope

- Add tests for `Stream.ScopedAsync`:
  - waits for children
  - child failure cancels siblings
  - parent cancellation cancels children
- Add integration tests for concurrent operators under union semantics.
- Add integration tests for channel boundaries under union semantics.
- Add/adjust resource-safety tests for disposal ordering and teardown.

### Acceptance Criteria

- New tests fail against pre-union implementation and pass after integration.
- Coverage includes success, cancellation, failure propagation, completion ordering.

### Files Likely Involved

- `src/Streamix.Tests/ConcurrencyTests.cs`
- `src/Streamix.Tests/StreamTests.cs`
- `src/Streamix.Tests/BatchOperatorTests.cs`
- `src/Streamix.Tests/ResourceSafetyTests.cs`

## Task 6: Docs and Roadmap Alignment

### Priority

Medium

### Goal

Align external docs with what was actually implemented.

### Scope

- Update README with:
  - structured concurrency usage
  - how it relates to channel boundaries and `maxConcurrency`
- Update roadmap/work log wording to mark completed parts accurately.
- Keep claims limited to behavior that exists in code and tests.

### Acceptance Criteria

- README examples compile against current APIs.
- Roadmap wording does not overclaim.

### Files Likely Involved

- `README.md`
- `docs/CHANNEL-WORK.md` (status note only)
- `docs/STRUCTURED-CONCURRENCY-TASKS.md` (status note only)
- `WORK.md` (if used)

## Suggested Agent Batches

### Batch A (Decision + Primitive)

- Task 1
- Task 2

### Batch B (Implementation)

- Task 3
- Task 4

### Batch C (Verification + Docs)

- Task 5
- Task 6

## Ready-to-Use Agent Prompts

### Prompt: Task 1

"Use `docs/UNION.md`, `docs/CHANNEL-TASKS.md`, and `docs/STRUCTURED-CONCURRENCY-DESIGN.md` to finalize the union contract section in `docs/UNION.md`. Do not change public API names unless required. Keep output concise and decision-oriented."

### Prompt: Task 2

"Harden `StreamScope` and `Stream.ScopedAsync` to fully satisfy the union contract in `docs/UNION.md`. Keep public API minimal. Add/adjust focused primitive tests only if needed."

### Prompt: Task 3

"Integrate the union supervision model into concurrent operators in `StreamExtensions` (`FlatMap`, `MapOrdered`, `FlatMapOrdered`, related paths). Preserve ordering and backpressure behavior."

### Prompt: Task 4

"Integrate the union supervision model into channel boundaries in `ChannelExecution` (`PipeThroughChannel`, `RunOnChannel`, and evaluate `TeeToChannel` semantics). Keep phase 2/3 API shape intact."

### Prompt: Task 5

"Add a behavior-first test matrix for union semantics across `ScopedAsync`, concurrent operators, and channel boundaries. Ensure tests demonstrate completion ordering, cancellation, and failure propagation."

### Prompt: Task 6

"Update README and status docs to reflect implemented union semantics and avoid overclaiming. Keep roadmap statements precise and verifiable."

## Final Checklist

- one supervision model across structured + channel concurrency
- no phase 2/3 API churn without explicit semantic reason
- tests prove completion, cancellation, and failure semantics
- docs describe implemented behavior only
