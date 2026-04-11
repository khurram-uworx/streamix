# Channel Integration Task Breakdown

## Purpose

This document breaks the "deeper integration with `System.Threading.Channels`" roadmap slice into concrete, assignable tasks.

Current state:

- Phase 1 is complete:
  - `Stream.FromChannel(Channel<T>)`
  - `Stream.FromChannel(ChannelReader<T>)`
  - `IStream<T>.ToChannel(ChannelWriter<T>, ...)`
  - terminal `ToChannel(...)` overloads
- Phase 2 is complete:
  - `ChannelBackpressureMode`
  - `PipeThroughChannel(...)`
  - `RunOnChannel(...)`
  - bounded `ToChannel(capacity, mode, ...)`
  - `Stream.MergeChannels(...)`
- Phase 3 is complete:
  - `TeeToChannel(...)`
  - channel-backed `Buffer(count, capacity, mode)`
  - channel-backed `Window(count, capacity, mode)`

What remains is the phase-4 differentiation work: make the channel runtime story more explicit without turning Channels into a competing composition model.

## Suggested Execution Order

1. Task 1: Define the phase-4 contract and non-goals
2. Task 2: Decide whether to expose execution-graph diagnostics or keep phase 4 limited to structured-concurrency integration
3. Task 3: Implement the chosen channel execution-graph or supervision primitive
4. Task 4: Integrate the primitive with concurrent operators and channel boundaries
5. Task 5: Add behavioral tests for observability, supervision, and completion semantics
6. Task 6: Update README, roadmap wording, and work-log notes

## Coordination Notes

- Task 1 is a decision gate. Do not start broad implementation until the public contract is settled.
- Task 2 is also a decision gate if phase 4 is split into multiple deliverables.
- Task 3 and Task 4 will touch the same core files and should be owned by one agent or sequenced carefully.
- Task 5 can begin once Task 1 is settled and the implementation direction is stable.
- Task 6 should wait until naming and behavior are final.
- Shared files with merge-conflict risk:
  - `src/Streamix/IStream.cs`
  - `src/Streamix/Stream.cs`
  - `src/Streamix/Implementations/Stream.cs`
  - `src/Streamix/Implementations/ConnectableStream.cs`
  - `src/Streamix/ChannelExecution.cs`
  - `src/Streamix.Tests/ConcurrencyTests.cs`
  - `src/Streamix.Tests/StreamTests.cs`
  - `README.md`
  - `WORK.md`

## Task 1: Define Phase-4 Channel Contract

### Priority

High

### Goal

Choose and document the minimum public semantics required to call the next slice of channel work complete.

### Why this exists

Phases 2 and 3 already delivered strong channel boundaries and batching. Phase 4 should not become a grab-bag of low-level channel helpers. The repo needs an explicit decision about whether phase 4 is primarily about observability, structured supervision, or both.

### Decision required

Decide the user-facing phase-4 target:

- Is the primary deliverable an execution-graph / diagnostic view?
- Is the primary deliverable structured concurrency over channel-backed execution boundaries?
- Are both required for phase 4, or should one be deferred?
- Is the phase-4 API diagnostic-only, behavioral, or both?

### Scope

- Define what "phase 4" means in Streamix terms, not generic reactive-system terms.
- State what is already complete so the roadmap does not re-open solved phase-2/3 work.
- Define the non-goals for the first phase-4 cut.
- Pick the smallest API shape that still delivers a clear differentiator.

### Constraints

- Do not expose raw channel machinery broadly through the composition layer.
- Keep `IStream<T>` as the primary user mental model.
- Avoid adding APIs that duplicate `PipeThroughChannel(...)` or `RunOnChannel(...)` under new names.

### Suggested implementation path

- Start by writing 2-3 concrete usage examples.
- Choose whether those examples need introspection, supervision, or both.
- Prefer one explicit boundary model over multiple weak optional flags.

### Acceptance criteria

- A documented phase-4 proposal exists with concrete examples.
- The proposal states what stays roadmap-only versus what will be implemented now.
- The proposal is specific enough that implementation tasks can proceed without reopening API semantics.

### Files likely involved

- `docs/CHANNEL.md`
- `README.md`
- `WORK.md`

## Task 2: Resolve Phase-4 Scope Split

### Priority

High

### Goal

Decide whether phase 4 should ship as one slice or split into "execution graph" and "structured supervision" sub-phases.

### Why this exists

The current channel doc mentions an optional execution-graph view and structured concurrency direction. Those are related but not identical deliverables, and combining them blindly risks a large ambiguous feature.

### Decision required

- Should phase 4A be diagnostics/graph only?
- Should phase 4B be structured supervision only?
- If both ship together, what is the minimum cross-cutting primitive?

### Scope

- Compare the API weight and implementation cost of each direction.
- Identify which one has stronger product value for the next milestone.
- Record the decision in `WORK.md`.

### Acceptance criteria

- The phase-4 split decision is documented.
- Downstream implementation tasks clearly target one chosen direction.
- The roadmap wording is updated if phase 4 is split.

### Files likely involved

- `WORK.md`
- `README.md`

## Task 3: Implement Core Phase-4 Primitive

### Priority

High

### Goal

Add the internal and public primitive that powers the chosen phase-4 direction.

### Scope

- If diagnostics-focused:
  - add an execution-boundary descriptor or graph snapshot primitive
  - capture channel boundary metadata without changing pipeline semantics
- If supervision-focused:
  - add a parent/child lifetime primitive for channel-backed worker boundaries
  - make boundary completion and child tracking explicit

### Constraints

- Reuse existing channel boundary machinery in `ChannelExecution.cs` where possible.
- Preserve current ordering, cancellation, and backpressure guarantees.
- Avoid creating separate implementations for `Stream<T>` and `ConnectableStream<T>` if a shared primitive works.

### Suggested implementation path

- Start from the explicit boundaries that already exist: `PipeThroughChannel(...)`, `RunOnChannel(...)`, `TeeToChannel(...)`, and channel-backed batching.
- Add one shared internal primitive that all relevant boundaries can use.
- Keep the first public surface small.

### Acceptance criteria

- One clear production primitive exists for the selected phase-4 goal.
- Existing phase-2/3 APIs continue to behave the same unless the proposal explicitly changes them.
- The primitive is usable by multiple channel-backed boundaries.

### Files likely involved

- `src/Streamix/ChannelExecution.cs`
- `src/Streamix/IStream.cs`
- `src/Streamix/Stream.cs`
- `src/Streamix/Implementations/Stream.cs`
- `src/Streamix/Implementations/ConnectableStream.cs`

## Task 4: Integrate Phase-4 Primitive With Concurrent Operators

### Priority

High

### Goal

Wire the phase-4 primitive into the relevant channel-backed and concurrent operator paths.

### Scope

- Integrate the primitive with `RunOnChannel(...)`.
- Integrate it with `PipeThroughChannel(...)`.
- Evaluate whether `FlatMap`, ordered concurrency, and terminal concurrency helpers should participate.
- Ensure nested channel-backed boundaries compose predictably.

### Constraints

- Do not regress existing tests for ordering or backpressure.
- Do not introduce duplicated supervision or duplicated metadata collection across nested boundaries.

### Acceptance criteria

- Relevant channel-backed operators participate consistently in the phase-4 model.
- Nested boundaries have defined and tested behavior.
- The implementation does not reopen the public API decisions from Task 1.

### Files likely involved

- `src/Streamix/ChannelExecution.cs`
- `src/Streamix/Implementations/Stream.cs`
- `src/Streamix/Implementations/ConnectableStream.cs`
- `src/Streamix/Extensions/TerminalExtensions.cs`

## Task 5: Add Behavioral Test Matrix

### Priority

High

### Goal

Prove the chosen phase-4 semantics with focused tests.

### Scope

- Add tests for success behavior.
- Add tests for cancellation and failure propagation.
- Add tests for nested boundary behavior.
- Add tests for completion ordering and teardown.
- If diagnostics-focused, add tests proving graph metadata matches executed boundaries.
- If supervision-focused, add tests proving parent boundaries do not complete before child work settles.

### Constraints

- Prefer observable coordination over timing-only assertions.
- Keep tests explicit about which semantics come from phase 2/3 versus phase 4.

### Acceptance criteria

- Tests fail against the pre-feature implementation and pass afterward.
- The suite covers success, cancellation, exception propagation, and boundary completion semantics.
- At least one test distinguishes the phase-4 behavior from the current phase-3 implementation.

### Files likely involved

- `src/Streamix.Tests/ConcurrencyTests.cs`
- `src/Streamix.Tests/StreamTests.cs`
- `src/Streamix.Tests/BatchOperatorTests.cs`
- `src/Streamix.Tests/ResourceSafetyTests.cs`

## Task 6: Update README And Roadmap

### Priority

Medium

### Goal

Document the phase-4 result accurately and keep roadmap claims honest.

### Scope

- Update the channel integration section in `README.md`.
- Add examples only for APIs that actually exist.
- Update the roadmap wording if phase 4 is fully or partially complete.
- Remove or qualify any statements in `docs/CHANNEL.md` that no longer match the implemented sequencing.

### Acceptance criteria

- README examples compile against the implemented APIs.
- Roadmap wording matches the actual repo state.
- Phase-2 and phase-3 work are described as complete rather than aspirational.

### Files likely involved

- `README.md`
- `docs/CHANNEL.md`
- `WORK.md`

## Suggested Agent Handout Batches

### Batch A: decision-critical

- Task 1
- Task 2

### Batch B: core implementation

- Task 3
- Task 4

### Batch C: tests and docs

- Task 5
- Task 6

## Final Checklist

- every task has a clear owner-sized scope
- every task has acceptance criteria
- decision-gate tasks are clearly marked
- likely files are listed to reduce agent search time
- execution order reflects real dependencies
