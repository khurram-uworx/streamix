# Concurrency

## Purpose

This document is the canonical carry-forward source for Streamix concurrency work.

It replaces the planning context that had been retained in `docs/CONCURRENCY-REVIEW.md` and earlier deleted source docs such as `docs/UNION.md`, `docs/STRUCTURED-CONCURRENCY-DESIGN.md`, `docs/STRUCTURED-CONCURRENCY-TASKS.md`, `docs/CHANNEL-TASKS.md`, and `docs/CHANNEL-WORK.md`.

Use this file for:

- the current concurrency and supervision contract
- what is already implemented and considered landed
- what follow-up work still exists
- execution-ready tasks that can be handed to coding agents

## Current Status

Concurrency implementation is substantially complete for the current product contract.

The following are already landed in code and tests:

- `Stream.ScopedAsync`, `IStreamScope`, and `StreamScope` establish the structured concurrency/supervision model
- concurrent operators participate in the same supervision flow
- channel execution boundaries participate in the same cancellation/failure/finalization model
- fail-fast first-fault propagation is the active failure policy
- ordered operators and channel-backed boundaries preserve intended ordering/backpressure semantics
- test coverage exists for success, cancellation, failure propagation, sibling cancellation, teardown, and nested supervision behavior
- the top-level README already exposes structured concurrency as part of the public story

## What Still Needs Doing

The remaining work is not core implementation completion. It is follow-up work in three areas:

1. Documentation refinement
   The public docs should more explicitly distinguish plain bounded concurrency such as `maxConcurrency` from supervised concurrency via `ScopedAsync` and supervised boundaries.

2. Unified verification narrative
   The repo has meaningful test coverage, but it does not yet present one clearly labeled concurrency/supervision matrix that makes the cross-cutting invariants easy to audit.

3. Channel-specific documentation cleanup
   `TeeToChannel(...)` is less explicitly documented than `PipeThroughChannel(...)` and `RunOnChannel(...)`, especially around completion and teardown semantics within the shared supervision model.

## Active Semantic Contract

- Use one supervision/lifetime model across structured concurrency, concurrent operators, and channel-backed execution boundaries.
- Keep `IStream<T>` as the primary user model. Channels are explicit execution boundaries, not a competing composition model.
- A supervised boundary does not complete until the parent body and all registered children have reached a terminal state.
- Parent or outer cancellation flows into supervised child work through linked cancellation.
- The v1 failure policy is fail-fast: the first non-cancellation fault cancels siblings, waits for settlement, then propagates that first fault.
- Boundary-caused `OperationCanceledException` is expected cancellation, not a new primary fault.
- Supervision integration must not weaken ordered operator guarantees, backpressure behavior, or resource teardown correctness.

## Non-Goals And Deferred Areas

- Do not introduce a broader channel-first composition model.
- Do not reopen the public API shape for concurrency/channel integration without a concrete semantic gap.
- Do not expand into a larger fault-policy matrix unless a real product need appears.
- Do not introduce exception aggregation as incidental cleanup; first-fault propagation is the current stable contract.
- Execution-graph diagnostics remain deferred unless future debugging or verification needs justify them.

## Release Planning Guidance

- Preserve the distinction between plain bounded concurrency and supervised concurrency in future docs and examples.
- Keep nested-scope transitive completion and cancellation behavior explicitly tested as the implementation evolves.
- Treat diagnostics or alternative fault policies as new contract work, not as routine cleanup.
- Keep channel-boundary docs precise about how `PipeThroughChannel(...)`, `RunOnChannel(...)`, and `TeeToChannel(...)` participate in supervision while preserving the stream-first mental model.

## Suggested Execution Order

1. Task 1: clarify the public concurrency contract in docs
2. Task 2: add a unified concurrency verification matrix or audit section
3. Task 3: tighten channel-boundary documentation, especially `TeeToChannel(...)`
4. Task 4: perform a deferred diagnostics decision pass only if the prior work exposes a real need

## Coordination Notes

- Task 1 is the main decision gate because it locks how the product explains supervision versus bounded concurrency.
- Tasks 2 and 3 can proceed in parallel once Task 1 is settled enough to avoid contradictory terminology.
- Task 4 should not begin unless Tasks 1 through 3 uncover a concrete observability/debugging gap.
- Shared files likely to create merge conflicts are `README.md` and any architecture or docs index pages that summarize concurrency behavior.

## Task 1: Clarify The Public Concurrency Contract

### Priority

High

### Goal

Make the public docs explicitly distinguish bounded operator concurrency from structured supervised concurrency.

### Why this exists

The implementation is already aligned around a unified supervision model, but the current public explanation still leaves room for readers to conflate `maxConcurrency` with `ScopedAsync`-style supervision semantics.

### Decision required

Confirm the preferred public framing for concurrency in v1:

- `maxConcurrency` is an operator-level throughput/parallelism control
- `ScopedAsync` and supervised boundaries define lifetime, failure, and settle semantics

### Scope

- review current concurrency wording in `README.md`
- decide where the primary explanation belongs between `README.md` and deeper architecture docs
- add concise wording that contrasts throughput control with supervision/lifetime control
- ensure examples do not imply that every concurrent operator invocation is equivalent to entering an explicit scope

### Constraints

- keep the public API contract aligned with existing implementation
- avoid promising diagnostics, aggregation, or additional policies that do not exist
- keep examples truthful to the current API surface

### Suggested implementation path

- start from the structured concurrency section already present in `README.md`
- add one short contrast section or note covering `maxConcurrency` versus supervision
- if needed, add a deeper explanatory paragraph in architecture docs rather than overloading the README

### Acceptance criteria

- the docs explicitly distinguish bounded concurrency from supervision semantics
- the wording matches current implementation behavior and tests
- no examples overstate the role of channels or imply a channel-first user model

### Files likely involved

- `README.md`
- `ARCHITECTURE.md`
- `docs/CONCURRENCY.md`

## Task 2: Add A Unified Concurrency Verification Matrix

### Priority

Medium

### Goal

Make the cross-cutting concurrency contract easier to audit by collecting the important invariants into one clearly labeled section.

### Why this exists

Coverage exists today, but the evidence is spread across multiple suites and is not presented as one auditable matrix covering operators, channel boundaries, cancellation, failure, ordering, and teardown semantics.

### Scope

- identify the existing test coverage that proves the concurrency contract
- add a labeled verification matrix in docs, tests, or both
- map each major invariant to representative tests or sections
- call out any thin spots discovered during the audit

### Constraints

- prefer organizing and labeling existing coverage before creating broad new test suites
- if gaps are found, keep follow-up tasks narrowly scoped and evidence-based

### Suggested implementation path

- inspect `StreamScopeTests`, `ConcurrencyTests`, `ResourceSafetyTests`, and relevant `StreamTests`
- create a concise matrix organized by invariant, not by file
- link the matrix to operator categories and channel boundaries

### Acceptance criteria

- one clearly labeled concurrency/supervision matrix exists
- the matrix covers success, cancellation, first-fault propagation, sibling cancellation, ordering, and teardown/resource safety
- any uncovered invariants are identified explicitly instead of being implied

### Files likely involved

- `src/Streamix.Tests/StreamScopeTests.cs`
- `src/Streamix.Tests/ConcurrencyTests.cs`
- `src/Streamix.Tests/ResourceSafetyTests.cs`
- `src/Streamix.Tests/StreamTests.cs`
- `README.md`
- `ARCHITECTURE.md`
- `docs/CONCURRENCY.md`

## Task 3: Tighten Channel-Boundary Concurrency Documentation

### Priority

Medium

### Goal

Document channel-backed boundaries consistently, with explicit completion and teardown semantics for `TeeToChannel(...)`.

### Why this exists

`PipeThroughChannel(...)` and `RunOnChannel(...)` fit more visibly into the supervision story today, while `TeeToChannel(...)` is easier to overlook even though its completion and teardown behavior matters to the same contract.

### Scope

- review the current docs for `PipeThroughChannel(...)`, `RunOnChannel(...)`, and `TeeToChannel(...)`
- add precise wording for `TeeToChannel(...)` completion, teardown, and supervision participation
- ensure the docs reinforce that channels are execution boundaries inside a stream-first model

### Constraints

- do not invent new semantics
- do not drift into a channel-first composition narrative

### Suggested implementation path

- align wording for all three channel-related APIs
- keep the behavior explanation short and contract-focused
- prefer one shared channel-boundary section over scattered partial notes

### Acceptance criteria

- `TeeToChannel(...)` documentation is explicit about completion and teardown behavior
- the channel-boundary docs use consistent terminology across all related APIs
- the resulting documentation stays aligned with the unified supervision contract

### Files likely involved

- `README.md`
- `ARCHITECTURE.md`
- `docs/CONCURRENCY.md`

## Task 4: Decide Whether Diagnostics Work Is Actually Needed

### Priority

Low

### Goal

Make an explicit go/no-go decision on execution-graph or supervision diagnostics instead of letting it remain an undefined maybe.

### Why this exists

Diagnostics were intentionally deferred, which is still the right default. A small decision pass is useful later only if documentation or verification work exposes a real debugging blind spot.

### Decision required

Is there a concrete maintainability, debugging, or test-audit problem that current tracing/logging/test structure cannot handle without new diagnostics support?

### Scope

- review findings that emerge from Tasks 1 through 3
- identify whether any real debugging gaps remain
- either document that diagnostics stay deferred or open a new scoped work item with a concrete problem statement

### Constraints

- no implementation work unless a specific need is demonstrated
- no speculative diagnostics architecture

### Suggested implementation path

- treat this as a short investigation memo or explicit backlog decision
- if the answer is “not needed,” document that clearly and stop there

### Acceptance criteria

- the repo has an explicit recorded decision about diagnostics for the next phase
- any follow-up work is tied to a concrete problem statement rather than generic future-proofing

### Files likely involved

- `docs/CONCURRENCY.md`
- `WORK.md`
- `README.md`

## Additional Tasks

Recommended pattern for any future concurrency follow-up:

- separate docs clarification from runtime behavior changes
- separate audit/matrix work from new test authoring unless a real gap is found
- treat fault-policy expansion or diagnostics as new contract work

## Suggested Agent Handout Batches

### Batch A: decision-critical

- Task 1

### Batch B: implementation

- Task 2
- Task 3

### Batch C: follow-up planning

- Task 4

## Final Checklist

- the remaining concurrency work is documented as follow-up rather than missing implementation
- the active semantic contract is preserved in one durable file
- decision-gate work is clearly separated from implementation/doc cleanup
- tasks are small enough to hand to coding agents with acceptance criteria
