# EF Stream

## Purpose

This document is the canonical carry-forward source for Streamix Entity Framework integration work.

It replaces the planning and review context that had been retained in `docs/EF-STREAMS-REVIEW.md` and the earlier implementation-planning docs referenced from that review.

Use this file for:

- the current shipped EF integration contract
- what is already implemented and considered complete for v1
- what decisions remain intentionally deferred
- execution-ready tasks that can be handed to coding agents for later phases

## Current Status

EF integration is implemented for the current release baseline.

The following are already landed in code, tests, and product docs:

- EF integration lives in `Streamix.Extensions`, not core `Streamix`
- public entry points are `EfStream.From(...)` and `Func<DbContext>.ToStream(...)`
- the API shape enforces the lifetime rule that query build and execution share the same `DbContext` instance
- the v1 execution path materializes with `ToListAsync(cancellationToken)` before yielding downstream items
- factory-based context ownership creates and disposes one context per subscription
- integration tests exist in `src/Streamix.Tests/EfStreamTests.cs`
- public documentation is aligned in `README.md`, `GETTING-STARTED.md`, `ARCHITECTURE.md`, and `src/Streamix.Extensions/README.md`

## What Still Needs Doing

The remaining EF stream work is next-phase design and refinement work, not unfinished v1 implementation.

The main carry-forward areas are:

1. Streamed execution mode design
   An explicit opt-in `AsAsyncEnumerable`-style execution path is still deferred and should be treated as a new API/behavior decision rather than a small implementation tweak.

2. Lifetime and unit-of-work guidance
   The factory-per-subscription model is shipped, but guidance for transaction-oriented or caller-managed context patterns can still be clarified.

3. Materialization and memory guidance
   Buffered execution is the baseline, but larger-query guidance or helper APIs for chunking/paging may become useful in later phases.

4. Provider-caveat documentation
   Any future streamed mode must be documented with concrete caveats around ordering, cancellation timing, and error timing differences by provider.

## Shipped Semantic Contract

- EF integration remains outside core `Streamix` and belongs in `Streamix.Extensions`.
- Public EF entry points are:
  - `EfStream.From(Func<DbContext, IQueryable<T>>, Func<DbContext>, ...)`
  - `Func<DbContext>.ToStream(Func<DbContext, IQueryable<T>>, ...)`
- The lifetime rule is non-negotiable: query construction and query execution must use the same `DbContext` instance.
- Buffered execution is the v1 baseline: queries execute via `ToListAsync(cancellationToken)` and items are yielded after materialization.
- Full materialization happens per subscription before the first downstream item is observed.
- Factory-based execution creates and disposes one context per subscription.

## Deferred Decisions That Must Be Preserved

- Caller-owned context overloads are intentionally not part of v1.
- Streamed query execution is deferred to a later explicit opt-in mode; it must not silently replace buffered default behavior.
- Provider caveats for streamed mode must be documented before release of such a mode.
- Buffered mode remains the compatibility baseline unless a deliberate API decision changes it.

## Non-Goals And Boundaries

- Do not turn Streamix into an ORM replacement.
- Do not introduce a new query DSL on top of EF.
- Do not hide EF behavior behind a fake database-abstraction layer.
- Do not pull EF references into core `Streamix`.
- Do not add streamed execution without disposal, cancellation, and error-propagation coverage.

## Release Planning Guidance

- Treat streamed mode as a new API-shape decision first, then an implementation task.
- Keep backward compatibility anchored on buffered default behavior.
- Preserve the lifetime rule in every future overload or extension.
- If caller-owned context support is ever added, document ownership and disposal semantics as part of the public contract, not as incidental XML docs.
- Add provider-caveat documentation alongside any streamed-mode work, not after it.

## Suggested Execution Order

1. Task 1: decide the API shape for opt-in streamed execution
2. Task 2: implement streamed execution with parity and safety tests
3. Task 3: document provider caveats and consumer guidance
4. Task 4: decide whether caller-owned context support should exist
5. Task 5: evaluate batching or memory-oriented helpers if real usage justifies them

## Coordination Notes

- Task 1 is the main decision gate because it controls compatibility and the public contract for future EF behavior.
- Task 2 should not start until Task 1 settles the API shape and default-behavior story.
- Task 3 can begin once Task 1 is stable, but it should be finalized alongside Task 2 so docs match the implementation.
- Task 4 depends partly on the outcome of Task 1 because streamed execution and caller-owned lifetimes interact.
- Shared files likely to create merge conflicts are `README.md`, `GETTING-STARTED.md`, `ARCHITECTURE.md`, and `src/Streamix.Extensions/README.md`.

## Task 1: Decide The API Shape For Streamed Execution

### Priority

High

### Goal

Choose a clear, backward-compatible public API for an opt-in streamed EF execution mode.

### Why this exists

The review preserves streamed mode as a likely Phase 2 direction, but the repo should not drift into implementation before deciding whether the API is a new entry point, an execution-mode parameter, or another explicit opt-in shape.

### Decision required

Decide how streamed mode is expressed publicly while preserving buffered mode as the default baseline.

Possible decision space includes:

- a separate streamed factory/API path
- an explicit execution-mode parameter
- another opt-in surface that keeps buffered behavior unchanged unless requested

### Scope

- review existing `EfStream` and `ToStream(...)` surface area
- evaluate candidate API shapes against compatibility, discoverability, and semantic clarity
- record the preferred shape and the reasoning
- define the intended buffered-versus-streamed contract in public terms

### Constraints

- buffered execution must remain the default baseline
- the lifetime rule must remain enforceable
- avoid adding API shape that obscures disposal, cancellation, or provider caveats

### Suggested implementation path

- inspect current `Streamix.Extensions` EF APIs and docs
- compare one or two small candidate shapes rather than reopening the whole EF surface
- record the chosen design in docs before runtime work begins

### Acceptance criteria

- one preferred streamed-mode API shape is documented
- the compatibility story is explicit: buffered stays default, streamed is opt-in
- the design records how lifetime/ownership semantics remain safe and understandable

### Files likely involved

- `docs/EF-STREAM.md`
- `ARCHITECTURE.md`
- `src/Streamix.Extensions/README.md`
- `src/Streamix.Extensions`

## Task 2: Implement Streamed Execution With Safety Tests

### Priority

High

### Goal

Add the chosen opt-in streamed execution path without regressing the shipped buffered baseline.

### Why this exists

If streamed mode is approved, it needs to be introduced as a deliberate alternative execution behavior with explicit safety coverage, not as an internal swap of `ToListAsync` for `AsAsyncEnumerable`.

### Scope

- implement the streamed execution path in `Streamix.Extensions`
- preserve existing buffered behavior as default
- add tests for cancellation, disposal, and error propagation in streamed mode
- add parity tests where buffered and streamed behavior should match
- document intentional behavior differences where parity should not be expected

### Constraints

- do not weaken the current lifetime rule
- do not silently change the semantics of existing public APIs
- do not ship without tests covering disposal/cancellation/error timing

### Suggested implementation path

- implement the smallest surface implied by Task 1
- start from current `EfStreamTests` and extend with mode-specific tests
- verify behavior for success, cancellation, exception propagation, and disposal

### Acceptance criteria

- opt-in streamed mode exists behind the agreed API shape
- existing buffered default behavior remains unchanged
- tests cover streamed-mode cancellation, error propagation, and disposal correctness
- any documented differences between buffered and streamed behavior are intentional and test-backed

### Files likely involved

- `src/Streamix.Extensions`
- `src/Streamix.Tests/EfStreamTests.cs`
- `ARCHITECTURE.md`
- `src/Streamix.Extensions/README.md`

## Task 3: Document Provider Caveats And Usage Guidance

### Priority

Medium

### Goal

Make EF stream behavior understandable for consumers, especially if streamed mode is introduced.

### Why this exists

The review explicitly calls out provider differences and behavior-timing caveats as important context that must not be lost, particularly for a future streamed execution mode.

### Scope

- document buffered baseline semantics clearly
- if streamed mode exists, add a concise caveat matrix for ordering, cancellation timing, and error timing
- add usage guidance for typical read/query scenarios
- keep the docs honest about provider-sensitive behavior

### Constraints

- avoid implying identical provider behavior where that is not guaranteed
- keep EF guidance in the extensions docs and architecture/docs pages, not in core stream docs alone

### Suggested implementation path

- update `src/Streamix.Extensions/README.md` first for integration-specific guidance
- add concise summary wording to `README.md` or `GETTING-STARTED.md` only as needed
- prefer one clear caveat section over scattered warnings

### Acceptance criteria

- consumers can tell the difference between buffered and streamed EF execution
- provider-sensitive caveats are documented in one easy-to-find place
- docs do not overpromise database/provider uniformity

### Files likely involved

- `src/Streamix.Extensions/README.md`
- `README.md`
- `GETTING-STARTED.md`
- `ARCHITECTURE.md`
- `docs/EF-STREAM.md`

## Task 4: Decide Whether To Support Caller-Owned Contexts

### Priority

Medium

### Goal

Make an explicit product decision on whether caller-owned `DbContext` overloads should ever be part of the public EF integration surface.

### Why this exists

Caller-owned contexts are the most obvious extension point for transaction and unit-of-work scenarios, but they carry lifetime and disposal complexity that v1 intentionally avoided.

### Decision required

Decide whether caller-owned context support should:

- remain unsupported
- be documented as an external pattern only
- be added as a public overload with explicit ownership/disposal semantics

### Scope

- evaluate real scenarios that the factory-based model does not serve well
- assess whether documentation-only guidance is sufficient
- if proposing public support, define exact ownership and disposal rules

### Constraints

- no ambiguous disposal semantics
- no overloads that make it easy to build the query on one context and execute on another

### Suggested implementation path

- start with a docs/product decision memo
- only move to API work if a concrete scenario justifies the complexity

### Acceptance criteria

- the repo has an explicit recorded decision on caller-owned contexts
- if support remains deferred, that is stated clearly
- if support is approved, the ownership contract is precise enough to implement and test

### Files likely involved

- `docs/EF-STREAM.md`
- `ARCHITECTURE.md`
- `src/Streamix.Extensions/README.md`

## Task 5: Evaluate Batching Or Memory-Oriented Helpers

### Priority

Low

### Goal

Decide whether large-result guidance or helper APIs are needed beyond the current buffered baseline.

### Why this exists

The review preserves memory/materialization follow-up as a valid Phase 2 direction, but it should be driven by real pressure from large queries rather than speculative helper design.

### Scope

- identify whether existing buffering behavior creates practical memory pressure in expected usage
- evaluate whether docs guidance is enough or helper APIs are warranted
- if justified, define a narrow batching/chunking direction that aligns with Streamix semantics

### Constraints

- do not add helper APIs without a concrete scenario
- avoid turning EF integration into a general-purpose pagination framework

### Suggested implementation path

- start with usage examples, stress scenarios, or benchmark notes
- only propose APIs if docs guidance is insufficient

### Acceptance criteria

- the repo records whether batching/memory follow-up is needed now, later, or not at all
- any proposed direction is narrowly scoped and consistent with current EF boundaries

### Files likely involved

- `docs/EF-STREAM.md`
- `WORK.md`
- `src/Streamix.Extensions/README.md`

## Additional Tasks

Recommended pattern for future EF follow-up:

- separate API-shape decisions from runtime implementation
- keep provider-caveat documentation coupled to behavior changes
- treat lifetime and ownership changes as contract work, not convenience cleanup

## Suggested Agent Handout Batches

### Batch A: decision-critical

- Task 1
- Task 4

### Batch B: implementation

- Task 2

### Batch C: docs and planning

- Task 3
- Task 5

## Final Checklist

- the shipped EF integration contract is preserved in one durable file
- remaining work is framed as later-phase design/refinement, not as unfinished v1 delivery
- decision-gate tasks are clearly separated from implementation work
- tasks are small enough to hand to coding agents with acceptance criteria
