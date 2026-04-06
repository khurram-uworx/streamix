# Concurrency Follow-up Tasks

## Purpose

This document breaks the gaps identified in [docs/CONCURRENCY-REVIEW.md](E:\khurram-uworx\streamix\docs\CONCURRENCY-REVIEW.md) into concrete, assignable tasks for coding agents so the 0.6 concurrency work can be closed with a defensible release contract.

## Suggested Execution Order

1. Task 1: Record and apply the chosen `Map` concurrency contract
2. Task 2: Align concurrency docs and README to the agreed contract
3. Task 3: Record the chosen LINQ/query-syntax concurrency scope
4. Task 4: Align LINQ docs to the chosen concurrency scope
5. Task 5: Make `FlatMapOrdered` buffering policy explicit
6. Task 6: Document ordered failure and buffering semantics

## Coordination Notes

- Task 1 decision is now settled. Agents should treat it as a contract-recording and implementation-alignment task, not a design exploration task.
- Task 3 decision is now settled. Agents should treat it as a contract-recording task, not a design exploration task.
- Task 4 should align documentation and examples to the settled LINQ scope instead of inventing new LINQ concurrency helpers.
- Task 5 decision is now settled. Agents should treat it as an implementation task to expose ordered buffering control, not a design exploration task.
- Task 2 and Task 6 can run in parallel only after the relevant product decisions are made.
- Shared files likely to cause merge conflicts:
  - `README.md`
  - `docs/CONCURRENCY.md`
  - `src/Streamix/IStream.cs`
  - `src/Streamix/Implementations/Stream.cs`
  - `src/Streamix/Implementations/ConnectableStream.cs`
  - `src/Streamix/Extensions/LinqExtensions.cs`

## Task 1: Record and Apply Chosen `Map` Concurrency Contract

### Priority

High

### Goal

Record the chosen 0.6 `Map` contract and update the code-facing contract documents so agents and reviewers use the same semantics.

### Why this exists

The current release plan says concurrency and ordering should be explicit and discoverable, but the shipped surface still splits `Map` semantics by overload shape rather than by operator name. The team has decided to keep the current overload-based API for 0.6 and make the contract explicit in docs and API wording.

### Decision required

None. The decision is already made:
- `Map(Func<T, TResult>)` is sequential and ordered
- `MapAwait(Func<T, ValueTask<TResult>>)` is sequential and ordered
- `Map(Func<T, Task<TResult>>, int maxConcurrency = int.MaxValue)` is concurrent and unordered
- `MapOrdered(Func<T, Task<TResult>>, int maxConcurrency)` is concurrent and ordered

### Scope

- review all existing `Map`, `MapAwait`, and `MapOrdered` overloads and confirm they match the agreed semantics
- update `docs/CONCURRENCY.md` with the chosen contract at the design level
- update wording in task-facing docs if needed so future agents do not reopen this decision
- identify any XML doc or README wording that Task 2 must change to reflect the settled contract

### Constraints

- keep the surface area unchanged for this task unless a clear contract mismatch is found
- treat overload-based semantics as the intentional 0.6 product decision
- preserve consistency between `IStream<T>` and `ConnectableStream<T>`

### Suggested implementation path

- start from the current overload matrix in `IStream<T>`
- write the settled decision into `docs/CONCURRENCY.md`
- adjust any nearby wording that still frames this as unresolved

### Acceptance criteria

- the chosen `Map` contract is written down unambiguously
- `docs/CONCURRENCY.md` no longer describes `Map()` in a way that conflicts with `IStream<T>`
- a follow-on agent can execute Task 2 without reopening API design

### Files likely involved

- `docs/CONCURRENCY.md`
- `src/Streamix/IStream.cs`
- `README.md`

## Task 2: Align README and API Docs with the Chosen `Map` Contract

### Priority

High

### Goal

Remove misleading concurrency claims from the README and XML docs so the public documentation matches the actual agreed API semantics.

### Why this exists

The current README describes `Map()` generically as unordered and fastest, which is not accurate for the sync overload and is only conditionally true for task-returning overloads.

### Scope

- update the concurrency table and surrounding prose in `README.md`
- update examples that currently imply a broader `Map` contract than the API actually provides
- update XML docs in `src/Streamix/IStream.cs` where wording is ambiguous or misleading
- ensure terminology is consistent across `Map`, `MapAwait`, `MapOrdered`, `FlatMap`, `ConcatMap`, and `FlatMapOrdered`

### Constraints

- do not document behavior that is not actually implemented
- do not broaden claims about backpressure or concurrency beyond what the code guarantees

### Suggested implementation path

- fix the operator summary section first
- then fix the dedicated concurrency section
- then update examples and XML comments

### Acceptance criteria

- README operator descriptions no longer conflict with `IStream<T>`
- XML docs describe ordering and concurrency precisely
- examples use operator names and overloads in ways that match the agreed 0.6 story

### Files likely involved

- `README.md`
- `src/Streamix/IStream.cs`
- `docs/CONCURRENCY.md`

## Task 3: Record Chosen LINQ / Query-Syntax Concurrency Scope for 0.6

### Priority

High

### Goal

Record the chosen 0.6 LINQ/query-syntax contract so agents do not reopen design work that has already been decided.

### Why this exists

The README promotes query comprehension and LINQ-style composition as first-class usage, but today the `SelectMany` extensions only route to unordered flattening. The team has decided that this is intentional for 0.6: LINQ is an additional convenience layer and should route to the fastest flattening path rather than mirror the full fluent concurrency-control surface.

### Decision required

None. The decision is already made:
- `SelectMany` and `SelectManyAsync` remain unordered flattening helpers
- LINQ/query syntax is not part of the full 0.6 explicit concurrency-control surface
- users who need explicit ordered or sequential concurrency control should use fluent Streamix operators such as `FlatMap`, `ConcatMap`, and `FlatMapOrdered`

### Scope

- review current `SelectMany` and `SelectManyAsync` overloads and confirm they match the chosen 0.6 contract
- update `docs/CONCURRENCY.md` so the LINQ scope is explicitly documented
- identify README and examples wording that Task 4 must align with this settled contract

### Constraints

- preserve current LINQ surface area for 0.6
- treat fastest-path unordered flattening as the intentional LINQ behavior
- do not propose new LINQ helper names in this task

### Suggested implementation path

- catalog the current LINQ extension surface first
- write the settled LINQ scope into `docs/CONCURRENCY.md`
- adjust any nearby wording that still suggests this is unresolved

### Acceptance criteria

- the chosen LINQ/query-syntax scope is written down unambiguously
- `docs/CONCURRENCY.md` reflects that LINQ routes to unordered fastest-path flattening
- a follow-on agent can execute Task 4 without reopening API design

### Files likely involved

- `src/Streamix/Extensions/LinqExtensions.cs`
- `README.md`
- `docs/CONCURRENCY.md`

## Task 4: Align LINQ Docs to the Chosen Concurrency Scope

### Priority

Medium

### Goal

Make the LINQ/query-syntax story consistent with the 0.6 concurrency contract by updating documentation and examples to reflect the settled scope.

### Scope

- update README and docs to say LINQ/query syntax does not expose the full concurrency-control model for 0.6
- ensure examples do not imply ordered/sequential LINQ support unless it exists
- keep `SelectMany` and `SelectManyAsync` described as the unordered fastest-path helpers

### Constraints

- keep the current LINQ extension surface unchanged
- avoid implying that additional LINQ concurrency helpers are planned for this task

### Suggested implementation path

- update the explanatory docs first
- then update README examples and wording
- only touch tests if an existing test encodes contradictory documentation assumptions

### Acceptance criteria

- LINQ/query-syntax behavior is clearly documented as limited for concurrency control in 0.6
- there is no remaining ambiguity between README messaging and the extension surface
- examples consistently steer ordered/sequential use cases to fluent operators

### Files likely involved

- `src/Streamix/Extensions/LinqExtensions.cs`
- `README.md`
- `docs/CONCURRENCY.md`

## Task 5: Expose `FlatMapOrdered` Buffering Control

### Priority

High

### Goal

Remove the hidden magic-number behavior from `FlatMapOrdered` by exposing buffering as an intentional user-facing control.

### Why this exists

`FlatMapOrdered` currently hard-codes a per-inner channel capacity of `16`, which materially affects throughput, memory, and stalling under ordered concurrent execution. The team has decided Streamix should not hide this kind of production behavior behind an undocumented constant.

### Decision required

None. The decision is already made:
- `FlatMapOrdered` should expose ordered buffering control as part of the public API
- Streamix should not keep this behavior hidden behind a hard-coded internal constant
- the implementation should remain bounded and preserve ordered output guarantees
- use a minimal parameter-based API change for 0.6 rather than introducing a new options type

### Settled API direction

Use this shape for 0.6 unless a concrete implementation constraint forces a narrow adjustment:
- `IStream<TResult> FlatMapOrdered<TResult>(Func<T, IStream<TResult>> selector, int maxConcurrency = int.MaxValue, int maxBufferedItemsPerInner = 16)`

Parameter intent:
- `maxConcurrency`: maximum number of active inner streams
- `maxBufferedItemsPerInner`: maximum number of items each later inner may buffer while waiting for earlier inners to drain

Parameter rules:
- both parameters should be optional on the public signature
- both parameters must be validated as greater than `0`
- no hidden fallback constant should remain in the implementation path once the parameter is introduced

Design rationale:
- it matches the existing `FlatMapOrdered` signature closely and minimizes churn
- it exposes the actual production control users need without introducing an options object prematurely
- it keeps the buffering bound explicit at the call site
- it preserves Streamix’s existing ergonomics by keeping concurrency-related knobs optional

### Scope

- review the current `FlatMapOrdered` implementation in both `Stream<T>` and `ConnectableStream<T>`
- add the agreed public API shape for ordered buffering control
- implement the chosen public option consistently in both implementations
- add or update tests that validate the intended buffering/backpressure behavior

### Constraints

- preserve ordered output guarantees
- do not introduce unbounded buffering
- keep `Stream<T>` and `ConnectableStream<T>` behavior aligned
- avoid adding an API shape that is inconsistent with existing Streamix naming and parameter conventions
- prefer the settled three-parameter overload over introducing a new options object in this task
- do not introduce extra overload clutter if one optional-parameter signature is sufficient

### Suggested implementation path

- start by locating every ordered-inner buffer capacity assumption in the current implementations
- add `maxBufferedItemsPerInner` to the relevant `FlatMapOrdered` surface
- thread the parameter through both implementations and remove the hard-coded constant
- add focused tests around bounded buffering and ordered draining behavior

### Acceptance criteria

- there is no unexplained hard-coded inner buffer policy in `FlatMapOrdered`
- ordered buffering behavior is visible as public API, not just documentation
- the public signature exposes `maxBufferedItemsPerInner` explicitly
- both `maxConcurrency` and `maxBufferedItemsPerInner` validate input consistently
- the default values are documented explicitly and are not hidden only in implementation code
- tests cover the intended ordered/backpressure behavior well enough for future refactors

### Files likely involved

- `src/Streamix/Implementations/Stream.cs`
- `src/Streamix/Implementations/ConnectableStream.cs`
- `src/Streamix/IStream.cs`
- `src/Streamix.Tests/ConcurrencyTests.cs`
- `README.md`
- `docs/CONCURRENCY.md`

## Task 6: Document Ordered Failure, Cancellation, and Reordering Semantics

### Priority

Medium

### Goal

Make the semantics of ordered operators explicit, especially fail-fast behavior, buffering, and what “ordered” means when earlier work is slow or later work fails first.

### Why this exists

The current docs describe ordering in broad terms, but they do not explain operational semantics that matter in production, such as whether ordered operators fail immediately or only when the blocked earlier item/inner is reached.

### Scope

- inspect `MapOrdered` and `FlatMapOrdered` behavior for error timing, cancellation, and buffering interactions
- document the actual semantics in `README.md` and `docs/CONCURRENCY.md`
- add or tighten tests if the behavior is intentional but currently under-specified

### Constraints

- document the actual behavior, not the idealized behavior
- if the team wants different semantics, split that into a follow-up implementation task instead of hiding it in docs work

### Suggested implementation path

- capture the current behavior with focused tests first if it is not already covered
- then write concise semantic notes in the concurrency docs

### Acceptance criteria

- ordered operator docs clearly explain output ordering, buffering, and failure timing
- tests pin the intended semantics where the implementation choice is non-obvious
- release messaging no longer leaves these behaviors to inference

### Files likely involved

- `src/Streamix/Implementations/Stream.cs`
- `src/Streamix/Implementations/ConnectableStream.cs`
- `src/Streamix.Tests/ConcurrencyTests.cs`
- `README.md`
- `docs/CONCURRENCY.md`

## Suggested Agent Handout Batches

### Batch A: decision-critical

- Task 1
- Task 3

### Batch B: implementation

- Task 5
- Task 6

### Batch C: docs and contract cleanup

- Task 2
- Task 4
- Task 6

## Final Checklist

- every task has a clear owner-sized scope
- every task has acceptance criteria
- decision-gate tasks are clearly marked
- likely files are listed to reduce agent search time
- execution order reflects real dependencies
