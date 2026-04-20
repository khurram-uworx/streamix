# Structured Concurrency Task Breakdown

## Purpose

This document breaks the "structured concurrency support" roadmap item into concrete, assignable tasks.

Current state:

- Streamix already supports bounded and ordered/unordered concurrency in operators such as `Map`, `MapOrdered`, `FlatMap`, and `FlatMapOrdered`.
- Streamix already propagates cancellation and disposes per-subscription resources correctly.
- Streamix does not yet expose a clear structured-concurrency model with an explicit parent/child lifetime boundary for concurrent work.

This breakdown keeps that distinction explicit so the roadmap item is only closed when the public contract and behavior actually exist.

## Suggested Execution Order

1. ✅ Task 1: Define the structured concurrency contract and minimal API
2. ✅ Task 2: Implement scope/lifetime primitives in the core library
3. ✅ Task 3: Integrate structured concurrency with stream operators and terminals
4. ✅ Task 4: Add behavioral tests for cancellation, failure, and completion semantics
5. ✅ Task 5: Update README and roadmap language

## Coordination Notes

- Task 1 is a decision gate. Do not start broad implementation until the public API and semantics are settled.
- Task 2 and Task 3 are tightly related and will likely touch shared core files. They should either be owned by one agent or sequenced carefully.
- Task 4 can begin in parallel once Task 1 is settled and the implementation direction is stable.
- Task 5 should wait until the behavior and naming are finalized.
- Shared files with merge-conflict risk:
  - `src/Streamix/IStream.cs`
  - `src/Streamix/Stream.cs`
  - `src/Streamix/Implementations/Stream.cs`
  - `src/Streamix/Implementations/ConnectableStream.cs`
  - `src/Streamix.Tests/ConcurrencyTests.cs`
  - `README.md`

## ✅ Task 1: Define Structured Concurrency Contract

### Priority

High

### Goal

Choose and document the minimum public API and runtime semantics required for Streamix to claim structured concurrency support.

### Why this exists

The repo currently has concurrency controls but no explicit structured-concurrency abstraction. Without a decision gate, implementation work risks shipping another concurrency helper while still leaving the roadmap item ambiguous.

### Decision required

Decide the user-facing model. At minimum, answer:

- Is the primary abstraction a scope object, a builder callback, or operator-level overloads?
- What counts as a child task/child stream under supervision?
- What is the failure policy: cancel siblings on first failure, aggregate failures, or make policy configurable?
- What completion rule makes a scope complete?
- How does the model interact with `ConnectableStream`, `Using`, and terminal operations?

### Scope

- Define what "structured concurrency" means for Streamix in .NET terms.
- Pick a minimal API shape that fits the current fluent surface area.
- Define cancellation, failure, ordering, and disposal semantics.
- Identify non-goals for the first version.
- Document the chosen contract in a short design note or work log.

### Constraints

- Keep the initial API small and idiomatic for `IAsyncEnumerable<T>`-style usage.
- Do not relabel existing `maxConcurrency` operators as structured concurrency unless they gain explicit scope semantics.
- Avoid introducing surface area that requires a large later cleanup.

### Suggested implementation path

- Start from a minimal "scope owns spawned work and does not complete until children settle" model.
- Prefer a single explicit parent lifetime boundary over scattered optional flags.
- Reuse existing cancellation and channel machinery where possible.

### Acceptance criteria

- A documented API proposal exists with examples.
- Failure, cancellation, and completion semantics are explicitly stated.
- The proposal identifies which existing roadmap and README statements will change.
- The proposal is concrete enough that implementation tasks can proceed without re-litigating semantics.

### Files likely involved

- `README.md`
- `docs/IDEA.md`
- `WORK.md`

## ✅ Task 2: Implement Core Scope/Lifetime Primitive

### Priority

High

### Goal

Add the internal and public primitives needed to represent a structured concurrency scope.

### Scope

- Introduce the chosen scope/supervisor abstraction.
- Implement parent-linked cancellation and deterministic child tracking.
- Ensure scope completion waits for all child work to finish or cancel.
- Ensure disposal/cancellation paths are idempotent and exception-safe.

### Constraints

- Preserve nullable correctness and existing coding conventions.
- Avoid duplicating similar scope machinery across `Stream` and `ConnectableStream` if a shared primitive can be used.

### Suggested implementation path

- Create one internal primitive that tracks child operations and final completion.
- Make child registration explicit so operator implementations can participate consistently.
- Keep failure behavior aligned with Task 1 decisions rather than embedding ad hoc first-failure logic in each operator.

### Acceptance criteria

- A scope primitive exists in production code with deterministic completion semantics.
- Child cancellation propagates from the parent scope.
- Failure behavior matches the Task 1 contract.
- Basic unit tests cover the primitive directly if the type is testable in isolation.

### Files likely involved

- `src/Streamix/Stream.cs`
- `src/Streamix/IStream.cs`
- `src/Streamix/Implementations/Stream.cs`
- `src/Streamix/Implementations/ConnectableStream.cs`

## ✅ Task 3: Integrate Structured Concurrency Into Stream Operations

### Priority

High

### Goal

Ensure concurrent stream operations run inside the structured-concurrency model rather than only using local semaphores and linked tokens.

### Scope

- Update the relevant concurrent operators to register child work with the scope primitive.
- Evaluate whether terminals such as `ForEachAsync(maxConcurrency)` should also participate.
- Ensure nested concurrent operators compose predictably.
- Ensure resource-owning operators such as `Using` behave correctly when child work is still active.

### Constraints

- Do not regress current ordering and backpressure guarantees.
- Preserve current non-structured code paths if the selected API makes structured behavior opt-in for v1.

### Acceptance criteria

- At least one public entry point exposes structured concurrency behavior.
- Concurrent child operations are supervised by the selected scope model.
- Operator completion does not race ahead of in-scope child work.
- Failure/cancellation semantics remain consistent across `Stream` and `ConnectableStream`.

### Files likely involved

- `src/Streamix/IStream.cs`
- `src/Streamix/Stream.cs`
- `src/Streamix/Implementations/Stream.cs`
- `src/Streamix/Implementations/ConnectableStream.cs`
- `src/Streamix/Extensions/TerminalExtensions.cs`

## ✅ Task 4: Add Behavioral Test Matrix

### Priority

High

### Goal

Prove the structured-concurrency contract with tests that distinguish it from the existing concurrency operators.

### Scope

- Add tests for parent cancellation canceling active children.
- Add tests for child failure behavior according to the chosen policy.
- Add tests proving scope completion waits for children.
- Add tests for nested scopes or nested concurrent operators if supported.
- Add tests covering resource disposal ordering when child work is canceled or fails.

### Constraints

- Tests should make ordering and lifetime boundaries observable without relying on timing-only assertions where possible.
- Reuse existing concurrency test helpers if available.

### Acceptance criteria

- Tests fail against the pre-feature implementation and pass after the feature is implemented.
- The suite covers success, cancellation, exception propagation, and completion ordering.
- At least one test distinguishes plain bounded concurrency from structured concurrency semantics.

### Files likely involved

- `src/Streamix.Tests/ConcurrencyTests.cs`
- `src/Streamix.Tests/ResourceSafetyTests.cs`
- `src/Streamix.Tests/StreamTests.cs`
- `src/Streamix.Tests/Extensions/TerminalExtensionsTests.cs`

## ✅ Task 5: Update README And Roadmap

### Priority

Medium

### Goal

Document the structured concurrency feature accurately and remove the roadmap item only if the implemented semantics match the agreed contract.

### Scope

- Add a README section explaining the new model and how it differs from plain `maxConcurrency`.
- Update examples to show the intended entry point.
- Remove or reword the roadmap item only if the feature is fully implemented.
- Document any deliberate limitations of the first version.

### Acceptance criteria

- README examples match implemented APIs.
- The roadmap wording is accurate.
- The docs do not overclaim based on existing bounded-concurrency operators alone.

### Files likely involved

- `README.md`
- `docs/IDEA.md`

## Suggested Agent Handout Batches

### Batch A: decision-critical

- Task 1

### Batch B: core implementation

- Task 2
- Task 3

### Batch C: tests and docs

- Task 4
- Task 5

## Final Checklist

- every task has a clear owner-sized scope
- every task has acceptance criteria
- decision-gate tasks are clearly marked
- likely files are listed to reduce agent search time
- execution order reflects real dependencies
