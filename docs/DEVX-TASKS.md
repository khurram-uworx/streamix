# Developer Experience (DEVX) Task Breakdown

## Purpose

This document breaks the "Developer Experience (DEVX)" roadmap slice (GitHub Issue #47) into concrete, assignable tasks.

Current state:
- Streamix has basic side-effect operators: `DoOnNext`, `DoOnError`, `DoOnComplete`, `DoOnTerminate`.
- There is no built-in way to easily log or trace items without manual `DoOnNext(Console.WriteLine)` calls.
- There is no concept of a "Named" pipeline for observability or metrics.

## Suggested Execution Order

1. ✅ Task 1: Implement `Named` metadata tracking
2. ✅ Task 2: Implement `Log` and `Debug` operators
3. ✅ Task 3: Implement `Checkpoint` operator
4. ✅ Task 4: Implement `Trace` operator
5. Task 5: Add behavioral tests for DEVX operators
6. Task 6: Update documentation and examples

## Coordination Notes

- Task 1 is a prerequisite for more advanced observability in later tasks.
- Task 2 and Task 3 are independent but both build on Task 1's naming if available.
- Task 4 is the most complex tracing task and should likely come after basic logging is settled.
- Shared files with merge-conflict risk:
  - `src/Streamix/IStream.cs`
  - `src/Streamix/ISingle.cs`
  - `src/Streamix/Implementations/Stream.cs`
  - `src/Streamix/Implementations/Single.cs`
  - `src/Streamix.Tests/DiagnosticOperatorTests.cs`

## Task 1: Implement `Named` Metadata Tracking ✅

### Priority

High

### Goal

Allow streams and singles to be tagged with a name that can be used by other diagnostic operators and future metrics/tracing integrations.

### Why this exists

Issue #47 identifies `Named("...")` as a key for metrics, tracing, and logging. Without a name, logs are anonymous and hard to correlate in complex pipelines.

### Decision required

- Should `Name` be part of the `IStream<T>` / `ISingle<T>` interface or an internal property?
- Should `Named` create a new stream wrapper that carries the name?

### Scope

- Add `Name` property (optional) to `IStream<T>` and `ISingle<T>`.
- Implement `Named(string name)` operator on both types.
- Ensure the name is propagated or preserved where appropriate (decide on propagation rules).

### Acceptance criteria

- `stream.Named("MyStream").Name` returns "MyStream".
- The `Named` operator returns a stream of the same type.
- Documentation reflects how naming works.

### Files likely involved

- `src/Streamix/IStream.cs`
- `src/Streamix/ISingle.cs`
- `src/Streamix/Implementations/Stream.cs`
- `src/Streamix/Implementations/Single.cs`

## ✅ Task 2: Implement `Log` and `Debug` Operators

### Priority

High

### Goal

Provide zero-boilerplate operators for logging items, errors, and completion.

### Why this exists

Users shouldn't have to write `DoOnNext(x => Console.WriteLine(x))` every time they want to see what's happening.

### Scope

- Implement `Log()` operator: logs items, errors, and completion to standard output (or a configurable logger).
- Implement `Debug()` operator: similar to `Log()` but potentially more verbose or specifically targeted at debugger output.
- Support optional `prefix` or use the stream `Name` if available.

### Suggested implementation path

- Build on `DoOnNext`, `DoOnError`, and `DoOnComplete`.
- Allow passing an `Action<string>` or `ILogger` for the actual output.

### Acceptance criteria

- `stream.Log()` prints items to the console by default.
- `stream.Named("Orders").Log()` uses the name in the output.
- `stream.Log("Prefix")` uses the prefix.

### Files likely involved

- `src/Streamix/IStream.cs`
- `src/Streamix/ISingle.cs`
- `src/Streamix/Implementations/Stream.cs`
- `src/Streamix/Implementations/Single.cs`

## Task 3: Implement `Checkpoint` Operator

### Priority

Medium

### Goal

Track progress through a specific stage of a pipeline.

### Why this exists

`Checkpoint("stage-name")` helps identify where a pipeline is hanging or failing in a multi-stage async flow.

### Scope

- Implement `Checkpoint(string name)` operator.
- Logs when the checkpoint is reached, and potentially how long it took since the last checkpoint or stream start.

### Acceptance criteria

- `stream.Checkpoint("Stage 1")` logs an event when items pass through.
- Performance impact is minimal when not in use.

### Files likely involved

- `src/Streamix/IStream.cs`
- `src/Streamix/Implementations/Stream.cs`

## Task 4: Implement `Trace` Operator

### Priority

Medium

### Goal

Provide a full "trace" of every signal (Next, Error, Complete, Cancel, Subscribe, Request).

### Why this exists

Full lifecycle tracing is essential for debugging complex backpressure and cancellation issues.

### Scope

- Implement `Trace()` operator.
- Should capture subscription start, every item emitted, every error, and completion/cancellation.

### Acceptance criteria

- `stream.Trace()` provides a comprehensive log of the stream lifecycle.
- Output is clear and helps diagnose timing/ordering issues.

### Files likely involved

- `src/Streamix/IStream.cs`
- `src/Streamix/Implementations/Stream.cs`

## Task 5: Behavioral Test Matrix

### Priority

High

### Goal

Verify all DEVX operators behave correctly and don't interfere with stream logic.

### Scope

- Tests for `Log`, `Debug`, `Checkpoint`, `Trace`, and `Named`.
- Verify that these operators do not change the items or the timing significantly (except for intended logging overhead).
- Verify that they handle errors and cancellation correctly.

### Acceptance criteria

- A new test file or expanded `DiagnosticOperatorTests.cs` covers all new operators.
- Tests pass consistently.

### Files likely involved

- `src/Streamix.Tests/DiagnosticOperatorTests.cs`

## Task 6: Update Documentation and Examples

### Priority

Medium

### Goal

Make sure the new DEVX tools are discoverable.

### Scope

- Update `README.md` and `GETTING-STARTED.md`.
- Add examples showing how to use `Log()`, `Named()`, etc.

### Acceptance criteria

- Documentation is accurate and reflects the new API.

### Files likely involved

- `README.md`
- `GETTING-STARTED.md`

## Suggested Agent Handout Batches

### Batch A: core metadata and logging

- Task 1
- Task 2

### Batch B: advanced diagnostics

- Task 3
- Task 4

### Batch C: tests and docs

- Task 5
- Task 6
