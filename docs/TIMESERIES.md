# Time Series

## Purpose

This document is the canonical carry-forward source for Streamix time-series and event-time windowing work.

It replaces the planning and review context that had been retained in `docs/TIMESERIES-REVIEW.md`.

Use this file for:

- the current shipped time-series contract
- what is already implemented and considered complete for the current release
- what future enhancements remain explicitly deferred
- execution-ready tasks that can be handed to coding agents for later phases

## Current Status

Time-series windowing is implemented and considered release-ready for the current product contract.

The review confirms the following are already landed:

- `WindowByTime` handles boundary conditions correctly
- window management is efficient and uses bounded memory
- sliding window cleanup removes expired windows correctly
- backpressure behavior flows through `ChannelExecution.WriteAsync`
- cancellation propagates correctly through the window hierarchy
- tests pass, including edge cases and slow-consumer stress scenarios
- the public API surface is intentionally small and clean
- the README already exposes event-time windowing as part of the product story

## What Still Needs Doing

There is no obvious unfinished v1 time-series implementation called out by the review.

The remaining work is future enhancement planning in four areas:

1. Watermarks and late-event handling
   Future work for out-of-order event processing once the repo is ready to define lateness semantics explicitly.

2. Session windows
   Dynamic-gap windowing remains a candidate extension beyond current tumbling and sliding behavior.

3. Time-based joins
   Joining streams by time proximity remains a separate design/problem space and should not be treated as a small extension of existing windows.

4. Additional time-based operators
   Operators such as `BufferByTime`, `Sample`, and related time-oriented utilities remain roadmap candidates.

## Shipped Semantic Contract

- Event-time windowing with tumbling and sliding windows is part of the current Streamix feature set.
- Existing window behavior is expected to preserve correctness at window boundaries.
- Window maintenance should remain memory-bounded as streams advance.
- Backpressure and cancellation semantics must continue to compose with the rest of the stream pipeline.
- The current time-series surface area is intentionally minimal; future additions should preserve that discipline.

## Deferred Decisions That Must Be Preserved

- Watermark support and late-event policy are not part of the current contract.
- Out-of-order event handling should not be added without an explicit semantic model for lateness, cutoff, and downstream visibility.
- Session windows are deferred and should be treated as a distinct API/behavior design area.
- Time-based joins are deferred and should not be implied by current `WindowByTime` support.
- Additional time-based operators should be added deliberately rather than as a grab-bag of convenience methods.

## Non-Goals And Boundaries

- Do not overgrow the time-series API surface without clear semantics and strong use cases.
- Do not add out-of-order processing behavior implicitly to existing operators.
- Do not weaken current backpressure, cancellation, or bounded-memory characteristics while adding new time-based features.
- Do not treat time-based joins as a minor helper around existing windows if they require distinct semantics.

## Release Planning Guidance

- Preserve the current minimal API surface unless a new operator has clearly defined semantics and meaningful user value.
- Treat late-event handling as a decision-first feature, not an implementation-first feature.
- Keep stress, slow-consumer, and cancellation behavior central to any new time-series work.
- Document boundary semantics precisely whenever a new windowing or time-based operator is introduced.

## Suggested Execution Order

1. ✅ Task 1: define the semantic model for watermarks and late events
2. ✅ Task 2: implement watermark-aware or late-event behavior only after the semantic contract is settled
3. ✅ Task 3: design and implement session windows if real scenarios justify them
4. Task 4: evaluate time-based joins as a separate feature area
5. ✅ Task 5: prioritize additional time-based operators based on concrete usage needs
6. ✅ Task 6: implement BufferByTime
7. ✅ Task 7: implement Sample

## Coordination Notes

- Task 1 is the main decision gate because watermark and lateness semantics influence how future event-time features should behave.
- Task 2 must not begin until Task 1 settles the behavioral contract.
- Task 3 can proceed independently of Task 2 if session windows do not depend on watermark semantics in the chosen design.
- Task 4 should be treated as its own feature slice because it may affect API shape, performance expectations, and test strategy differently from windowing.
- Task 5 can run in parallel with later planning work once the team decides which operator is worth adding next.
- Shared files likely to create merge conflicts are `README.md`, `ARCHITECTURE.md`, and time-series-related test files.

## ✅ Task 1: Define Watermark And Late-Event Semantics

### Priority

High

### Goal

Define a clear event-time contract for out-of-order data before any watermark-aware implementation work begins.

### Why this exists

The review identifies watermarks and late-event handling as the most meaningful next time-series enhancement, but that work is risky unless the repo first settles what “late” means and how those events should be surfaced or dropped.

### Decision required

Decide the product semantics for:

- how watermarks are represented
- when an event is considered late
- whether late events are dropped, routed separately, or admitted within an allowed lateness policy
- how downstream operators observe window completion and late data

### Scope

- define the conceptual model for watermark progression
- define late-event behavior and cutoff semantics
- determine whether the initial design should support allowed lateness or keep a narrower v1-style contract
- document how the semantics relate to existing `WindowByTime` behavior

### Constraints

- do not retrofit implicit out-of-order behavior into existing operators without explicit contract wording
- keep the design understandable for .NET users and aligned with the current stream-first model

### Suggested implementation path

- start with a design note in `docs/TIMESERIES.md` or `ARCHITECTURE.md`
- compare one narrow, shippable semantic model against broader alternatives
- prefer a smaller contract over a flexible but ambiguous one

### Acceptance criteria

- one explicit watermark/late-event semantic model is documented
- the design states what happens to late events and when windows are considered complete
- the design is narrow enough to implement and test without guesswork

### 🎯 Task 1 decision

Task 1 is now settled with a narrow bounded-out-of-orderness model for future watermark-aware event-time operators.

The intent is to make Task 2 implementation work mechanical rather than interpretive.

### Canonical semantic model

- The existing `WindowByTime` contract remains unchanged and remains suitable for already ordered event-time input.
- Watermark-aware behavior must be introduced as an explicit new operator or explicit opt-in mode. It must not silently change the behavior of the current `WindowByTime`.
- A watermark is an event-time cutoff, not wall-clock time.
- The watermark is monotonic and never moves backwards.
- For the initial design, the watermark is derived from observed events rather than injected as a separate control stream.
- The derived watermark is:
  `watermark = maxObservedEventTimestamp - outOfOrderness`
- `outOfOrderness` is a non-negative duration that defines how much reordering the operator tolerates before an event is considered late.
- If `outOfOrderness = TimeSpan.Zero`, the operator expects strictly non-decreasing event timestamps.
- Watermark progression happens only when a new event is observed or when upstream completes. The initial design does not include idle-source heuristics or processing-time timers.

### Event classification

- An event is on time if `event.Timestamp > currentWatermark` at the moment the operator receives it.
- An event is late if `event.Timestamp <= currentWatermark` at the moment the operator receives it.
- Equality is intentionally late:
  if the watermark has reached `2025-01-01T00:05:00Z`, an event with timestamp `2025-01-01T00:05:00Z` is late.
- Late-ness is determined on arrival. Once an event has been admitted to a window, later watermark advancement does not retroactively invalidate it.

### Window membership and completion

- Window membership remains based on event time and existing boundary rules.
- Tumbling and sliding windows continue to use half-open intervals:
  `[windowStart, windowEnd)`
- An admitted event may belong only to windows whose event-time interval contains its timestamp.
- A window becomes complete when `watermark >= windowEnd`.
- Once complete, a window is final:
  it emits no more items, is not reopened, and is not revised.
- On upstream completion, all remaining open windows are completed immediately, equivalent to advancing the watermark to positive infinity for finalization purposes.

### Late-event policy for the initial contract

- The initial watermark-aware contract does not support allowed lateness.
- The initial watermark-aware contract does not support retractions, corrections, or re-opening closed windows.
- A late event is dropped from the main windowed output.
- A late event must not be inserted into any already completed window.
- A late event must not create a new window by itself.
- The initial contract does not require a side output for late events.
- If the library later adds explicit late-event routing, that must be a separate additive API and must not change the drop behavior of the initial contract unless the caller opts in explicitly.

### Relationship to current implementation

- The current `WindowByTime` implementation closes windows based on observed event timestamps and source completion, not on a formal watermark.
- The current implementation assumes the input stream is already ordered enough for its boundary logic to be meaningful.
- Task 2 must preserve the current implementation for callers who do not opt into watermark-aware behavior.
- Watermark-aware behavior is therefore an additive event-time mode, not a reinterpretation of existing tests.

### Required downstream visibility rules

- Downstream consumers observe each window exactly once.
- Downstream consumers observe only final window contents.
- Downstream consumers do not observe partial completion signals followed by later corrections.
- Downstream consumers do not observe dropped late events in the main output.
- If diagnostics are added later, they are informational and separate from the main data path.

### Explicit non-goals for the first implementation

- no configurable allowed-lateness period after watermark completion
- no separate watermark records in the public data stream
- no processing-time fallback for idle partitions or idle sources
- no retraction/update model for already emitted windows
- no automatic side channel for late events
- no implicit behavior change to `MapWithTimestamp` or `WindowByTime`

### Mechanical rules for Task 2

Coding agents implementing Task 2 should be able to apply the following rules directly:

1. Track `maxObservedEventTimestamp`.
2. Derive `currentWatermark = maxObservedEventTimestamp - outOfOrderness`.
3. Before admitting an event, classify it as late when `event.Timestamp <= currentWatermark`.
4. Drop late events from the main output.
5. Admit on-time events to all matching windows under the existing half-open interval rules.
6. After watermark advancement, complete every open window where `windowEnd <= currentWatermark`.
7. Never reopen or mutate a completed window.
8. When upstream completes, complete every remaining open window.

### Test matrix implied by this contract

- in-order input with `outOfOrderness = 0`
- bounded out-of-order input that still arrives before the watermark cutoff
- event exactly at the watermark boundary and therefore late
- event earlier than the watermark and therefore dropped
- windows that remain open until watermark advancement
- final upstream completion that flushes remaining windows
- cancellation and backpressure behavior unchanged relative to the existing operator model

### Files likely involved

- `docs/TIMESERIES.md`
- `ARCHITECTURE.md`
- `README.md`

## ✅ Task 2: Implement Watermark-Aware Event-Time Behavior

### Priority

High

### Goal

Add the agreed watermark/late-event behavior without regressing current time-series guarantees.

### Why this exists

Once the semantic model is defined, the runtime and tests need to prove that out-of-order handling works while preserving backpressure, cancellation, and bounded-memory expectations.

### Scope

- implement the chosen watermark/late-event behavior
- add tests for in-order and out-of-order event sequences
- add tests for late-event cutoff behavior and cancellation
- verify that slow-consumer and backpressure characteristics remain sound

### Constraints

- no implementation before Task 1 settles the contract
- do not weaken current boundary correctness or cleanup behavior
- do not ship without strong behavioral tests

### Suggested implementation path

- extend the existing time-series/windowing implementation incrementally
- add focused tests for watermark progression, late events, and window completion timing
- keep the initial feature narrow if the full space is too broad

### Acceptance criteria

- watermark-aware behavior matches the documented contract
- tests cover in-order, out-of-order, and late-event scenarios
- cancellation, cleanup, and backpressure behavior remain correct

### Files likely involved

- `src/Streamix`
- `src/Streamix.Tests`
- `ARCHITECTURE.md`
- `README.md`

## ✅ Task 3: Design And Implement Session Windows

### Priority

Medium

### Goal

Introduce session windows only if the repo can define clear dynamic-gap semantics and prove them with tests.

### Why this exists

Session windows are a natural next step for time-series work, but they differ enough from fixed windows that they need their own semantics, API shape, and test strategy.

### 🎯 Task 3 decision

Task 3 is complete. Session windows are implemented with support for both in-order and watermark-aware merging semantics.

### Canonical session semantics

- A session is defined by a maximum gap of inactivity between events.
- A session's range is `[minTimestamp, maxTimestamp]`.
- An event with `timestamp` belongs to a session if it falls within the session's influence range: `[minTimestamp - gap, maxTimestamp + gap]`.

### Ordered mode (outOfOrderness is null)

- Optimized for mostly in-order data.
- Emits session streams immediately upon the first event of a session.
- A session is extended as long as incoming events arrive within `gap` of the current session's boundaries.
- If an event arrives outside the `gap`, the current session is completed and a new one starts.
- This mode allows for low-latency processing of sessions as they happen.

### Watermark-aware mode (outOfOrderness is provided)

- Supports late and out-of-order data with session merging.
- Emits only final, completed session contents to satisfy the "observe only final window contents" contract from Task 1.
- A session is considered finalized and emitted when `watermark >= session.maxTimestamp + gap`.
- **Merging**: If a new event bridges the gap between two active sessions, or extends an existing one, the affected sessions and the new event are merged into a single session.
- **Lateness**: Events arriving after the watermark (`event.timestamp <= watermark`) are dropped.
- **Ordering**: Sessions are emitted in chronological order based on their start times. Items within a session are sorted by timestamp before emission.

### Implementation and API

- Operator: `WindowBySession(gap, capacity, mode, outOfOrderness)`
- Respects standard backpressure (`capacity`, `mode`) and `CancellationToken`.
- On upstream completion, all remaining active sessions are flushed immediately.

### Acceptance criteria

- session-window behavior is documented clearly
- the implementation matches the documented merge and close semantics
- tests cover boundary cases and operational behavior

### Files likely involved

- `src/Streamix`
- `src/Streamix.Tests`
- `ARCHITECTURE.md`
- `docs/TIMESERIES.md`

## Task 4: Evaluate Time-Based Joins As A Separate Feature Slice

### Priority

Medium

### Goal

Decide whether time-based joins belong in the near roadmap and, if so, what the smallest coherent contract would be.

### Why this exists

Time-based joins are powerful but substantially broader than windowing alone, so they need to be handled as a separate feature area rather than folded casually into existing time-series APIs.

### Decision required

Determine whether the immediate need is:

- a design memo only
- a narrow initial join operator
- or explicit deferral

### Scope

- identify the most relevant time-based join scenarios
- define the minimal useful semantics if the feature is worth pursuing
- decide whether this should become its own dedicated task breakdown doc later

### Constraints

- avoid broad join abstractions without a concrete first use case
- do not couple this work too tightly to existing window APIs unless the semantics genuinely align

### Suggested implementation path

- begin with a planning/design investigation
- split implementation into a separate workstream if it proves worthwhile

### Acceptance criteria

- the repo has a clear decision on whether time-based joins are in or out for the next phase
- if in scope, the initial semantic contract is documented narrowly enough for implementation planning

### Files likely involved

- `docs/TIMESERIES.md`
- `ARCHITECTURE.md`
- `WORK.md`

## ✅ Task 5: Prioritize Additional Time-Based Operators

### Priority

Low

### Goal

Turn the remaining time-operator wishlist into a prioritized, evidence-based backlog.

### Why this exists

The review mentions operators like `BufferByTime` and `Sample`, but they should be chosen based on clear user value and semantic fit, not added as generic completeness work.

### Scope

- identify the most useful next operators
- document their intended semantics and priority
- decide whether any are simple enough to group or whether each deserves separate design work

### Constraints

- no operator sprawl
- no additions without clear semantics for timing, cancellation, and backpressure

### Suggested implementation path

- start with a short prioritization memo or backlog note
- create separate implementation tasks only for the top one or two operators

### Acceptance criteria

- the repo has a prioritized list of candidate time-based operators
- each shortlisted operator has a short semantic description
- low-value or ambiguous operators are explicitly deferred

### 🎯 Task 5 decision

Task 5 is complete. Additional time-based operators have been prioritized based on common reactive stream patterns and user value.

### Prioritized operators

- **BufferByTime** (High): Groups items into `IList<T>` based on fixed time intervals. Essential for batching items over time for efficient processing or sink writes.
- **Sample** (High): Emits the latest item from each time interval. Critical for rate-limiting and observing the latest state in a high-frequency stream.
- **Debounce** (Medium): Emits an item only after a specified period of inactivity. Important for UI interactions and event suppression; deferred to a later planning cycle.

### Files likely involved

- `docs/TIMESERIES.md`
- `WORK.md`
- `README.md`

## ✅ Task 6: Implement BufferByTime

### Priority

High

### Goal

Implement the `BufferByTime` operator to group items into lists based on a time interval.

### Scope

- Implement `BufferByTime(TimeSpan interval, int? maxCount = null)` in `src/Streamix/Extensions/StreamExtensions.cs`.
- Use `IClock` for testable timing.
- Ensure proper cancellation and backpressure support.
- Add unit tests covering various interval and item arrival scenarios.

### Acceptance criteria

- `BufferByTime` correctly batches items into lists.
- A buffer is emitted when the time interval elapses or when `maxCount` is reached (if provided).
- The operator handles source completion by flushing the final partial buffer.
- Cancellation stops the timer and enumeration immediately.

### Files likely involved

- `src/Streamix/Extensions/StreamExtensions.cs`
- `src/Streamix.Tests/Extensions/TimeBasedOperatorTests.cs`

## ✅ Task 7: Implement Sample

### Priority

High

### Goal

Implement the `Sample` operator to emit the latest item within a periodic time interval.

### Scope

- Implement `Sample(TimeSpan interval)` in `src/Streamix/Extensions/StreamExtensions.cs`.
- Use `IClock` for testable timing.
- If no items arrive during an interval, nothing is emitted for that interval.
- Ensure proper cancellation and backpressure support.
- Add unit tests covering regular and sparse item arrival.

### Acceptance criteria

- `Sample` emits the most recent item at each interval boundary.
- If the source is faster than the sampling interval, only the latest item is kept.
- If the source is slower, some intervals may emit nothing.
- The operator handles source completion and cancellation correctly.

### Files likely involved

- `src/Streamix/Extensions/StreamExtensions.cs`
- `src/Streamix.Tests/Extensions/TimeBasedOperatorTests.cs`

## Additional Tasks

Recommended pattern for future time-series follow-up:

- separate semantic-contract decisions from implementation
- keep new time-based features narrow and strongly tested
- treat out-of-order handling and joins as substantial contract work, not minor follow-ons

## Suggested Agent Handout Batches

### Batch A: decision-critical

- Task 1
- Task 4

### Batch B: implementation

- Task 2
- Task 3
- Task 6
- Task 7

### Batch C: backlog planning

- Task 5

## Final Checklist

- the current time-series contract is preserved in one durable file
- remaining work is framed as future enhancement planning rather than missing release work
- decision-gate tasks are clearly separated from implementation tasks
- tasks are small enough to hand to coding agents with acceptance criteria
