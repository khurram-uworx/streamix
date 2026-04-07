# Task Breakdown: Backpressure Strategies for Streamix

## Purpose

Implement explicit backpressure strategies (`OnBackpressureBuffer`, `OnBackpressureDrop`, `OnBackpressureLatest`, `OnBackpressureError`) to give developers clear control over stream overflow behavior when producers outpace consumers.

Reference: `docs/BACKPRESSURE.md`

## Suggested Execution Order

1. **✅ Task 1**: Define `BackpressureException` and backpressure strategy enum
2. **✅ Task 2**: Add backpressure operator methods to `IStream<T>` interface
3. **✅ Task 3**: Implement `OnBackpressureBuffer` operator
4. **Task 4**: Implement `OnBackpressureDrop` operator
5. **✅ Task 5**: Implement `OnBackpressureLatest` operator
6. **Task 6**: Implement `OnBackpressureError` operator
7. **Task 7**: Add comprehensive tests for all strategies
8. **Task 8**: Add examples to README and docs

## Coordination Notes

- **Task 1 is a decision gate**: Once `BackpressureException` and strategy enum are defined, Tasks 3–6 can run in parallel
- **Task 2 is a decision gate**: API shape must be finalized before implementation tasks
- **Task 7 should not begin** until Tasks 3–6 are complete
- **Shared file risk**: `IStream.cs` (Task 2) will cause merge conflicts if multiple tasks edit it; execute sequentially
- **Implementation file risk**: Each operator has its own implementation, but all extend `Stream.cs`; coordinate if patterns diverge

---

## ✅ Task 1: Define BackpressureException and Strategy Types

### Priority

**High**

### Goal

Create the exception type and enum that all backpressure operators will use for consistent error handling and strategy identification.

### Why this exists

Foundation task; without this, all implementation tasks are blocked. Ensures consistent exception semantics across all strategies.

### Decision required

- Should `BackpressureException` extend `InvalidOperationException` or `Exception`?
- Should there be a strategy enum, or just method names? (Suggested: enum for internal use, methods for public API)

### Scope

- Define `BackpressureException` class in a new file `src/Streamix/BackpressureException.cs`
- Define internal `BackpressureStrategy` enum (optional, for tagging or configuration)
- Add XML documentation explaining when each exception type is thrown

### Constraints

- Must be compatible with `.NET 10`
- Exception must be serializable (if applicable to your error handling story)

### Suggested implementation path

1. Create `BackpressureException.cs` in `src/Streamix/`
2. Define `BackpressureException : InvalidOperationException` with a single constructor accepting a message
3. Optionally create `BackpressureStrategy` enum with values: `Buffer`, `Drop`, `Latest`, `Error`
4. Add `[Serializable]` if needed; document typical messages for each scenario

### Acceptance criteria

- `BackpressureException` can be instantiated with a descriptive message
- Exception is catchable in consumer code
- Exception clearly indicates a backpressure overflow event
- Type is accessible from `Streamix` namespace (no nested/internal barriers)

### Files likely involved

- `src/Streamix/BackpressureException.cs` (new)

---

## ✅ Task 2: Add Backpressure Operator Methods to IStream<T>

### Priority

**High** (decision gate for implementation tasks)

### Goal

Define the public API surface for all four backpressure strategies in the `IStream<T>` interface.

### Why this exists

Establishes the contract that all implementations must follow. Developers must see these methods available on their streams. API shape is critical before coding begins.

### Decision required

- Confirm method signatures match the proposed API from `docs/BACKPRESSURE.md`
- Decide if strategies should be mutually exclusive (suggested: yes, last one wins) or composable
- Confirm buffer capacity constraints (e.g., capacity > 0)

### Scope

- Add four new method declarations to `IStream<T>`:
  - `IStream<T> OnBackpressureBuffer(int capacity)`
  - `IStream<T> OnBackpressureDrop()`
  - `IStream<T> OnBackpressureLatest()`
  - `IStream<T> OnBackpressureError()`
- Add XML documentation for each method (copy from `docs/BACKPRESSURE.md`)

### Constraints

- No implementation logic in the interface; only signatures and docs
- Parameter validation (e.g., `capacity > 0`) happens in implementations, not here
- Methods must be chainable (return `IStream<T>`)

### Suggested implementation path

1. Open `src/Streamix/IStream.cs`
2. Find a logical insertion point (near other operator methods, e.g., after `Throttle` or before error-handling operators)
3. Add four method declarations with full XML `<summary>`, `<param>`, `<returns>`, and `<exception>` tags
4. Reference the strategy behavior doc link in the remarks

### Acceptance criteria

- All four methods appear in `IStream<T>` with correct signatures
- Each method has complete XML documentation
- IntelliSense shows the documentation when developers invoke these methods
- No compilation errors after edits

### Files likely involved

- `src/Streamix/IStream.cs`

---

## Task 3: Implement OnBackpressureBuffer Operator

### Priority

**High**

### Goal

Implement the `OnBackpressureBuffer(int capacity)` operator so streams can buffer items up to a fixed capacity and throw on overflow.

### Why this exists

Core backpressure strategy for scenarios where temporary queuing is acceptable but overflow must fail fast. This is the most "traditional" strategy.

### Decision required

- None (API shape decided in Task 2)

### Scope

- Implement `OnBackpressureBuffer` in `Stream.cs` (public method)
- Create internal helper method to manage the bounded channel with buffer semantics
- Validate `capacity > 0` and throw `ArgumentOutOfRangeException` if not
- Throw `BackpressureException` if buffer overflows
- Ensure operator chains correctly with other operators

### Constraints

- Must use `Channel<T>` internally (consistent with Streamix architecture)
- Buffer full behavior: throw immediately, don't wait
- Operator must be composable (if a later backpressure operator is chained, it overrides this one)

### Suggested implementation path

1. Open `src/Streamix/Implementations/Stream.cs`
2. Add public method `IStream<T> OnBackpressureBuffer(int capacity)` that validates capacity and delegates to an internal helper
3. Create private async method (following Streamix patterns) to manage the buffering channel:
   - Create `Channel<T>` with bounded capacity
   - Subscribe to source stream and write items to the channel
   - Catch channel full scenario and throw `BackpressureException`
   - Return wrapped stream
4. Add unit tests in `BackpressureTests.cs` (Task 7)

### Acceptance criteria

- Stream can be created with `stream.OnBackpressureBuffer(100)`
- When buffer reaches capacity, next item causes `BackpressureException`
- Items up to capacity are correctly emitted downstream
- Negative or zero capacity throws `ArgumentOutOfRangeException`
- Operator chains: `stream.OnBackpressureBuffer(100).Map(...).ForEachAsync(...)` works
- No items are lost while buffering

### Files likely involved

- `src/Streamix/Implementations/Stream.cs`
- `src/Streamix.Tests/BackpressureTests.cs` (created in Task 7, referenced here)

---

## Task 4: Implement OnBackpressureDrop Operator

### Priority

**High**

### Goal

Implement the `OnBackpressureDrop()` operator so streams drop items when downstream cannot keep pace, always emitting the most recent item.

### Why this exists

Essential for real-time data streams (metrics, events) where recent values are more important than historical ones. Missing old events is acceptable.

### Decision required

- None (API shape decided in Task 2)

### Scope

- Implement `OnBackpressureDrop` in `Stream.cs` (public method)
- Create internal helper to manage drop semantics
- Use `Channel<T>` with `ChannelFullMode.DropWrite` or custom logic
- Ensure that when buffer is full, the new item is kept and the oldest is dropped
- Do not throw exceptions for normal operation (only for configuration errors)

### Constraints

- Must preserve FIFO semantics for items that fit in the buffer
- When dropping, discard the oldest, keep the newest
- Operator must be composable (if a later backpressure operator is chained, it overrides this one)

### Suggested implementation path

1. Open `src/Streamix/Implementations/Stream.cs`
2. Add public method `IStream<T> OnBackpressureDrop()`
3. Create private async method to manage the drop semantics:
   - Create a bounded `Channel<T>` (suggest capacity 1 or configurable small value)
   - Use `TryWrite` with `ChannelFullMode.DropWrite` or implement custom logic to drop oldest
   - Ensure no exceptions during normal operation
4. Add unit tests in `BackpressureTests.cs` (Task 7)

### Acceptance criteria

- Stream can be created with `stream.OnBackpressureDrop()`
- When buffer is full, the oldest item is dropped, newest is added
- No exception is thrown during normal operation
- Items are correctly emitted downstream without gaps (except intentional drops)
- Operator chains correctly with other operators
- Test verifies that only the most recent item is emitted when producer is much faster than consumer

### Files likely involved

- `src/Streamix/Implementations/Stream.cs`
- `src/Streamix.Tests/BackpressureTests.cs` (created in Task 7, referenced here)

---

## ✅ Task 5: Implement OnBackpressureLatest Operator

### Priority

**High**

### Goal

Implement the `OnBackpressureLatest()` operator so only the latest item is kept when downstream is slow; intermediate items are discarded.

### Why this exists

Essential for state-like streams (UI updates, configuration changes) where only the newest value matters. Replaces old values as they arrive.

### Decision required

- None (API shape decided in Task 2)

### Scope

- Implement `OnBackpressureLatest` in `Stream.cs` (public method)
- Create internal helper to manage "latest only" semantics
- Use `Channel<T>` with custom logic or bounded behavior
- Always emit the most recent item; discard anything before it if buffer is full
- Do not throw exceptions for normal operation

### Constraints

- Semantics: If 5 items arrive while consumer is blocked, only the 5th is emitted when consumer unblocks
- Operator must be composable (if a later backpressure operator is chained, it overrides this one)

### Suggested implementation path

1. Open `src/Streamix/Implementations/Stream.cs`
2. Add public method `IStream<T> OnBackpressureLatest()`
3. Create private async method to manage "latest only" semantics:
   - Use a single-slot bounded `Channel<T>` or custom volatile storage
   - On each new item, if buffer is full, replace the buffered item with the new one
   - When consumer catches up, emit the latest item
4. Add unit tests in `BackpressureTests.cs` (Task 7)

### Acceptance criteria

- Stream can be created with `stream.OnBackpressureLatest()`
- When multiple items arrive while consumer is busy, only the latest is emitted
- No exception is thrown during normal operation
- Operator chains correctly with other operators
- Test verifies that 5 rapid items (while consumer blocked) result in only 1 emission once consumer unblocks

### Files likely involved

- `src/Streamix/Implementations/Stream.cs`
- `src/Streamix.Tests/BackpressureTests.cs` (created in Task 7, referenced here)

---

## Task 6: Implement OnBackpressureError Operator

### Priority

**High**

### Goal

Implement the `OnBackpressureError()` operator so backpressure conditions immediately throw a `BackpressureException` instead of buffering or dropping.

### Why this exists

Essential for strict scenarios where overflow indicates a design problem that must be surfaced immediately. Fail fast rather than hide the issue.

### Decision required

- None (API shape decided in Task 2)

### Scope

- Implement `OnBackpressureError` in `Stream.cs` (public method)
- Create internal helper to detect and throw on backpressure
- Throw `BackpressureException` as soon as the internal channel cannot accept an item
- Do not silently drop or buffer

### Constraints

- Use a bounded `Channel<T>` (small capacity, e.g., 1)
- On write failure, immediately throw `BackpressureException` with a descriptive message
- Operator must be composable (if a later backpressure operator is chained, it overrides this one)

### Suggested implementation path

1. Open `src/Streamix/Implementations/Stream.cs`
2. Add public method `IStream<T> OnBackpressureError()`
3. Create private async method to manage error semantics:
   - Create a small bounded `Channel<T>` (capacity 1)
   - Use `TryWrite` and check for failure
   - If channel is full, throw `BackpressureException` immediately
4. Add unit tests in `BackpressureTests.cs` (Task 7)

### Acceptance criteria

- Stream can be created with `stream.OnBackpressureError()`
- When downstream cannot keep pace, `BackpressureException` is thrown
- Exception contains a descriptive message indicating the overflow
- Exception propagates to the consumer's error handling (e.g., `.ContinueWith` or try/catch)
- Operator chains correctly with other operators

### Files likely involved

- `src/Streamix/Implementations/Stream.cs`
- `src/Streamix.Tests/BackpressureTests.cs` (created in Task 7, referenced here)

---

## Task 7: Add Comprehensive Tests for Backpressure Strategies

### Priority

**High**

### Goal

Create a new test file `BackpressureTests.cs` with unit and integration tests for all four backpressure operators, verifying behavior under load and with other operators.

### Why this exists

Ensures each strategy behaves as documented. Critical for catching edge cases (e.g., empty streams, single item, rapid succession, chaining with operators).

### Scope

- Create `src/Streamix.Tests/BackpressureTests.cs`
- Test each of the four operators in isolation:
  - `OnBackpressureBuffer`: Happy path, overflow exception, negative capacity validation
  - `OnBackpressureDrop`: Happy path, drop semantics, no exceptions during normal operation
  - `OnBackpressureLatest`: Happy path, latest-only semantics, rapid succession
  - `OnBackpressureError`: Happy path, exception on backpressure
- Test operator composition and chaining:
  - `OnBackpressureBuffer(...).Map(...).ForEachAsync(...)`
  - Override semantics (last operator wins)
- Test with real producer/consumer mismatch scenarios (fast produce, slow consume)
- Test exception messages are descriptive

### Constraints

- Follow existing test patterns in `Streamix.Tests` (use xUnit, async patterns, etc.)
- Tests should be fast (no multi-second delays unless testing time-based behavior)
- Use `TaskScheduler` or similar to control timing where needed

### Suggested implementation path

1. Create `src/Streamix.Tests/BackpressureTests.cs`
2. Add test class `BackpressureTests` inheriting from `IDisposable` (if needed for setup/teardown)
3. For each operator, create test methods:
   - `[Fact] public async Task OnBackpressureBuffer_HappyPath_BuffersItems()`
   - `[Fact] public async Task OnBackpressureBuffer_OverflowThrows_BackpressureException()`
   - etc.
4. Add integration tests:
   - `[Fact] public async Task MultipleBackpressureOperators_LastOneWins()`
   - `[Fact] public async Task FastProducerSlowConsumer_DemoBackpressureBehavior()`
5. Run `dotnet test` to verify all pass

### Acceptance criteria

- All four operators have at least 3 test cases each (happy path, error/edge case, integration)
- Tests verify correct item count, exception types, and semantics
- All tests pass (`dotnet test` succeeds)
- Coverage includes chaining and operator precedence
- Test names are descriptive and follow Streamix conventions

### Files likely involved

- `src/Streamix.Tests/BackpressureTests.cs` (new)

---

## Task 8: Update Documentation and Examples

### Priority

**Medium**

### Goal

Add backpressure strategy examples to README.md and create a guide for developers on when and how to use each strategy.

### Why this exists

Developers must understand the backpressure API and know which strategy to choose for their use case. Examples in the README make it discoverable.

### Scope

- Add section to `README.md` introducing backpressure strategies
- Provide one example for each of the four strategies showing typical use
- Link to `docs/BACKPRESSURE.md` for detailed semantics
- Consider adding a "Common Patterns" guide in `docs/BACKPRESSURE.md` (examples for metrics, state, strict validation)

### Constraints

- Keep README examples brief; detailed docs live in `docs/BACKPRESSURE.md`
- Examples must be runnable (or at least realistic enough to inspire confidence)

### Suggested implementation path

1. Open `README.md`
2. Find the operators section or add a new "Backpressure" section
3. Add a brief intro paragraph
4. Add four small code examples (one per strategy)
5. Link to `docs/BACKPRESSURE.md`
6. Optionally, add common patterns to `docs/BACKPRESSURE.md` (metrics, UI, validation)

### Acceptance criteria

- README contains at least one example for each backpressure strategy
- Examples show practical use cases (metrics, state, validation, errors)
- Link to detailed docs is present
- Formatting matches existing README style

### Files likely involved

- `README.md`
- `docs/BACKPRESSURE.md` (may add "Common Patterns" section)
