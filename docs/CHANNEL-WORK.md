# Work Log

## Channel Integration Status

Completed:

- Phase 1:
  - channel ingress and egress interop
- Phase 2:
  - explicit channel execution boundaries
  - `ChannelBackpressureMode`
  - `PipeThroughChannel(...)`
  - `RunOnChannel(...)`
  - bounded `ToChannel(capacity, mode, ...)`
  - `MergeChannels(...)`
- Phase 3:
  - `TeeToChannel(...)`
  - channel-backed `Buffer(count, capacity, mode)`
  - channel-backed `Window(count, capacity, mode)`

## Sequencing Constraints

- Phase 4 should not re-open phase-2/3 API shape unless a concrete semantic gap is found.
- The next decision gate is whether phase 4 is primarily:
  - execution-graph / boundary observability
  - structured supervision over channel-backed work
  - or an explicitly split 4A / 4B roadmap
- `IStream<T>` remains the primary programming model. Channels should stay at explicit boundary control points rather than leaking into general composition APIs.

## Follow-up Docs

- Actionable backlog for the remaining channel work lives in:
  - `docs/CHANNEL-TASKS.md`
