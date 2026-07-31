# Changelog

All notable changes to `@nikhilverma/durably` are recorded here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.0] - 2026-07-31

Field feedback from a 449-batch, 448k-image harvest built on 0.3.0. Every change
here removes something that cost that build time.

### Added

- `onStep` on `RunOptions` and `TestRunOptions`: a progress channel symmetric
  with `onAdvisory`, delivering `{ runId, path, label, status, attempt,
  executions, ms?, error? }` where `status` is `running | replayed | ok |
  retrying | failed`. `console.log` inside a workflow body re-prints on every
  replay and cannot tell a replayed step from an executing one; a retrying step
  was previously indistinguishable from a slow one. Observation never fails a
  run: a listener that throws is swallowed. `testEngine().resume(runId, {
  onStep })` takes the same callback and detaches it when that resume returns.
- `list({ key })` and `key` on `RunState`, so the name an operator chose leads
  back to a run id without globbing every `state.json`.

### Changed

- `undefined` now follows `JSON.stringify` at the durable boundary: an undefined
  property is dropped from its object and an undefined array element becomes
  `null`. `{ maxImages: undefined }` and `{}` are therefore the same input and
  hash identically. The previous behaviour rejected the ordinary `field?: T`
  idiom with a `SerializationError` before the run started, which is the one
  place the serializer diverged from the JS mental model it otherwise matches.
  Values JSON genuinely cannot express still throw with their path.
- `KeyConflictError` names what conflicts: the message reports the bound run id
  and both inputs, and the error carries `key`, `runId`, `boundInput`, and
  `input`. Deciding whether the change was meaningful no longer requires
  locating and reading `state.json`.

### Documentation

- The default concurrencies are stated where they bite: `ctx.parallel` runs
  every thunk at once unless capped, while an engine runs 4 *runs* at a time —
  the knob that actually throttles `spawnAll` fan-out.
- The batching pattern for work above the fan-out ceiling is written down:
  coarse durable steps plus your own per-item ledger inside each step.

## [0.3.0] - 2026-07-25

### Fixed

- Hydration no longer flattens `Date` values and recorded `Error`s to `{}` when
  another process resumes a run. Decoding is now idempotent, and a recorded
  error keeps its class and reported name.
- Signals are routed by the durable path of the parked `waitFor` instead of by
  signal name, so a workflow may await the same signal name more than once.
- A delivered signal payload stays recorded, so replaying a satisfied `waitFor`
  returns its value instead of parking the run again.
- Signal payloads are validated where the wait's schema exists, inside the
  workflow. A payload sent by a process that never ran the body no longer fails
  the run permanently: it is discarded, the run stays parked for a corrected
  signal, and the refusal is visible on the recorded wait.
- `cancel` leaves an already-terminal run and its children untouched instead of
  rewriting a completed outcome.
- `retry` re-executes steps whose compensations already ran, so a retried saga
  can no longer complete on state that was rolled back.
- A step attempt that outlives its lease is discarded instead of being written
  over the state of the engine that reclaimed the run; that engine adopts the
  stored outcome and `LeaseLostError` is now raised by the runtime.
- The default heartbeat interval fits inside short leases instead of being
  pinned at 100ms.

### Changed

- `ctx.charge` rejects negative amounts so recorded spend stays monotonic.
- Array holes are normalized to `null` at the durable boundary so a live value
  and the value a resumed process reads back have the same shape.

## [0.2.0] - 2026-07-24

### Added

- Effect-free structural compatibility replay for changed workflow bodies.
- Durable compatibility checks for recorded paths, operation kinds, labels,
  schemas, wait identities, loop history, and completed-step compensation.
- Coverage for policy edits across parallel concurrency, retries, backoff,
  jitter, timeouts, keyed concurrency, loop limits, and wait timeouts.
- Coverage for compatible edits across process restarts, anonymous workflows,
  and multiple resumable runs with the same input or key.

### Changed

- Execution-policy changes now resume the existing run and preserve completed
  steps instead of creating a new run or making the run stale.
- Failed and unfinished steps use the latest callback and execution policy when
  a changed workflow is structurally compatible with its recorded history.
- Anonymous workflows with the same input are matched against all resumable
  histories so a compatible run wins over simple recency.

## [0.1.1] - 2026-07-24

### Fixed

- Shared one default runtime across the core, test, and SQLite package
  entrypoints so mixed imports operate on the same runs.
- Hardened the dependency-audit release gate.

## [0.1.0] - 2026-07-24

### Added

- Initial public release of local-first durable workflow execution for
  TypeScript.
- File and SQLite storage, resumable steps, parallel execution, retries,
  timeouts, compensation, waits, loops, child runs, budgets, resources,
  advisories, and the shadow-replay test engine.
