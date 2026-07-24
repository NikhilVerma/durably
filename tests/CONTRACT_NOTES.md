# Test contract notes

## `run()` and advisory propagation

The direct `run()` return remains the workflow's inferred `T`, including primitive results. Advisory metadata therefore uses side channels that do not wrap or weaken `T`: stderr once per code, `opts.onAdvisory(advisory)`, persisted `state.json`/`inspect()`, and thrown `DurablyError` instances carrying `runId` plus the run's active `advisories[]`. `testEngine.run()` has a structured result and exposes `advisories[]` directly.

`onStart`/early run-id exposure is intentionally still open and is not assumed by the suite.

## Remaining implementation-test seams

- A brand-new `run()` that parks in `waitFor` cannot be signalled by its caller until the run id is discoverable through stderr or `list()`. The suite does not invent `onStart`; implicit waiting-run resume should be finalized with that governance decision.
- Retry-storm tests require eventual detection and verify reset/scoping through the internal accumulator, but do not pin an undocumented numeric threshold.
- File chunk and compaction thresholds are deliberately internal. Deterministic append/rename fault injection and forced compaction need an implementation-owned test seam; public constructor options are not invented here.
- SQLite execution remains a Node >=22.5 subpath concern. Static export isolation is always tested; runtime execution is covered by the adapter suite when the environment supports `node:sqlite`.
