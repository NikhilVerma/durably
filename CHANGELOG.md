# Changelog

All notable changes to `@nikhilverma/durably` are recorded here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
