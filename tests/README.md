# durably test suite

This is the executable contract built before the library. No test is skipped or marked TODO. The M0 declarations now satisfy the type contract; runtime files still intentionally fail during import until their implementation milestones land.

## Layout

| Area | Contract exercised |
| --- | --- |
| `types/public-api.contract.ts` | overload inference, schema inference, `run(): T`, loop `done`, heterogeneous children, `Result` narrowing, selectors, engines/storage/resources, advisory and step callbacks |
| `unit/m1-kernel.test.ts` | positional steps, replay memoization, purity, serialization, replay-safe values/logging |
| `unit/m2-test-engine.test.ts` | failure injection, selectors, shadow replay, fake time, signal/wait |
| `unit/m3-policies.test.ts` | retry/backoff, timeout/abort, schema validation, saga rollback, keyed concurrency, budgets |
| `unit/m3-backoff-policy.test.ts` | deterministic jitter bounds and max-delay capping |
| `unit/m4-storage.test.ts` | seven-method adapter, state materialization, framed records, atomic state, torn-tail recovery, hydration fidelity for dates and errors, third-party adapter conformance |
| `unit/m5-engine.test.ts` | five-row identity table, keys, lifecycle operations, priority, leases, stale/retry/restart/adopt, engine concurrency cap, terminal-state protection for cancel and retry |
| `unit/m5-policy-compatibility.test.ts` | policy-only edits, structural compatibility replay, file restart, public `run()`, and incompatible topology/schema/compensation changes |
| `unit/m5-advisories.test.ts` | all initial codes, thresholds, dedupe, severity, cap, silence, callbacks, stderr, error metadata, attempt timing |
| `unit/m5-advisory-accumulator.test.ts` | non-vacuous active cap, severity crossings, retry-streak reset and scoping |
| `unit/m6-topology-agent.test.ts` | hierarchical parallelism, children, cancellation, loop snapshots and limits, signal routing, waits, timers, stash, result helpers |
| `unit/m7-resources-sqlite.test.ts` | cross-run resource policies, global budget, SQLite persistence |
| `unit/m8-ship-gates.test.ts` | CORE/EXTENDED tokenizer budgets and agent-facing registry |
| `unit/m8-node-portability.test.ts` | package exports, ESM/CJS Node smoke, SQLite isolation, no Bun production imports |
| `unit/m8-registry-contract.test.ts` | actual advisory/error registries, docs routes, implemented API-reference manifest |
| `unit/m9-operator-ergonomics.test.ts` | JSON `undefined` semantics at the durable boundary, self-describing key conflicts, key lookup, `onStep` progress |
| `integration/hero-workflow.test.ts` | README fan-out/fan-in story, flaky children, crash/resume, saga recovery |
| `integration/readme-hero-process.test.ts` | literal file-backed hero command killed and rerun across real processes |
| `integration/file-engine-cooperation.test.ts` | state/advisory parity, two-engine subscribe, two-process kill/reclaim, lease heartbeat and stale-owner protection, key conflicts |

## Commands

```bash
bun run test:types
bun run test:unit
bun run test:integration
bun run test:all
```

Tests use `bun:test` and Bun scripts, while workflows and storage/process harnesses use standard JavaScript and `node:*` APIs available on modern Node runtimes. The SQLite adapter is deliberately imported from the separate `sqlite.ts`/`@nikhilverma/durably/sqlite` entrypoint so the core import remains usable before Node 22.5.
