# durably decisions

Append-only governance notes for choices that affect the public contract.

## 2026-07-24 — Advisories signpost cliffs, not style

Each initial code names a concrete cliff and an implemented next move:

| Code | Cliff | Implemented next move | Documentation route |
| --- | --- | --- | --- |
| `LOOP_SUGGESTED` | Linear replay cost from loop-shaped positional steps | `ctx.loop` snapshots | `durably.dev/loop` |
| `FANOUT_CEILING` | Child-run fan-out approaching storage limits | Batch-provider recipe or `SqliteStorage` | `durably.dev/fanout` |
| `RETRY_STORM` | Repeated same-class failures amplify provider pressure | Named `resource` circuit breaker | `durably.dev/resources` |
| `STASH_SUGGESTED` | A long interrupted step restarts from zero | `stash` / `stashed` recovery | `durably.dev/stash` |
| `BUDGET_NEAR` | Intended spend approaches its configured cap | `budget.remaining` or an explicit higher budget | `durably.dev/budgets` |
| `WAITFOR_NO_TIMEOUT` | A parked run may wait forever | `waitFor(..., { timeoutMs })` | `durably.dev/wait-for` |
| `SNAPSHOT_HEAVY` | State size times snapshot frequency creates excessive I/O | Compact state or raise `snapshotEvery` | `durably.dev/loop#snapshot-size` |
| `SLOW_STEP_NO_TIMEOUT` | A slow attempt can hang indefinitely | Step `timeoutMs` with `AbortSignal` handling | `durably.dev/timeouts` |

The active set is capped at ten, warning severity is reserved for stated quantitative ceilings, and stderr emits at most once per code and run.

## 2026-07-24 — `run(): T` is inviolable

`run()` returns the workflow value directly, including primitives. It never wraps `T` in metadata. Advisories use stderr-once, `onAdvisory`, persisted state/`inspect()`, and thrown `DurablyError` metadata (`runId`, active `advisories`, `hint`, `docs`). `testEngine.run()` already returns a structured result and includes `advisories`.

An `onStart({ runId })` callback remains deferred until a concrete consumer requires early run-id delivery.

## 2026-07-24 — `durably` is the canonical identity

The package, public default-engine namespace, error base class, persistence directory, subpaths, documentation routes, and examples use one name consistently: `durably`, `DurablyError`, `.durably/`, and `durably/*`. The checkout directory is not part of the public contract.

## Internal contract manifests

Hints and documentation routes are governed from these implementation-owned paths:

- `src/advisories/registry.ts`
- `src/errors/registry.ts`
- `src/docs/routes.ts`
- `src/public-api-manifest.ts`

Tests compare the actual registries to the route and public-API manifests; duplicating hint strings only in tests is not acceptable.
