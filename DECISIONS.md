# durably decisions

Append-only governance notes for choices that affect the public contract.

## 2026-07-24 — Execution policy edits preserve durable history

The workflow body hash is a compatibility fast path, not durable identity. When a resumable run has the same workflow family and input/key but a different body hash, durably performs an effect-free structural replay over recorded history. Matching paths, operation kinds, and labels keep the run; the new body hash is pinned before failed or unfinished work continues.

Concurrency, retry/backoff/jitter, future-attempt timeout and keyed-concurrency settings, loop limits, wait timeouts, and future step implementations are operational changes. They apply at the next unfinished boundary without invalidating completed results.

Recorded topology remains protected. Inserted, removed, reordered, or renamed recorded operations; schemas that reject stored output; changed wait identity; and changed compensation for a completed effect produce `StaleRunError`. `restart` and explicit `adopt` remain the recovery choices when automatic compatibility cannot prove the edit.

This decision replaces the earlier assumption that every `fn.toString()` change must become stale. Source equality was stricter than the durable invariant and caused operational tuning to discard useful history.

## 2026-07-24 — Advisories signpost cliffs, not style

Each initial code names a concrete cliff and an implemented next move:

| Code | Cliff | Implemented next move | Documentation route |
| --- | --- | --- | --- |
| `LOOP_SUGGESTED` | Linear replay cost from loop-shaped positional steps | `ctx.loop` snapshots | `https://www.npmjs.com/package/@nikhilverma/durably#docs-loop` |
| `FANOUT_CEILING` | Child-run fan-out approaching storage limits | Batch-provider recipe or `SqliteStorage` | `https://www.npmjs.com/package/@nikhilverma/durably#docs-fanout` |
| `RETRY_STORM` | Repeated same-class failures amplify provider pressure | Named `resource` circuit breaker | `https://www.npmjs.com/package/@nikhilverma/durably#docs-resources` |
| `STASH_SUGGESTED` | A long interrupted step restarts from zero | `stash` / `stashed` recovery | `https://www.npmjs.com/package/@nikhilverma/durably#docs-stash` |
| `BUDGET_NEAR` | Intended spend approaches its configured cap | `budget.remaining` or an explicit higher budget | `https://www.npmjs.com/package/@nikhilverma/durably#docs-budgets` |
| `WAITFOR_NO_TIMEOUT` | A parked run may wait forever | `waitFor(..., { timeoutMs })` | `https://www.npmjs.com/package/@nikhilverma/durably#docs-wait-for` |
| `SNAPSHOT_HEAVY` | State size times snapshot frequency creates excessive I/O | Compact state or raise `snapshotEvery` | `https://www.npmjs.com/package/@nikhilverma/durably#snapshot-size` |
| `SLOW_STEP_NO_TIMEOUT` | A slow attempt can hang indefinitely | Step `timeoutMs` with `AbortSignal` handling | `https://www.npmjs.com/package/@nikhilverma/durably#docs-timeouts` |

The active set is capped at ten, warning severity is reserved for stated quantitative ceilings, and stderr emits at most once per code and run.

## 2026-07-24 — `run(): T` is inviolable

`run()` returns the workflow value directly, including primitives. It never wraps `T` in metadata. Advisories use stderr-once, `onAdvisory`, persisted state/`inspect()`, and thrown `DurablyError` metadata (`runId`, active `advisories`, `hint`, `docs`). `testEngine.run()` already returns a structured result and includes `advisories`.

An `onStart({ runId })` callback remains deferred until a concrete consumer requires early run-id delivery.

## 2026-07-24 — `durably` is the canonical identity

The product, public default-engine namespace, error base class, and persistence directory use `durably`, `DurablyError`, and `.durably/`. The npm package is `@nikhilverma/durably`: the unscoped `durably` name was already owned by another publisher when M8 reached its availability gate. The checkout directory is not part of the public contract.

Until a dedicated documentation domain is deployed, every error and advisory links to an explicit anchor in the public npm README. This keeps the self-correction channel resolvable at release time; a future domain migration must update the route registry, README examples, and this table together.

## 2026-07-24 — M0 preserves inference with currying and completion tokens

TypeScript does not support partial type-argument inference: in `workflow<Input, Output>(fn)`, supplying `Input` forces `Output` to be supplied or defaulted rather than inferred from `fn`. The explicit-input form is therefore `workflow<Input>()(fn)`. `workflow(fn)` still infers both sides when callback parameters carry the input type, and `workflow({ input: schema }, fn)` remains direct.

Likewise, `complete(value): never` erases `value` from the callback's return type, making end-to-end result inference impossible for a workflow that exits through `complete`. The typed contract is `return ctx.complete(value)`, where `complete` produces a branded completion token that `workflow` unwraps to the public result type. The M1 runtime may implement the immediate exit with an internal throw, but the public type remains the branded token so the result is machine-checkable.

## Internal contract manifests

Hints and documentation routes are governed from these implementation-owned paths:

- `src/advisories/registry.ts`
- `src/errors/registry.ts`
- `src/docs/routes.ts`
- `src/public-api-manifest.ts`

Tests compare the actual registries to the route and public-API manifests; duplicating hint strings only in tests is not acceptable.
