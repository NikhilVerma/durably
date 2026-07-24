# HANDOVER.md — durably implementation

**To:** the coding agent taking over this project
**From:** the design session (README-driven development, complete)
**Date:** 2026-07-24

---

## 1. Mission and current state

durably is a durable workflow library for TypeScript: single-process, local-first, zero-dependency, no server, no compiler magic, built for agent scripts and CLI pipelines. Think "Temporal's core idea with everything distributed, magical, or versioned deliberately removed."

**Nothing is implemented. Zero lines of code exist.** What exists is `README.md` — a fully worked spec produced outside-in (README-driven development), which has survived adversarial reviews. It is the contract you are implementing.

## 2. How to treat the spec

- `README.md` is **authoritative** for API shape, semantics, defaults, and guarantees. Implement what it says.
- If implementation reveals the spec is wrong, ambiguous, or type-system-impossible: **do not silently deviate.** Change the code AND the README in the same commit, with a note in `DECISIONS.md` explaining why. The README must never lie about the code.
- The "Design notes: sharp edges" section of the README is part of the contract — those limitations are *promised*, not apologized for. Don't "fix" them into complexity (e.g., don't add versioning/patching to solve staleness).
- The Anti-goals section is binding. If you find yourself building a scheduler daemon, an event bus, an Agent class, a dashboard, or distributed claiming — stop, you've left the map.

## 3. Decision log (the *why* behind the spec — rationale not fully in the README)

| Decision | Rationale (condensed) |
| --- | --- |
| Explicit-checkpoint replay (`ctx.step(fn)`), not static chain, not compiler transform | Only model where dynamic flow is a plain `if`; DBOS-style, no magic. Static chains (Reflow) can't express the feature set; `"use workflow"` transforms violate the no-magic principle. |
| Positional step identity, names optional | Enabled by refusing versioning: same-code resume (source hash) + purity ⇒ deterministic sequence ⇒ names unnecessary for correctness. Labels are selectors only — a label collision may select the wrong step in a test (loud), never match the wrong recorded result (silent). |
| **Hierarchical position paths** `[k, i, j]`, branch-local counters | A flat counter breaks under `parallel`: interleaving depends on real completion timing ⇒ replay order ≠ original ⇒ spurious PurityError blaming the user for a scheduler artifact. Found by adversarial review; treat as the #1 correctness landmine. |
| Body-only source hash (`fn.toString()`), not transitive imports | Recorded steps never re-execute ⇒ step-callback internals are free to change (completed steps use recorded results; future/retrying steps run new code — desired). Hash protects only what replay depends on: the control-flow skeleton. The known hole (impure between-step helpers) is documented and shadow-replay-mitigated. |
| `run()` join-or-start-or-resume (5-row table in README) | Zero-config hero story ("crash → rerun same command → finishes") requires implicit matching; the table makes it precise. completed→fresh (never memoize), failed→resume-from-failure (rerun *is* the retry), live-lease→subscribe (two terminals cooperate). |
| Policies as plain data, not combinators | Agent context economy + JSON-serializable. Effect ideas borrowed, Effect dependency declined (API surface = the enemy of the 2k budget). |
| Cross-run limits (breaker/rate-limit/shared concurrency) on named `resource()`, not steps | They're shared state across runs by definition; per-step breakers are semantically wrong. |
| `ctx.loop` durable reducer with state snapshots | Step-replay is O(N) at resume for N-turn agent loops; snapshots make resume O(1) and let old step records compact away. This is `continueAsNew` as an internal mechanism, not a user-facing workaround. |
| `stash`/`stashed` in step context | Intra-step recovery hook (interrupted streams, partial agent turns), inspired by Cloudflare Think's recovery data. durably owns mechanics; consumer owns the resume contract. |
| FileStorage default (hardened), SqliteStorage (`node:sqlite`) in box | Greppability is a feature (filesystem-is-the-UI principle). Hardening: length-prefixed CRC records + torn-tail truncation + per-run lockfile. sqlite covers Windows/NFS/transactional without breaking zero-dep (built into Node ≥22.5). |
| Shadow replay default in testEngine | Strongest available answer to "the purity rule is hidden": every existing test auto-verifies replay determinism. |
| No cron, no `ctx.restart`, no CLI in core | Cron ⇒ daemon ⇒ server territory (OS's job; durably makes invocations idempotent). Log growth is internal (chunking + compaction snapshots), never user-facing. Library-first; `durably-cli` may exist later as separate sugar. |
| `retry(runId, {fromStep?})` vs `restart(runId)` vs `adopt(runId)` | Three verbs, three meanings: failed-run reprocessing / stale-run fresh start / stale-run dangerous resume (user asserts compat; assertion recorded in log). |
| Progressive disclosure w/ signposted cliffs | Temporal's sin isn't depth, it's invisible cliffs (`continueAsNew` discovered from wreckage). durably: delete the cliff where possible; otherwise warn in-band before the fall. |
| Advisories as first-class channel | Warnings ride surfaces the reader already consumes (`state.json`/`inspect`/`testEngine` results, `run()` callback/stderr, error hints). Dedupe-by-code + severity-crossing emission + cap 10 = anti-spam contract. |
| NO eslint plugin (reversal of earlier decision) | Lint-required-for-correct-use = API design failure (React hooks precedent). Shadow replay is the in-band check; residual silent-re-execution risk stays documented, not tool-patched. |
| `run(): T` is inviolable; advisories via stderr/callback/error, never a wrapper | Hero ergonomics outrank metadata plumbing. Checker finding 2026-07-24; wrapper rejected. `testEngine` result keeps `advisories[]` (already a rich object). |

## 4. Build order (milestones — each independently verifiable)

**M0 — Types-only skeleton (`d.ts` / signatures, no implementation).**
Purpose: falsify the API before building it. The three known type-system risk spots:

  1. `ctx.loop`'s reducer return: `state | done(v)` union — `done` should return a branded never-ish type so `return done(v)` narrows correctly and `V` infers from the `done` argument.
  2. `spawnAll([[wfA, inputA], [wfB, inputB]])` heterogeneous tuples — needs `const` type params / mapped tuple inference so each child's output type survives into `joinAll`'s `Result<...>[]`. If full heterogeneous inference gets ugly, propose: homogeneous `spawnAll` + variadic overloads, and update the README.
  3. `Result<T, E>` narrowing through `.filter(r => r.ok)` — provide `isOk`/`partition` helpers because bare `.filter` doesn't narrow.
  Also verify: `workflow<Input>()` (type param) and `workflow({ input: schema })` (Standard Schema inference) coexist via overloads; `run(wf, input)` infers input AND result from `wf` by value.
  Gate: `tsc --strict --noEmit` on an examples file exercising every README code block.

**M1 — Kernel (~the 500-line heart).**
In-memory first: run lifecycle state machine (statuses per README), event log per run, position-path assignment (context stack: entering `parallel` pushes `[k, i]` frames; each frame owns a counter), step memoization, replay, `PurityError` on path-sequence divergence, `complete()`, `now()`/`random()` recording, `ctx.log`.
Gate: replay determinism property tests — run any workflow, kill at every possible step boundary, resume, assert identical final result + `executions === 1` everywhere. Include the adversarial case: `parallel` branches with different step counts and randomized completion timing.

**M2 — testEngine + shadow replay.**
Sync execution, fake clock (`advance` drives retries/sleeps/timeouts), `crashAfter`/`crashInStep` (selector: path | label | `{label, nth}`), shadow replay on by default. This lands *before* real storage so every later milestone is testable.
Gate: the three test examples in the README's Testing section pass verbatim.

**M3 — Policies on steps.**
retry/backoff/jitter, per-attempt `timeoutMs` via AbortSignal, `schema` validation (Standard Schema v1 interface only — no schema lib imported), `compensate` with durable LIFO unwind (crash mid-rollback resumes rollback), keyed concurrency.
Gate: saga crash-mid-compensation test; timeout aborts the signal; retry timing verified via fake clock.

**M4 — FileStorage.**
Record format: `[u32 length][u32 crc32][payload NDJSON]`. Read path truncates at first invalid record (torn tail self-heal). Per-run lockfile (`flock` where available; `O_EXCL` lockfile + pid + staleness fallback elsewhere). `state.json` materialized on every event via atomic rename (`tmp` + `rename`). Internal chunking (`events.log`, `events.1.log`, …) + compaction snapshots (memo-table snapshot event; resume = latest snapshot + tail). Directory sharding by run-id prefix.
Gate: crash-injection tests (kill -9 during append; partial-write simulation by truncating at every byte offset of the last record); resume correctness after each.

**M5 — Engine + `run()`.**
Default engine (lazy singleton, `./.durably`), the 5-row `run()` matching table exactly, canonical input hash (sorted-key stable stringify; document float caveat), `key` override + `KeyConflictError`, lease/heartbeat/reclaim, subscribe-on-live-lease, priority claim ordering, `enqueue/signal/cancel/pause/resume/inspect/list/retry/restart/adopt/drain/stop`, engine-wide budget pool. Advisory subsystem: code registry, dedupe/severity-crossing/cap logic, propagation into `state.json` + `inspect()` + `testEngine` result + `run()`'s `onAdvisory` callback + error hints/active advisories + stderr-once.
Gate: two-process integration test on one storage dir — cooperative subscribe, and reclaim-after-kill.

**M6 — Topology + agent layer.**
`parallel` (hierarchical paths under concurrency), `spawn/spawnAll/joinAll` (child ids derived `parentRunId:path:i` for idempotent spawning; parent-cancel cascades unless `detached`), `waitFor`, `sleep/sleepUntil` (persisted `wake_at`), `ctx.loop` (snapshot events, `snapshotEvery`, `maxIterations`, resume-from-snapshot with zero prior-iteration replay, pause lands at iteration boundary), `stash`/`stashed`.
Gate: the README hero demo, run literally, with a real `^C` mid-flight; loop test crashing at turn ~50 and asserting O(1) resume (no step re-replay below snapshot).

**M7 — SqliteStorage + resources.**
`node:sqlite` adapter behind a subpath export (`durably/sqlite`) so Node <22.5 users aren't broken at import time. `resource()` with token-bucket rate limit, breaker state machine, shared concurrency; breaker/limit state persisted so restarts don't reset an open breaker.

**M8 — Ship gates.**

- AGENTS CORE block ≤ 2,000 tokens, **measured by tokenizer in CI** — this gate is the product's identity; enforce it mechanically. AGENTS EXTENDED has a soft ≤ 1,500-token budget.
- Every README code block extracted and compiled (and where feasible, executed) in CI. The README must never lie.
- Every hint (errors + advisories) references only implemented APIs, and every docs anchor resolves — CI-checked. A hint pointing at vaporware is a bug.
- Every new advisory code names the specific cliff it signposts in `DECISIONS.md`; no drive-by nagging or the channel trains readers to ignore it.
- Before publishing, verify npm availability for `durably`; if unavailable, record a scoped fallback in `DECISIONS.md` and update the docs.

## 5. Cross-cutting implementation notes

- **Runtime/tooling:** TypeScript strict, ESM-first with CJS via bundler, Bun as the primary package manager and `bun:test` runner, Node ≥ 20 compatibility for core (≥ 22.5 only for the sqlite subpath). Zero runtime dependencies is a hard rule for core — Standard Schema is an *interface* you vendor, not a package you import at runtime.
- **Serialization boundary:** inputs, step outputs, loop state, stash, annotations = plain JSON data + `Date` (encode as tagged string). Reject non-serializable at write time with `SerializationError{path}` — fail at the step that produced it, not at resume.
- **`fn.toString()` hashing:** normalize whitespace before hashing (minifier/formatter churn shouldn't fork identity). Note in DECISIONS.md that bundlers/transpilers can still rewrite bodies — acceptable for the target use (scripts run from source via tsx/bun); revisit if it bites.
- **AbortSignal discipline:** one signal per step attempt, aborted on timeout, cancel, and lease loss. `parallel` concurrency limiter must not start thunks after cancellation.
- **Attempt timing:** step-attempt events record `startedAt` and `ms`. `STASH_SUGGESTED` depends on knowing that a step re-ran from zero after a crash and its first attempt exceeded 30 seconds; preserve this data through compaction.
- **`ctx.log`:** an event type; on first execution also mirrors to stdout; silent on replay. Trivial, but it's the top-of-funnel teaching feature — make the replay-silence exact.
- **Budget accounting** must be crash-consistent: `charge` is an event; engine-wide pool is the sum over live runs' events, cached in the index.

## 6. Open questions — intentionally unresolved, decide when you hit them (record in DECISIONS.md)

1. `.durably/` garbage collection: auto-prune completed runs after N days vs explicit `durably.gc()` API only. (Lean: API only for v0; no background behavior.)
2. `parallel` thunk signature: thunks call `ctx.step` inside (current spec) — confirm ergonomics survive implementation, or switch to `ctx.parallel(items, (item, ctx) => ...)` if path-scoping is cleaner that way. README updates if changed.
3. `adopt` semantics detail: does adopt re-hash and pin to the *new* body (so a second edit re-stales)? (Lean: yes.)
4. Windows lockfile strategy specifics.
5. Whether `waitFor` timeout produces a failed run or a typed timeout result. (Lean: throws `StepTimeoutError`-family ⇒ failed run; catchable in-workflow via try around `waitFor`.)
6. Advisory propagation into *streaming* contexts (hooks/`onStepComplete`) — needed or is `state.json` enough? (Lean: `state.json` is enough for v0.)
7. Does `run()` need `onStart({ runId })`? `runId` currently reachable via stderr line + `DurablyError.runId` + `list()`. (Lean: defer until a consumer needs it.)

## 7. What NOT to do (repeated because it's the identity)

No distributed claiming. No cron/daemon. No event triggers. No Agent class or model bindings in core. No dashboard. No versioning/patching (staleness is terminal by design). No dependency — not even a schema lib, not even left-pad. No API surface growth without paying for it out of the 2k-token AGENTS budget: if a new export doesn't fit, it goes to `durably-contrib` or doesn't exist.

## 8. Files in this handover

- `README.md` — the contract (spec, API, AGENTS block, design notes, prior art)
- `HANDOVER.md` — this file
- Suggested repo additions on day one: `DECISIONS.md` (append-only; seed it from §3), `examples/` (one file per README code block, CI-compiled)
