# durably — complete API reference

Load this when the task needs something beyond `step` / `parallel` / `retry` / `testEngine`.
Package: `@nikhilverma/durably`. Entry points: `.`, `./test`, `./sqlite`.

## Replay model

The workflow function replays from the top after a crash, a `waitFor`, or a `sleep`. Each
`ctx.step()` is matched to the run's log **by position path**, not by name. Top-level steps are
`[0], [1], [2]…`; inside `ctx.parallel`, branch *i*'s *j*-th step under the *k*-th parallel is
`[k, i, j]`, nesting recursively — so branch-local counters make identity independent of
scheduling. Completed steps return their recorded result and never re-execute.

A replay whose path sequence diverges throws `PurityError { path, expected, actual }`
immediately rather than corrupting state. A purity violation that does **not** change control
flow (a stray `fetch` between steps whose result feeds no branch) is undetectable in principle
and will silently re-execute on every replay. Shadow replay in `testEngine` is the real defense.

## Define and run

```ts
const wf = workflow<Input>()(async (ctx, input) => result)   // curried: fixes Input, infers Output
const wf = workflow(async (ctx, input: Input) => result)     // both inferred when params are typed
const wf = workflow({ input: Schema, output?, name? }, fn)   // Standard Schema form

await run(wf, input, options?) -> Output    // the workflow's value, always; never a wrapper
```

`RunOptions`: `{ key?, fresh?, budget?, dir?, checkpointEvery?, advisories?: 'silent',
onAdvisory?: (a) => void, onStep?: (e: StepEvent) => void }`. Default storage directory is
`./.durably`.

`checkpointEvery` bounds how many completed steps a crash may re-execute. A checkpoint rewrites
the whole run state, so the default scales: every step up to 100 steps, then every one percent of
the steps so far, capped at every hundredth. Pass `1` when steps cost money — that is the setting
that makes "completed work is never re-billed" literally true at any run length. Also accepted by
`enqueue` and `createEngine`.

`StepEvent` = `{ runId, path, label, status, attempt, executions, ms?, error? }`, where `status` is
`running | replayed | ok | retrying | failed`. It is the progress channel for a long run:
`console.log` inside the workflow body re-prints on replay, and `replayed` vs `running` is exactly
the distinction that re-printing loses. Observers cannot fail a run — a listener that throws is
swallowed. `testEngine().run(wf, input, { onStep })` and `te.resume(runId, { onStep })` take the
same callback; a resume observer is detached when that resume returns.

Candidate identity is `(workflow family, canonicalInputHash)`, or `(workflow family, key)` when
a key is given. Input canonicalization is well defined because inputs are plain JSON. The body
hash is the compatibility fast path; a changed body attaches to history only after structural
replay succeeds.

| Existing run for this identity | `run()` does |
| --- | --- |
| unfinished (pending / waiting / sleeping / lease expired) | resume it |
| lease held by another live process | subscribe — wait for its result |
| latest completed | start fresh — re-running a success means "do it again" |
| latest failed | resume from the failed step; `{ fresh: true }` overrides |
| none | start |

Give workflows a stable `name` when several unrelated scripts share one storage directory. A
`key` replaces input identity *within* a family; it does not name the family.

## `ctx`

```ts
ctx.runId: string           // current durable run id
ctx.attempt: number         // workflow attempt number
ctx.budget: { remaining(kind), spent(kind) }   // kind: 'usd' | 'tokens' | 'units'

ctx.step(fn, options?) -> Promise<T>
  fn receives { signal: AbortSignal, stash: (v) => Promise<void>, stashed: unknown }
  options: { name?, retry?, timeoutMs?, schema?, compensate?, concurrencyKey?,
             concurrencyLimit? = 1, uses? }

ctx.parallel(thunks, { concurrency? }?) -> Promise<Result<T>[]>   // default: every thunk at once
  // child runs are capped by the ENGINE's concurrency (default 4), not by this
ctx.spawn(wf, input, { detached? }?) -> Promise<RunHandle<T>>
ctx.spawnAll([[wf, input], …], { detached? }?) -> Promise<RunHandle[]>
ctx.joinAll(handles) -> Promise<Result<T>[]>

ctx.loop(state0, async (state, { step, done }) => state | done(v),
         { snapshotEvery? = 1, maxIterations? }) -> Promise<V>

ctx.waitFor(name, schema?, { timeoutMs? }?) -> Promise<T>
ctx.sleep(ms); ctx.sleepUntil(date)

ctx.now(): number           // replay-safe clock
ctx.random(): number        // replay-safe randomness
ctx.log(...args)            // records once, silent on replay
ctx.annotate(obj)           // shallow-merged into state.json
ctx.charge({ usd?, tokens?, units? })   // negative amounts are rejected
return ctx.complete(v)      // exit the workflow early, inferring v as the result
```

`RetryPolicy`: `{ attempts, backoff: 'exponential' | 'linear' | 'none', baseMs?, maxMs?, jitter? }`.
Retries are off by default. `timeoutMs` is per attempt.

`Result<T>` = `{ ok: true, value: T } | { ok: false, error: unknown }`. Helpers `isOk` and
`partition` are exported from the package root.

Cancelling a parent cancels its children unless they were spawned `{ detached: true }`.

## Serialization boundary

Workflow inputs, step outputs, loop state, stash values, annotations, and the workflow result
are deep-cloned by a strict serializer. `SerializationError` carries the offending `path`.

Allowed: `null`, strings, booleans, finite numbers, valid `Date`, plain arrays, plain objects whose
prototype is `Object.prototype`.

Normalized exactly as `JSON.stringify` would: an `undefined` property is dropped from its object,
and an array hole or `undefined` element becomes `null`. So `{ maxImages: undefined }` and `{}` are
the same input, hash identically, and join the same run — `field?: T` is safe to pass straight to
`run()`. A top-level `undefined` step result stays `undefined`.

Rejected: `Infinity` / `NaN`, `bigint`, `symbol`, functions, any class instance (`Map`, `Set`,
`URL`, `Error`, your own classes), and cyclic references.

Thrown errors are a separate path: a recorded error keeps its class and reported name across
process boundaries. Returning an `Error` as a *value* is a class instance and is rejected.

## Stash — intra-step recovery

```ts
await ctx.step(async ({ signal, stash, stashed }) => {
  const progress = stashed as { text?: string; offset?: number } | undefined
  let acc = progress?.text ?? ''
  for await (const chunk of streamText({ prompt, resumeFrom: progress?.offset, signal })) {
    acc += chunk
    if (acc.length - (progress?.offset ?? 0) > 4096) await stash({ text: acc, offset: acc.length })
  }
  return acc
})
```

`stash(v)` persists to the run log, is scoped to this step, is handed back as `stashed` on the
next attempt, and clears on completion. What to stash and how to resume from it is your contract
with your provider; durably owns only the mechanics.

## Default engine and operations

```ts
import { durably } from '@nikhilverma/durably'      // lazy, ./.durably

durably.enqueue(wf, input, { key?, priority?, delayMs?, budget? }) -> RunHandle
durably.inspect(runId) -> RunState | null
durably.list({ status?, workflow?, key?, limit? }) -> RunState[]   // key -> runId -> run directory
durably.retry(runId, { fromStep? })       // failed run: completed steps stay done
durably.restart(runId) -> RunHandle       // stale run: fresh start under new code
durably.adopt(runId)                      // stale run: you assert replay compatibility
durably.signal(runId, name, payload)
durably.cancel(runId)                     // no-op on an already-terminal run
durably.pause(runId) / durably.resume(runId)   // lands at the next step or loop boundary
```

`RunStatus`: `pending | running | waiting | sleeping | paused | completed | failed | cancelled | stale`.

Step selectors accept a position path (canonical), a string (first label match), or
`{ label, nth }` (precise). Labels are derived from the callback source unless `name` is given.

## Long-lived engines, resources, storage

```ts
import { createEngine, FileStorage, resource } from '@nikhilverma/durably'

const openai = resource('openai', {
  rateLimit: { max: 60, perMs: 60_000 },
  breaker: { failureRate: 0.5, windowMs: 30_000, cooldownMs: 10_000 },
  concurrency: 8,
})

const engine = createEngine({
  storage: new FileStorage('~/.myagent/durably'),
  resources: [openai],
  concurrency: 8,           // concurrent RUNS, default 4 — this is what throttles child fan-out
  checkpointEvery: 1,       // bound crash loss to zero completed steps
  budget: { usd: 200 },        // fleet-wide; run budgets draw from it
  hooks: { onRunFailed: ({ runId, error }) => notify(error) },
  leaseMs, heartbeatMs,
})
await engine.start()
```

Attach a resource per step with `{ uses: 'openai' }`. An open breaker throws retryable
`CircuitOpenError`; exponential backoff rides out the cooldown.

Storage:

- **`FileStorage`** (default) — one directory per run; events are length-prefixed,
  CRC-checksummed records, so a torn tail from a crash truncates cleanly instead of corrupting
  the run. A per-run lockfile prevents interleaved writers after lease reclamation. Each state
  record embeds the whole run, so the log is compacted to its newest record past ~1 MB
  (`new FileStorage(dir, { compactAboveBytes })`); a resume reads only that record.
- **`MemoryStorage`** — tests and ephemeral work.
- **`SqliteStorage`** from `@nikhilverma/durably/sqlite` — built on `node:sqlite` (Node ≥ 22.5,
  no native addon). Use it on network filesystems, on Windows without caveats, or when you want
  transactional claims.

Custom backends implement the 7-method adapter: `init, createRun, append, read, claim,
heartbeat, list`. `append` must be atomic and ordered per run.

Two processes sharing a storage directory cooperate through leases: the owner heartbeats, and a
lost lease raises `LeaseLostError` — the late attempt is discarded rather than written over the
reclaiming engine's state. This is failover, not distribution; it is the ceiling.

## Errors

All extend `DurablyError` and carry `{ hint, docs, runId, advisories[] }` — the next move and
the run's active warnings, which makes them most diagnostic exactly when the run fails.

| Error | Raised when |
| --- | --- |
| `ValidationError` | an input, output, or signal payload fails its schema |
| `SerializationError` | a value cannot cross the durable boundary (`path` names it) |
| `StepTimeoutError` | an attempt exceeds `timeoutMs` |
| `PurityError` | the replayed step-path sequence diverges |
| `StaleRunError` | the workflow body changed incompatibly — `restart` or `adopt` |
| `BudgetExceededError` | a charge or step would cross a run or engine cap |
| `CircuitOpenError` | a resource breaker is open (retryable) |
| `KeyConflictError` | a `key` collides with an incompatible run — carries `key`, `runId`, `boundInput`, `input` |
| `RunCancelledError` | the run or its parent was cancelled |
| `LeaseLostError` | another engine reclaimed the run mid-attempt |

## Advisories

durably watches each run for approaching ceilings and missed tools, and surfaces advisories in
the responses you already read: `state.json`, `inspect()`, the `testEngine` result object, and
any thrown `DurablyError`. `run()` also emits them to stderr once, or to `onAdvisory`.

| Code | Fires when | Points at |
| --- | --- | --- |
| `LOOP_SUGGESTED` | thousands of steps in a loop-shaped pattern | `ctx.loop` |
| `FANOUT_CEILING` | child runs nearing the storage ceiling | provider Batch API recipe |
| `RETRY_STORM` | same step, same error class, N straight failures | breaker / resource |
| `STASH_SUGGESTED` | a step over 30s re-ran from zero after a crash | `stash` |
| `BUDGET_NEAR` | 80% of a cap | budget |
| `WAITFOR_NO_TIMEOUT` | a wait with no `timeoutMs` | `waitFor` timeouts |
| `SNAPSHOT_HEAVY` | loop state size × snapshot frequency | `snapshotEvery` |
| `SLOW_STEP_NO_TIMEOUT` | a slow step with no `timeoutMs` | timeouts |
| `CONCURRENCY_CAPPED` | more children spawned than the engine runs at once | `createEngine({ concurrency })` |

Codes are deduplicated per run, emitted once per severity crossing, capped at 10 active per run,
and silenced entirely with `advisories: 'silent'`.

## Testing

```ts
import { testEngine } from '@nikhilverma/durably/test'

const te = testEngine()                                   // memory storage + fake clock
te.run(wf, input, { crashAfter?, crashInStep?, budget?, advisories? })
  -> { runId, status, result, steps, advisories, error?, compensationError?, children }
te.resume(runId)
te.signal(runId, name, payload)
te.clock.advance(ms)
```

Each `steps[]` entry carries `{ path, label, status, executions, attempts, output?, error?,
stashed?, attemptHistory? }`. `executions` is the honest count of how many times the callback
actually ran — assert on it rather than on wall-clock behavior.

Shadow replay runs after every completed run and asserts an identical path sequence with zero
new executions. Crash selectors accept the same selector forms as step operations, including
`{ label: 'generateText', nth: 47 }`.

## Known limits, stated plainly

1. Compatibility replay proves recorded history, not arbitrary JavaScript. A helper changed
   outside the hashed workflow body is a hole; shadow replay is the check.
2. `run()`'s failed→resume behavior retries permanent failures — a bad input hits the same wall
   every invocation until `{ fresh: true }`.
3. `loop` state must be serializable and reasonably small. A 50 MB messages array snapshotted
   every turn is your I/O bill; compact it in your reducer or raise `snapshotEvery`.
4. In-run `parallel` is comfortable to ~10⁴ steps. Child runs: ~10³–10⁴ on `FileStorage`,
   ~10⁵ on `SqliteStorage`. Past ~10⁵ items, submit your provider's Batch API as one step and
   poll it with `sleep` inside a loop. Below that ceiling the same shape applies: batch the work
   into a few hundred durable steps and keep per-item idempotency in your own ledger inside each
   step. durably owns the coarse checkpoints, you own the fine ones.
5. A step is the unit of durability: one killed at 99% yields nothing and re-runs whole. `stash`
   is the hook for progress inside an attempt; nothing else checkpoints mid-step.
6. No distributed execution, no job queue, no persistent cron, no dashboards, no workflow
   versioning APIs. These are refusals, not roadmap gaps.
