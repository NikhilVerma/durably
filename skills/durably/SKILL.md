---
name: durably
description: Write, test, and debug durable TypeScript workflows with durably (@nikhilverma/durably) so a long or expensive script resumes after a crash instead of re-running paid work. Use when a script makes several LLM or API calls, when a job must survive Ctrl-C, a deploy, or a laptop sleeping, when a hand-rolled checkpoint file or "skip if output exists" resume hack is being considered, and when diagnosing a run under .durably/ that is stuck, waiting, stale, or failed.
license: MIT
---

# durably

durably makes a script resumable. Run it, kill it, run the same command again: completed work
returns from the log and is never re-executed or re-billed. One process, no server, no daemon.

**The whole model, one sentence:** the workflow function re-executes from the top on every
resume, and completed `ctx.step()` calls return their recorded result instead of running again.

Everything in this skill follows from that sentence.

## Reach for it when

- A script makes more than one network, LLM, or paid API call.
- Work is measured in minutes or dollars, and losing it halfway hurts.
- You are about to hand-roll checkpointing — a progress JSON, a `results/` directory checked
  for existing files, a `--resume-from` flag. Use durably instead; that is the exact job.

Do not reach for it for a fast pure computation, a worker fleet behind a load balancer
(that is Temporal's job), or persistent scheduling (let cron or launchd invoke the script —
durably makes the invocation idempotent, which is the hard part).

## Set it up

Match the repository's existing package manager; do not switch runtimes for this.

```bash
bun add @nikhilverma/durably      # or: npm install @nikhilverma/durably
```

Zero dependencies, zero native addons, Node ≥ 20. Pick one of two entry points:

- **`run(wf, input, opts?)`** — for a script a person or agent invokes. Persists to `./.durably`,
  resumes a matching unfinished run, and **returns the workflow's value directly**. Never a
  metadata wrapper — do not write `result.value` or `result.output`.
- **`durably`** — the lazy default engine, for enqueueing work and operating runs by id
  (`enqueue`, `inspect`, `list`, `retry`, `restart`, `cancel`, `signal`, `pause`, `resume`).

```ts
import { workflow, run } from '@nikhilverma/durably'

const digest = workflow<{ urls: string[] }>()(async (ctx, { urls }) => {
  const pages = await ctx.parallel(
    urls.map(url => () => ctx.step(() => fetch(url).then(r => r.text()), { timeoutMs: 10_000 })),
    { concurrency: 5 },
  )
  const ok = pages.filter(p => p.ok).map(p => p.value)
  const summary = await ctx.step(() => llm.summarize(ok), {
    retry: { attempts: 4, backoff: 'exponential', baseMs: 500 },
  })
  return { summary, failed: pages.length - ok.length }
})

console.log(await run(digest, { urls: process.argv.slice(2) }))
```

The extra `()` in `workflow<Input>()(fn)` lets TypeScript fix the input type and still infer the
result. If the callback params are already typed, `workflow(fn)` infers both.

## Four rules that keep replay correct

1. **Every effect goes inside `ctx.step()`.** Network calls, file writes, database queries,
   spending money, mutating anything outside the function.
2. **Code between steps must be pure.** Branching, mapping, and deriving are the point — that
   is what makes dynamic control flow free. But use `ctx.now()` instead of `Date.now()`,
   `ctx.random()` instead of `Math.random()`, and `ctx.log()` instead of `console.log`
   (`console.log` between steps prints again on every replay; `ctx.log` records once).
3. **Never call `ctx.step` — or any `ctx.*` durable operation — from inside a step callback.**
   The inner call allocates a sibling path that vanishes on replay. This fails loudly with
   `PurityError: step at N diverged during shadow replay`, but only once a test crashes the
   run, so write it correctly the first time. Split it into two sibling steps instead.
4. **Step outputs, loop state, workflow inputs, and results cross the disk boundary.** They are
   deep-cloned through a strict serializer before being recorded.

   | Survives | Rejected with `SerializationError` |
   | --- | --- |
   | strings, booleans, finite numbers, `null` | `Infinity`, `NaN` |
   | valid `Date` objects | `bigint`, `symbol`, functions |
   | plain objects and arrays | class instances — `Map`, `Set`, `URL`, `Error`, your own classes |
   | `undefined`, normalized exactly as `JSON.stringify` does | cyclic references |

   Return plain data. Convert a `Map` to an object and a `URL` to a string at the step boundary,
   not two steps later where the failure reads as a mystery. `undefined` follows JSON: an
   undefined property is dropped, an undefined array element becomes `null`, so an optional
   `field?: T` is safe to pass to `run()` and hashes the same as an absent one.

## Pick the primitive

| The situation | Use |
| --- | --- |
| One effect | `ctx.step(fn, opts)` |
| N effects concurrently, same run | `ctx.parallel(thunks, { concurrency })` → `Result<T>[]` |
| N units of work each wanting its own retries, budget, and lifecycle | `ctx.spawnAll([[wf, input], …])` + `ctx.joinAll(handles)` |
| An agent loop, or hundreds-to-thousands of iterations | `ctx.loop(state0, reducer, { snapshotEvery, maxIterations })` |
| Progress *inside* one long step (a stream, a partial turn) | the step's `stash(v)` / `stashed` |
| Human approval, or an external event | `ctx.waitFor(name, schema?, { timeoutMs })`, released by `durably.signal(runId, name, payload)` |
| Wait for wall-clock time | `ctx.sleep(ms)` / `ctx.sleepUntil(date)` |
| Watch a long run's progress from outside | `run(wf, input, { onStep })` |
| Undo completed work when a later step fails | `compensate` on the step that did it |

`parallel` and `joinAll` return `Result<T>[]` = `{ ok: true, value } | { ok: false, error }`.
Failures are values, never swallowed — handle them, or use the exported `isOk` / `partition`.
Never pass that array on as if it were `T[]`.

`ctx.parallel` runs every thunk at once unless given `{ concurrency }`; an engine runs 4 *runs*
at a time unless given `createEngine({ concurrency })`, and that is what throttles `spawnAll`
(a wider fan-out than the cap raises `CONCURRENCY_CAPPED` with both numbers).
Past ~10⁴ items, batch them into a few hundred steps and keep per-item idempotency in your own
ledger inside each step.

Reach for `ctx.loop` past a few hundred iterations. Plain step replay holds every prior output
in memory and re-runs the pure code each time; `loop` snapshots state so resume is O(1). Keep
loop state small — you control the reducer, so compact history yourself.

## Attach policy as data

```ts
await ctx.step(({ signal }) => call({ signal }), {
  name: 'charge-card',                 // stable label; renaming later makes the run stale
  retry: { attempts: 5, backoff: 'exponential', baseMs: 200, maxMs: 10_000, jitter: true },
  timeoutMs: 5_000,                    // per attempt
  schema: Extraction,                  // any Standard Schema (Zod, Valibot, ArkType)
  compensate: (out) => release(out.id),// saga rollback, LIFO, itself durable
  concurrencyKey: new URL(url).host,   // default limit 1 per key
  uses: 'openai',                      // shared rate limit + circuit breaker
})
```

When steps cost money, pass `checkpointEvery: 1`. A checkpoint rewrites the whole run state, so
the default scales with run length — every step up to 100, then every one percent of them — and a
crash re-executes whatever the last checkpoint missed. A step is also the unit of durability: one
killed at 99% yields nothing and re-runs whole, so use `stash` for progress inside a long step.

Retries are **off by default** — a failing step fails the run. Add them where the failure is
plausibly transient. Validate with `schema` where an LLM produced the value; recorded outputs
are re-validated on replay, which is a free safety net across code edits.

Cap spend with `run(wf, input, { budget: { usd: 5 } })` and report it with `ctx.charge({ usd })`.
Budget bounds *your intended spend*; it cannot see the provider's ledger. A 402 or an exhausted
quota is the breaker's and retry's job.

## Test it — one crash test, minimum

`testEngine()` runs in memory with a fake clock, and **shadow replay is on by default**: after
each completed run it silently replays and asserts an identical step-path sequence with zero
new executions. Any control-flow purity violation therefore fails tests you were writing anyway.

```ts
import { testEngine } from '@nikhilverma/durably/test'

it('never re-bills the LLM call across a crash', async () => {
  const te = testEngine()
  const crashed = await te.run(digest, { urls }, { crashAfter: 'summarize' })
  const done = await te.resume(crashed.runId)

  expect(done.status).toBe('completed')
  expect(done.steps.every(s => s.executions === 1)).toBe(true)
})
```

`crashAfter` kills the run once a step completes; `crashInStep` kills mid-attempt (use it to
test `stash`). `te.signal(runId, name, payload)` releases a `waitFor`; `te.clock.advance(ms)`
moves durable timers. A durably workflow without a crash test is untested.

## Debug a real run

The filesystem is the UI. There is no dashboard and none is needed.

```bash
cat .durably/runs/<runId>/state.json     # where is it NOW
grep -c . .durably/runs/<runId>/events.log
```

`durably.inspect(runId)` returns the identical object; `durably.list({ status: 'failed' })` is
the dead-letter queue; `durably.list({ key })` finds the run behind the key you chose, which is
the only name a human remembers. While a run is still going, `run(wf, input, { onStep })` streams
`{ label, status, attempt, ms }` — `replayed` means it came from the log, `retrying` is what makes
a five-minute retry loop distinguishable from a slow step. Read `status` first:

| `status` | What it means | Move |
| --- | --- | --- |
| `waiting` | parked on `waitFor` — see `waitingFor` | `durably.signal(runId, name, payload)` |
| `sleeping` | durable timer not yet due | wait, or advance the clock in tests |
| `paused` | someone called `pause` | `durably.resume(runId)` |
| `failed` | a step exhausted its policy — `error` names it | fix, re-run the same command, or `durably.retry(runId, { fromStep })` |
| `stale` | the workflow body changed incompatibly | `durably.restart(runId)`, or `durably.adopt(runId)` if you have audited the change |

Before proposing a fix, read `advisories[]` and the error's `hint` and `docs`. Every
`DurablyError` carries the next move and the run's active warnings; the advisory codes
(`LOOP_SUGGESTED`, `FANOUT_CEILING`, `RETRY_STORM`, `STASH_SUGGESTED`, `BUDGET_NEAR`,
`WAITFOR_NO_TIMEOUT`, `SNAPSHOT_HEAVY`, `SLOW_STEP_NO_TIMEOUT`) each name an implemented
primitive to switch to. Do not invent a diagnosis the run already printed.

A `KeyConflictError` is self-describing: it names the run holding the key and both inputs
(`key`, `runId`, `boundInput`, `input`), so decide from the message whether the change was
meaningful — reuse the bound input, pick a new key, or pass `{ fresh: true }`.

Note what re-running a command does, which surprises people: an **unfinished** run resumes, a
**failed** run resumes from the failed step (re-running *is* the retry — a permanently bad input
hits the same wall every time until `{ fresh: true }`), a run whose lease another live process
holds is subscribed to rather than duplicated, and a **completed** run starts fresh. Completed
runs are deliberately not memoized: re-running a successful command means "do it again".

## Edit a workflow that already has runs

Each run records a hash of the workflow function's own body. A changed body gets an effect-free
compatibility replay before it resumes.

- **Resumes automatically:** retry attempts, backoff, jitter, timeouts, parallel concurrency,
  keyed concurrency, loop limits, wait timeouts, and the code inside failed or not-yet-started
  step callbacks.
- **Goes stale:** inserting, removing, reordering, or renaming a recorded step; a schema that now
  rejects recorded output; changed `compensate` for a step that already completed; changed wait
  identity.

So when editing a live workflow, add new steps *after* the recorded ones, and leave existing
`name` labels alone. The known hole: a helper called *between* steps can change branching without
changing the hash — shadow replay in tests is the check.

## Report honestly

Say which steps re-executed and which returned from the log; those are different claims and the
`executions` count in a test result settles it. If a run is parked or stale, say so plainly and
name the operation that unparks it rather than starting a fresh run to make output appear.

## Full API

`reference.md` in this skill directory carries the complete surface — every `ctx` method, engine
operations, storage adapters, error classes, and advisory semantics. Read it when the task needs
something beyond the primitives above. Live docs: <https://nikhil-verma.com/durably/docs/>.
