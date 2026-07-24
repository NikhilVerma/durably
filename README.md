# durably

[Website](https://nikhil-verma.com/durably/) · [Documentation](https://nikhil-verma.com/durably/docs/) · [npm](https://www.npmjs.com/package/@nikhilverma/durably)

**Durable workflows for scripts and agents. One process, no server, no setup, no magic.**

Your script makes ten LLM calls. Two fail. The process dies at call seven. Run the same command again: it finishes calls eight through ten — the first seven are never re-executed, never re-billed.

```bash
npm install @nikhilverma/durably        # zero dependencies, zero native addons
```

```ts
// digest.ts
import { workflow, run } from '@nikhilverma/durably'
import { generateText } from 'ai'

type Input = { urls: string[] }

const summarize = workflow<{ url: string }>()(async (ctx, { url }) => {
  const page = await ctx.step(() => fetch(url).then(r => r.text()), { timeoutMs: 10_000 })
  const s = await ctx.step(
    () => generateText({ model, prompt: `Summarize:\n${page}` }),
    { retry: { attempts: 4, backoff: 'exponential', baseMs: 500 } },
  )
  return { url, title: s.text.slice(0, 60), summary: s.text }
})

const digest = workflow<Input>()(async (ctx, { urls }) => {
  const kids = await ctx.spawnAll(urls.map(url => [summarize, { url }]))   // each child: own run, own retries
  const results = await ctx.joinAll(kids)                                   // Result[] — failures are values

  const ok = results.filter(r => r.ok).map(r => r.value)
  const intro = await ctx.step(() => generateText({ model, prompt: `Intro for: ${ok.map(s => s.title).join(', ')}` }))
  return { intro: intro.text, items: ok, failed: results.length - ok.length }
})

console.log(await run(digest, { urls: process.argv.slice(2) }))
```

```bash
$ npx tsx digest.ts https://a.example https://b.example  # ...8 more
  ✓ fetch(url)                              × 10 children
  ✗ generateText — attempt 1 failed, retrying in 500ms          × 2 flaky ones
  ✓ generateText                            × 10 children
^C                                          # or a crash, a deploy, laptop sleep

$ npx tsx digest.ts https://a.example https://b.example  # same command
  ✓ intro generateText                      # only unfinished work runs
{ intro: '...', items: [...], failed: 0 }
```

`run()` persists to `.durably/` in your working directory, resumes the matching unfinished run if one exists, and returns the result. No engine setup, no config, no schema library.

The extra `()` in `workflow<Input>()(fn)` is deliberate: it lets TypeScript fix the input type and still infer the result type from `fn`. If the callback parameters are already typed, `workflow(fn)` infers both without currying; the Standard Schema form stays `workflow({ input: schema }, fn)`.

**One mental model to hold: on resume, your workflow function re-executes from the top, and completed `ctx.step()` calls return their recorded results instead of re-running.** Effects live inside steps; the code between them must be pure. Everything below is optional depth on that one sentence.

---

## First principles

1. **Built for the script you run, not the service you operate.** The unit of use is an agent script, a CLI tool, a local pipeline — something a person or an AI agent invokes, that should survive interruption, and that finishes. Not a fleet of workers behind a load balancer.
2. **Sane defaults, escape hatches when needed.** `run()` with zero config is the normal path. Storage, concurrency, retry behavior, and lease times remain configurable; most scripts should not need those controls at first.
3. **No magic.** No compiler transforms, no directives, no injected globals. durably's replay model is three sentences, its limits are stated plainly ([Design notes](#design-notes-sharp-edges-named-deliberately)), and what you write is what runs.
4. **The filesystem is the UI.** Runs are directories. History is grep-able NDJSON. Current state is one `state.json` you can `cat`. An agent debugging a stuck workflow needs file tools, not a dashboard.
5. **TypeScript is the schema.** Input and result types flow through the API. Optional runtime validation is available where data crosses the disk boundary without adding a required schema dependency.
6. **Small enough for a context window.** The complete core spec ([AGENTS block](#for-agents-the-complete-core-api)) stays under 2k tokens — a hard release gate. When a feature competes with that budget, the feature loses or moves to advanced docs.
7. **Progressive disclosure, signposted cliffs.** Five concepts cover the common path: workflow, run, step, parallel, and retry. Log growth stays internal. When a real ceiling remains, an advisory arrives through the API before the run reaches it (see [Advisories](#advisories-the-api-teaches-itself)).

## Goals

- Durable, resumable, exactly-once-per-step execution of multi-step local workflows
- First-class agent ergonomics: LLM fan-out, durable agent loops, intra-step recovery hooks, budgets, inspectable state, greppable logs
- Retries, backoff, timeouts, circuit breakers, rate limits, keyed concurrency — one small policy vocabulary
- Sagas (typed compensation), fan-out/in, human-in-the-loop pauses, durable sleep
- Pluggable persistence behind a 7-method event-log adapter; hardened NDJSON files by default, built-in `node:sqlite` alternative

## Anti-goals

Refusals, not roadmap gaps:

- **Distributed execution.** No clusters, no worker fleets, no task routing. Two processes sharing a storage dir get failover via lease reclamation; that's the ceiling. Need more → Temporal.
- **Being a job queue or event bus.** No brokers, no event triggers, no debounce/batching. `signal()` is the escape hatch to the outside world.
- **Persistent cron.** Durable schedules imply an always-alive daemon — that's an operating system's job. Have cron/launchd invoke your script; durably makes the invocation idempotent and resumable, which is the hard part anyway.
- **Dashboards and UIs.** `state.json` + `inspect()` + your terminal. Build your own on `list()` if you want one. Same spirit: durably is a **library first** — a `durably-cli` (terminal sugar over `list`/`inspect`/`retry`) may exist as a separate package, but core never requires or assumes it.
- **Workflow versioning/patching.** Recorded topology must remain compatible (see [Code changes](#code-changes-and-staleness)). Policy edits and future step implementations resume automatically; incompatible edits become stale without version markers or patch APIs.
- **An Agent class.** durably ships no model bindings, no message types, no agent loop that owns your architecture. It ships the durability primitives ([Agents on durably](#agents-on-durably)) and recipes; integrations (`durably-contrib/ai-sdk`) live outside core.
- **Production transaction orchestration at service scale.** Wrong tool. See Temporal, DBOS, Restate.

---

## The model

A workflow function may re-execute from the top many times — after a crash, a `waitFor`, a `sleep`. Each `ctx.step()` call is matched to the run's log **by position**: already-completed steps return their recorded result instantly instead of re-running. Therefore: **all side effects go inside `ctx.step()`; code between steps must be pure** (branching, mapping, deriving — fine; API calls, file writes, `Date.now()`, `Math.random()` — use `ctx.step`, `ctx.now()`, `ctx.random()` instead).

**Positions are hierarchical paths, not a flat counter.** Top-level steps: `[0], [1], [2]…`. Inside `ctx.parallel`, each branch owns its own counter: the *j*-th step of branch *i* of the *k*-th parallel is `[k, i, j]`, nesting recursively. This makes step identity independent of scheduling — concurrent branches completing in any order produce the same paths on every execution. (A flat counter would make replay depend on real completion timing; see [Design notes](#design-notes-sharp-edges-named-deliberately).)

**What's enforced, honestly.** durably records each run's step-path sequence. A replay that diverges — the purity rule was broken in a way that *changed control flow* — fails immediately with `PurityError{path, expected, actual}` rather than corrupting state. But a violation that does **not** change control flow is undetectable in principle: a stray `fetch()` or file write between steps, whose result feeds no branch, simply re-executes on every replay, silently, forever. That silent version is the one people actually ship. Two mitigations: keep non-step code trivially pure (the habit is easy), and rely on shadow replay in `testEngine` — every test you write already machine-checks the rule.

### Labels

Steps are identified by path but *labeled* automatically from the callback source (`generateText({ model, pro…`) so logs and state stay readable with zero API. Pass `{ name: 'charge-card' }` for a stable label. Ordinary replay selects recorded results by path. The code-change compatibility check also requires recorded labels to match, so renaming an existing step becomes stale instead of silently retargeting selectors. Selector forms: position path (canonical), string (first label match), `{ label, nth }` (precise).

### Code changes and staleness

<a id="docs-staleness"></a>

Each run records a hash of **the workflow function's own body** (`fn.toString()`) — deliberately not the transitive import graph. An exact hash resumes immediately. When the body changed, durably performs an effect-free compatibility replay against the recorded history before deciding.

Compatibility requires every recorded durable operation to appear at the same path, kind, and label. Completed outputs must pass current schemas, and compensation code for completed effects must be unchanged. No effect callback executes during this check.

Execution-policy edits therefore resume automatically: parallel concurrency; retry attempts, backoff, and jitter; future-attempt timeouts and keyed concurrency; loop snapshot/iteration limits; and wait timeouts. Completed steps keep their recorded results. Failed and unfinished work uses the new policy. Code inside failed or future step callbacks may also change.

Inserting, removing, reordering, or renaming recorded operations is incompatible. So are schemas that reject recorded output, changed compensation for a completed effect, and changed wait/signal identity. Those edits park the run as `stale`.

A stable workflow name makes the family unambiguous. For an anonymous `run()` with changed code, durably checks every resumable run with the same input or key and resumes the structurally compatible history. Give workflows stable names when several unrelated scripts share one storage directory; a key replaces input identity inside a family but does not name the family itself.

The stated hole remains: a helper called *between* steps can change branching without changing the workflow function's hash. Same purity rule, same shadow-replay check.

For a genuinely `stale` run: `durably.restart(runId)` starts fresh under new code, or `durably.adopt(runId)` resumes it with *you* asserting compatibility. Automatic compatibility handles policy edits; `adopt` remains the explicit escape hatch for a structural change you have audited.

### `run()` semantics, precisely

<a id="docs-keys"></a>

The candidate identity is the workflow family plus `canonicalInputHash`; `{ key }` replaces the input half within that family. Input canonicalization is well-defined because inputs are plain JSON data (sorted keys, stable stringify). The body hash is the compatibility fast path. A changed body attaches to resumable history only after structural replay succeeds. Then `run()` is join-or-start-or-resume:

| Existing run for this identity | `run()` does |
| --- | --- |
| unfinished (pending / waiting / sleeping / lease expired) | **resume it** — the crash-restart story |
| lease held by another live process | **subscribe** — wait for its result; two terminals cooperate instead of double-spending |
| latest completed | **start fresh** — re-running a successful command means "do it again", never memoize |
| latest failed | **resume from the failed step** — rerunning *is* the retry; completed steps stay done; `{ fresh: true }` overrides |
| none | **start** |

`run()` returns the workflow's value directly — never a wrapper. Advisories and the `runId` travel via stderr, `onAdvisory`, and thrown errors.

The failed→resume row is the one to internalize: a permanently-bad input will retry at the same wall on every invocation until you pass `fresh` — the failing step and error are one `cat state.json` away.

---

## Guide

### Retries, backoff, timeouts

<a id="docs-timeouts"></a>

Off by default (a failing step fails the run). Policies are plain data:

```ts
await ctx.step(({ signal }) => flaky({ signal }), {
  retry: { attempts: 5, backoff: 'exponential', baseMs: 200, maxMs: 10_000, jitter: true },
  timeoutMs: 5_000,   // per attempt
})
```

### Validation (optional)

<a id="docs-validation"></a>

Types are inferred; add runtime validation only where you want it — any [Standard Schema](https://github.com/standard-schema/standard-schema) library (Zod, Valibot, ArkType); durably depends on none.

```ts
const research = workflow({ input: ResearchInput }, async (ctx, input) => { ... })
const data = await ctx.step(() => llm.extract(doc), { schema: Extraction })
// LLM output is exactly where runtime checks earn their keep; recorded outputs
// are also revalidated on replay — a free safety net across code changes.
```

### Fan-out / fan-in

<a id="docs-fanout"></a>
<a id="docs-cancellation"></a>

```ts
// in-run: parallel steps (cheap concurrency; hierarchical positions keep replay exact)
const results = await ctx.parallel(urls.map(u => () => ctx.step(() => fetchPage(u))), { concurrency: 5 })

// child runs: own retries, budgets, lifecycle; durable barrier survives restarts
const kids = await ctx.spawnAll(items.map(i => [processOne, i]))
const outcomes = await ctx.joinAll(kids)
```

Both return `Result<T>[]` = `{ ok: true, value } | { ok: false, error }` — failures are typed values, never swallowed. Cancelling a parent cancels children by default (`{ detached: true }` to opt out).

### Keyed concurrency

```ts
await ctx.step(() => scrape(url), { concurrencyKey: new URL(url).host })   // default limit 1/key
```

### Sagas

```ts
const held = await ctx.step(() => inventory.hold(sku), {
  compensate: (h) => inventory.release(h.holdId),   // receives this step's typed output
})
// later step exhausts retries → compensations run LIFO, durably (crash mid-rollback resumes rollback)
```

### Pause, signals, human-in-the-loop

<a id="docs-wait-for"></a>

```ts
const approval = await ctx.waitFor('approval', ApprovalSchema, { timeoutMs: 72 * 3600_000 })
// run parks as 'waiting' — zero process cost, survives restarts
```

Release from anywhere: `await durably.signal(runId, 'approval', { by: 'nikhil' })`. Operational pause: `durably.pause(runId)` / `durably.resume(runId)` — lands at the next step or loop-iteration boundary.

### Durable timers

```ts
await ctx.sleep(6 * 3600_000)
await ctx.sleepUntil(new Date('2026-08-01T09:00:00Z'))
// wake time is persisted; whichever process next touches the run past it continues
```

### Dynamic flow

Not a feature — the model makes it free:

```ts
const triage = await ctx.step(() => llm.classify(ticket), { schema: Triage })
if (triage.kind === 'escalate') return { routed: (await ctx.waitFor('decision', Decision)).team }
```

### Budgets

<a id="docs-budgets"></a>

Per-run and engine-wide (the cap an agent fleet actually needs):

```ts
const r = await run(research, { topic }, { budget: { usd: 5 } })
// advanced: createEngine({ budget: { usd: 200 } }) — all runs draw from the shared cap

ctx.charge({ usd: draft.cost })
while (ctx.budget.remaining('usd') > 0.5) { ... }
// exceeding either cap → BudgetExceededError at next charge/step
```

Be precise about what budget is: it bounds **your intended spend** — it cannot see the provider's ledger. An account that's already maxed out, a 402, an exhausted quota: that's the *provider* saying no, and it's the breaker's and retry's job. The composition is the pattern: breaker opens on quota errors, long backoff or `waitFor('quota-restored')` rides out the outage, and budget guarantees the retries themselves can't compound the bill.

### Progress annotations

```ts
ctx.annotate({ phase: 'reviewing', reviewed: 12, total: 40 })   // shallow-merged into state.json
```

### Replay-aware logging

If your `console.log` between steps printed three times after two resumes — that's replay, working as designed. `ctx.log` records on first execution and stays silent on replay:

```ts
ctx.log('classified as', triage.kind)   // once per run, into events.log + stdout
```

(A `for` accumulator or derived value between steps is fine — pure recomputation is the point. It's *effects* that bite.)

## Advisories: the API teaches itself

durably watches each run for approaching ceilings and missed tools, and surfaces advisories inside the responses you already read — `state.json`, `inspect()`, and the `testEngine` result object; for `run()`, advisories surface via stderr-once, an optional `onAdvisory` callback, and any thrown `DurablyError` (which carries the run's active advisories — most diagnostic exactly when the run fails). An agent never hunts for warnings; they land in its context as a side effect of doing what it was already doing.

```json
"advisories": [
  { "code": "LOOP_SUGGESTED", "level": "warn",
    "msg": "2,000+ steps in a loop-shaped pattern; replay cost grows linearly",
    "hint": "ctx.loop checkpoints state snapshots — resume becomes O(1)",
    "docs": "https://www.npmjs.com/package/@nikhilverma/durably#docs-loop",
    "count": 1, "firstAt": "…" }
]
```

The advisory channel has fixed spam limits:

- Codes are deduplicated per run. `count` increments without repeating the message.
- Thresholds emit once per severity crossing (info at 1k steps, warn at 5k), rather than per iteration.
- Each run keeps at most 10 active advisories.
- `advisories: 'silent'` disables the channel.
- Level `warn` is reserved for approaching a stated ceiling. Style opinions stay at `info` or below.

Initial codes: `LOOP_SUGGESTED` · `FANOUT_CEILING` (children nearing storage ceiling → batch-API recipe) · `RETRY_STORM` (same step, same error class, N straight failures → breaker/resource hint) · `STASH_SUGGESTED` (a step re-ran from zero after a crash and its first attempt exceeded 30s) · `BUDGET_NEAR` (80%) · `WAITFOR_NO_TIMEOUT` · `SNAPSHOT_HEAVY` (loop state size × frequency) · `SLOW_STEP_NO_TIMEOUT`.

### Priorities, dead letters

```ts
await durably.enqueue(backfill, input, { priority: -10 })
const dead = await durably.list({ status: 'failed' })          // the DLQ is a query
await durably.retry(dead[0].runId, { fromStep: 'publish' })    // failed run: completed steps stay done
await durably.restart(staleRun.runId)                          // stale run: fresh start under new code
```

---

## Agents on durably

Three layers. The first costs nothing; the other two are the reason durably exists.

### Layer 0 — an agent loop is already a workflow

Each LLM turn is a step; each effectful tool execution is a step; `messages` is pure accumulation of step outputs. Crash at turn 500 → replay reconstructs 500 turns from recorded results in milliseconds and continues at 501. Temporal's workflow/activity split maps exactly onto durably's between-steps/inside-steps: tool *idempotency* is solved by never re-running recorded steps, not by demanding idempotent tools. No agent class, no lock-in — bring the AI SDK, a raw fetch, anything.

### Layer 1 — `ctx.loop`: the durable reducer

<a id="docs-loop"></a>
<a id="snapshot-size"></a>

At thousands of turns, step-replay has a cost: re-running the loop's pure code N times and holding N outputs. `ctx.loop` checkpoints **state snapshots** instead — resume is O(1) at turn 5,000:

```ts
const final = await ctx.loop(
  { messages: seed as unknown[], turn: 0 },                // serializable state
  async (state, { step, done }) => {
    const res = await step(() => generateText({ model, messages: state.messages, tools }))
    if (res.finishReason === 'stop') return done({ answer: res.text })
    const toolOut = await Promise.all(res.toolCalls.map(tc => step(() => execTool(tc))))
    return { messages: [...state.messages, ...res.msgs, ...toolOut], turn: state.turn + 1 }
  },
  { snapshotEvery: 10, maxIterations: 2_000 },
)
```

Resume loads the latest snapshot and continues — prior iterations are never replayed, and their step records compact away beneath the snapshot. This is Temporal's `continueAsNew` as an internal mechanism instead of a user-facing workaround. Iteration boundaries are also where `pause()` lands and where `waitFor`/budget checks naturally live.

### Layer 2 — `stash`: intra-step recovery, your hook

<a id="docs-stash"></a>

For resumability *inside one step* — an interrupted stream, a partial agent turn (the problem Cloudflare's harness solves with recovery data):

```ts
await ctx.step(async ({ signal, stash, stashed }) => {
  const progress = stashed as { text?: string; offset?: number } | undefined
  let acc = progress?.text ?? ''                           // progress from a crashed prior attempt
  for await (const chunk of streamText({ prompt, resumeFrom: progress?.offset, signal })) {
    acc += chunk
    if (acc.length - (progress?.offset ?? 0) > 4096) await stash({ text: acc, offset: acc.length })
  }
  return acc
})
```

durably owns the mechanics — `stash(v)` persists to the run log, survives crashes, is scoped to this step, handed back as `stashed` on the next attempt, cleared on completion. *What* to stash and how to resume from it is your contract with your provider. No magic, maximal leverage.

---

## Observability: the filesystem is the UI

```
.durably/runs/01J8ZKQ4/
├── state.json      # where is it NOW — one cat answers it
└── events.log      # what happened — length-prefixed, checksummed NDJSON records
                    # (chunked + compacted internally; torn tails self-heal on read)
```

```bash
$ cat .durably/runs/01J8ZKQ4/state.json
{
  "status": "waiting",
  "workflow": "digest",
  "waitingFor": { "signal": "approval", "since": "2026-07-24T10:12:03Z" },
  "progress": { "completed": 7, "current": "generateText({ model, pro…" },
  "annotations": { "phase": "reviewing", "reviewed": 12, "total": 40 },
  "steps": [
    { "path": [0], "label": "fetch(url)", "status": "ok", "ms": 812, "attempts": 1 },
    { "path": [1,0,1], "label": "generateText", "status": "retrying", "attempt": 3, "nextRetryAt": "…" }
  ],
  "loop": { "iteration": 214, "lastSnapshot": 210 },
  "budget": { "usd": { "spent": 1.42, "limit": 5 } },
  "advisories": [
    { "code": "LOOP_SUGGESTED", "level": "warn",
      "msg": "2,000+ steps in a loop-shaped pattern; replay cost grows linearly",
      "hint": "ctx.loop checkpoints state snapshots — resume becomes O(1)",
      "docs": "https://www.npmjs.com/package/@nikhilverma/durably#docs-loop",
      "count": 1, "firstAt": "…" }
  ],
  "error": null,
  "children": ["01J8ZKR1", "01J8ZKR2"]
}
```

`durably.inspect(runId)` returns the identical object programmatically. That file is the answer to "where is my workflow" — for you, and for the agent you send to find out.

---

## For agents: the complete core API

````
AGENTS.md — durably CORE (≤2k tokens, everything needed to be productive)

MODEL: workflow fn replays from top after crash/wait/sleep. ctx.step() matches the
log BY POSITION PATH. Parallel branches own local counters, so scheduling never
changes identity. Completed steps return recorded results and never re-execute.
RULE: effects only inside ctx.step(); between steps use pure code, ctx.now(), and
ctx.random(). Changed control flow throws PurityError; other purity violations are
undetectable outside shadow replay. A changed workflow body gets effect-free
compatibility replay: policy edits and future/retrying step code resume; changed
recorded topology, rejected stored output, or changed compensation becomes stale.

DEFINE + RUN
  const wf = workflow<Input>()(async (ctx, input) => result)
  const wf = workflow({ input?, output?, name? }, fn)      // Standard Schema
  await run(wf, input, {key?, fresh?, budget?, dir?, advisories?,
                        onAdvisory?: (a: Advisory)=>void}) -> result
    Returns your workflow value directly, always; never a metadata wrapper.
  Candidate identity is (workflow,inputHash), or (workflow,key); bodyHash is the
  compatibility fast path and changed bodies must match recorded durable paths:
    unfinished=>resume · live lease=>subscribe · completed=>fresh run ·
    failed=>resume failed step (fresh:true starts over) · none=>start

CTX
  step(fn, opts?) -> Promise<T>
    fn({signal,stash,stashed}); opts: {name?, retry?: {attempts,
    backoff:'exponential'|'linear'|'none',baseMs?,maxMs?,jitter?}, timeoutMs?,
    schema?, compensate?, concurrencyKey?, concurrencyLimit?=1}
  parallel(thunks,{concurrency?}?) -> Result<T>[]
    Result<T> = {ok:true,value:T}|{ok:false,error}
  log(...args) records once and is silent on replay
  now()/random() are replay-safe; return ctx.complete(v) exits and infers v

DEFAULT ENGINE  import { durably } from '@nikhilverma/durably' // lazy, ./.durably
  enqueue(wf,input,{key?,priority?,delayMs?,budget?}) -> handle
  inspect(runId) -> state|null; list({status?,workflow?,limit?})
  retry(runId,{fromStep?}); restart(runId); cancel(runId)

ERRORS extend DurablyError: ValidationError, StepTimeoutError, PurityError,
  StaleRunError, BudgetExceededError, CircuitOpenError, KeyConflictError,
  RunCancelledError, LeaseLostError. Every DurablyError carries {hint,docs,runId,
  advisories[]}: the next move and active run warnings, named. StaleRunError says
  "restart(id) starts fresh; adopt(id) resumes if you assert replay compatibility."
  Errors are documentation; an agent reading one self-corrects in a single turn.

TEST  const te = testEngine()                        // memory + fake clock
  Shadow replay is default: each completed run replays and asserts identical paths
  plus zero new executions.
  te.run(wf,input,{crashAfter?,crashInStep?,budget?,advisories?})
    -> {runId,status,result,steps,advisories}
  te.resume(runId); te.signal(runId,name,payload); te.clock.advance(ms)

OBSERVE: .durably/runs/<id>/state.json + events.log. state.json, inspect(), testEngine
results, and errors surface advisories[]/hints; run() uses stderr/onAdvisory/errors.
READ THEM: they name approaching limits and the implemented next move.
````

````
AGENTS-EXTENDED (skippable until an advisory or task points here; soft ≤1.5k tokens)

LOOP + RECOVERY
  loop(state0, async (state,{step,done}) => state|done(v),
       {snapshotEvery?=1,maxIterations?}) -> Promise<V>
    Snapshots make resume O(1); old iteration steps compact away.
  step context stash(v) persists partial attempt progress; stashed restores it on
    the next attempt and clears on completion.

TOPOLOGY + PARKING
  spawn(wf,input,{detached?}); spawnAll([[wf,input],...]); joinAll(handles)
  waitFor(name,schema?,{timeoutMs?}); sleep(ms); sleepUntil(date)

BUDGET + STATE
  budget.remaining/spent(kind); charge({usd?,tokens?,units?}); annotate(obj)
  ctx.runId is the current durable run id; ctx.attempt is the workflow attempt.

OPERATIONS
  signal(runId,name,payload); pause/resume(runId); adopt(runId)
  adopt resumes stale code only when you assert compatibility.

RESOURCES
  resource(name,{rateLimit?,breaker?,concurrency?}); step(...,{uses:name})

ENGINES + STORAGE
  createEngine({storage?,resources?,concurrency?,budget?,hooks?})
  FileStorage(dir); MemoryStorage(); SqliteStorage(path) from @nikhilverma/durably/sqlite.
  StorageAdapter has init,createRun,append,read,claim,heartbeat,list; append must be
  atomic and ordered per run.

ADVISORIES
  LOOP_SUGGESTED, FANOUT_CEILING, RETRY_STORM, STASH_SUGGESTED, BUDGET_NEAR,
  WAITFOR_NO_TIMEOUT, SNAPSHOT_HEAVY, SLOW_STEP_NO_TIMEOUT. Dedupe by code, emit
  once per severity crossing, cap 10 active per run, stderr once, or silence with
  advisories:'silent'.
````

---

## Testing

**Shadow replay is on by default:** after every `te.run()` completes, testEngine silently replays the run and asserts an identical step-path sequence with zero new executions. Any control-flow purity violation therefore fails your *existing* tests — you get durability checking without writing durability tests, and the replay rule is machine-verified in minute three instead of discovered in month three. Shadow replay is also why durably ships no lint plugin: the rule is checked in-band, by tests you were writing anyway.

```ts
import { testEngine } from '@nikhilverma/durably/test'

it('never re-bills an LLM call across a crash', async () => {
  const te = testEngine()
  const crashed = await te.run(digest, { urls }, { crashAfter: 'generateText' })
  const done = await te.resume(crashed.runId)
  expect(done.steps.filter(s => s.label.includes('generateText'))
                   .every(s => s.executions === 1)).toBe(true)
})

it('resumes an agent loop from its snapshot, not from turn zero', async () => {
  const te = testEngine()
  const crashed = await te.run(agentTask, { goal }, { crashAfter: { label: 'generateText', nth: 47 } })
  const done = await te.resume(crashed.runId)
  expect(done.status).toBe('completed')
  // snapshot resume: turns 1–40 not replayed as steps at all
})

it('hands stashed progress back after a mid-step crash', async () => {
  const te = testEngine()
  const crashed = await te.run(streamTask, input, { crashInStep: 'streamText' })
  const done = await te.resume(crashed.runId)
  expect(done.steps[0].executions).toBe(2)          // step DID re-run —
  expect((done.result as { resumedFromOffset: number }).resumedFromOffset)
    .toBeGreaterThan(0)                            // — but from the stash
})
```

---

## Advanced

### Long-lived engines

<a id="docs-leases"></a>

```ts
import { createEngine, FileStorage } from '@nikhilverma/durably'
const engine = createEngine({
  storage: new FileStorage('~/.myagent/durably'),
  resources: [openai],                 // shared rate limits + circuit breakers, see below
  concurrency: 8,
  budget: { usd: 200 },                // fleet-wide cap; run budgets draw from it
  hooks: { onRunFailed: ({ runId, error }) => notify(error) },
})
await engine.start()
```

### Shared resources

<a id="docs-resources"></a>

Limits that must hold across runs live on named resources: `resource('openai', { rateLimit: { max: 60, perMs: 60_000 }, breaker: { failureRate: 0.5, windowMs: 30_000, cooldownMs: 10_000 }, concurrency: 8 })`, attached via `ctx.step(fn, { uses: 'openai' })`. An open breaker throws retryable `CircuitOpenError` — exponential backoff rides out the cooldown.

### Storage

Two built-ins, both zero-dependency:

- **`FileStorage`** (default) — one directory per run; events as length-prefixed, CRC-checksummed records, so a torn tail from a crash truncates cleanly on read instead of corrupting the run; per-run lockfile prevents interleaved writers after lease reclamation. Greppable, cat-able, agent-debuggable.
- **`SqliteStorage`** — built on **`node:sqlite`** (Node ≥ 22.5, in the runtime, no native addon, no setup). One line to switch when you want transactional guarantees, network filesystems, or Windows without caveats: `createEngine({ storage: new SqliteStorage('./durably.db') })`.

Custom backends implement the 7-method event-log `StorageAdapter` (`init, createRun, append, read, claim, heartbeat, list` — append atomic + ordered per run). It maps directly onto a Redis stream, a Postgres table, a Mongo collection, or S3. Community adapters: `durably-contrib`.

---

## Design notes: sharp edges, named deliberately

<a id="docs-errors"></a>
<a id="docs-serialization"></a>
<a id="docs-purity"></a>

These are the tradeoffs that matter in practice:

1. **Parallel replay identity.** A flat step counter breaks under `parallel`: branches with differing step counts interleave by real completion timing, so replay order ≠ original order → spurious purity errors blaming the user for a scheduler artifact. durably's hierarchical position paths (branch-local counters) exist specifically to kill this bug class. If you write a custom combinator, preserve path scoping.
2. **The purity rule is only partially enforceable.** Sequence checks catch violations that alter control flow. A side effect between steps that feeds no branch re-executes on every replay, silently, forever — undetectable in principle. Shadow replay in your tests is your real defense.
3. **Compatibility replay proves recorded history, not arbitrary JavaScript.** A changed body is replayed without effects and must reproduce recorded durable paths. Helpers changed outside the hashed workflow body remain a hole; shadow replay is still the check. New code beyond a failed or unstarted boundary intentionally runs on resume.
4. **`run()` failed→resume retries permanent failures.** A bad input hits the same wall each invocation until `{ fresh: true }`. The failing step and error are in `state.json`.
5. **Plain-file durability has limits.** Checksummed records + torn-tail truncation + lockfiles make `FileStorage` solid on local disks and acceptable on Windows. Use `SqliteStorage` on network filesystems or when you want transactional claims.
6. **`loop` state must be serializable and reasonably small.** Snapshots are the resume path; a 50 MB messages array snapshotted every turn is your I/O bill. Compact your own history (you control the reducer) or raise `snapshotEvery`.
7. **Fan-out has a ceiling — know where durably taps out.** In-run `parallel` is comfortable to ~10⁴ steps. Child runs: ~10³–10⁴ on `FileStorage` (directory entries, fd churn, `list()` scans become the limit before the model does), ~10⁵ on `SqliteStorage`. Past ~10⁵ items, reach for your provider's Batch API — and that's a recipe, not a defeat: submit the batch as one step, poll with `sleep` + a step in a loop, and durably babysits the batch job instead of impersonating one.

---

## Prior art & inspiration

durably invents very little; it picks a point in a well-explored space and says no to the rest. Credit where due:

- **[Temporal](https://temporal.io)** — defined the category: replay, activities, signals, timers, sagas. durably is Temporal's core idea with the distributed systems, determinism ceremony, and versioning apparatus deliberately removed — and its workflow/activity split survives here as between-steps/inside-steps.
- **[Reflow](https://github.com/danfry1/reflow-ts)** — the closest ancestor: single-process, type-safe, SQLite-backed durable workflows with lease-based crash recovery. durably's lease/heartbeat design, test-engine ergonomics, and "no external services" stance come from here; durably diverges on execution model (replayed plain code vs. static chain) and default storage.
- **[DBOS](https://www.dbos.dev)** — proved durable execution works as a library with explicit, un-magical step calls. `ctx.step` is DBOS's model minus Postgres.
- **[Inngest](https://www.inngest.com)** — the flow-control vocabulary: keyed concurrency, throttling, priorities; and taking DX seriously.
- **[Vercel Workflow SDK / WorkflowAgent](https://github.com/vercel/workflow)** — pushed durable-agent ergonomics furthest; durably is partly a reaction to its compiler transforms and runtime coupling, but its ambition level is the benchmark, and its tool-approval-that-survives-suspension pattern shaped `waitFor`.
- **[Cloudflare Workflows, Durable Objects & the Think harness](https://developers.cloudflare.com/agents/)** — per-instance embedded storage validating one-log-per-run; and Think's recovery design (recovery data stashing, partial-progress persistence, bounded recovery budgets) directly inspired `stash` and engine budgets.
- **[Restate](https://restate.dev) / [Resonate](https://resonatehq.io)** — journal-based replay done rigorously; Resonate's "just functions, durable promises" framing shaped durably's API minimalism.
- **[Effect](https://effect.website)** — typed errors as values, interruption done properly, `Schedule`-style policy thinking. Ideas borrowed, dependency and API surface declined.
- **[cockatiel](https://github.com/connor4312/cockatiel) / Polly** — the resilience-policy family: retry, breaker, timeout, bulkhead as one vocabulary.
- **Gunnar Morling's [Persistasaurus](https://www.morling.dev/blog/building-durable-execution-engine-with-sqlite/)** and the 2025–26 "SQLite is enough" wave — articulated the case for local-first durable execution as its own category.
- **Agent-orchestration DSLs** (Claude Code's internal workflow tool, among others) — the `pipeline`/`parallel` topology decomposition, schema-validated step outputs, and budgets as a first-class primitive.

## License

MIT. Release history is maintained in [CHANGELOG.md](./CHANGELOG.md).
