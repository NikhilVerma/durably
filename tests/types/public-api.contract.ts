import {
  BudgetExceededError,
  CircuitOpenError,
  FileStorage,
  DurablyError,
  KeyConflictError,
  LeaseLostError,
  MemoryStorage,
  PurityError,
  RunCancelledError,
  SerializationError,
  StaleRunError,
  StepTimeoutError,
  ValidationError,
  createEngine,
  isOk,
  durably,
  partition,
  resource,
  run,
  testEngine,
  workflow,
  type Result,
  type Advisory,
  type RunHandle,
  type Selector,
  type StandardSchemaV1,
  type StorageAdapter,
  type Workflow,
  type WorkflowCompletion,
} from "../../index.ts";
import { SqliteStorage } from "../../sqlite.ts";

declare const expectType: <T>(value: T) => void;

type Equal<A, B> =
  (<T>() => T extends A ? 1 : 2) extends
  (<T>() => T extends B ? 1 : 2)
    ? true
    : false;
type Assert<T extends true> = T;

const typed = workflow<{ id: string }>()(async (ctx, input) => {
  expectType<string>(ctx.runId);
  expectType<number>(ctx.attempt);
  const value = await ctx.step(() => Promise.resolve(input.id.length));
  return { id: input.id, length: value };
});

type _InputIsPreserved = Assert<Equal<typeof typed, Workflow<{ id: string }, { id: string; length: number }>>>;

const fullyInferred = workflow(async (_ctx, input: { value: number }) => input.value * 2);
expectType<Workflow<{ value: number }, number>>(fullyInferred);

declare const advisory: Advisory;
expectType<
  | "LOOP_SUGGESTED"
  | "FANOUT_CEILING"
  | "RETRY_STORM"
  | "STASH_SUGGESTED"
  | "BUDGET_NEAR"
  | "WAITFOR_NO_TIMEOUT"
  | "SNAPSHOT_HEAVY"
  | "SLOW_STEP_NO_TIMEOUT"
>(advisory.code);
expectType<"info" | "warn">(advisory.level);
expectType<string>(advisory.msg);
expectType<string>(advisory.hint);
expectType<string>(advisory.docs);
expectType<number>(advisory.count);
expectType<string>(advisory.firstAt);

declare const durablyError: DurablyError;
expectType<string>(durablyError.hint);
expectType<string>(durablyError.docs);
expectType<string | undefined>(durablyError.runId);
expectType<Advisory[]>(durablyError.advisories);

const result = await run(typed, { id: "abc" });
expectType<{ id: string; length: number }>(result);

// @ts-expect-error workflow input is inferred and required
await run(typed, { nope: true });

// @ts-expect-error an input is required for a non-void workflow
await run(typed);

await run(typed, { id: "abc" }, {
  key: "customer:abc",
  fresh: true,
  budget: { usd: 5, tokens: 10_000, units: 12 },
  dir: "/tmp/durably",
  onAdvisory: (advisory) => {
    expectType<Advisory>(advisory);
  },
});

type User = { id: string; role: "admin" | "member" };
const UserSchema = {
  "~standard": {
    version: 1 as const,
    vendor: "contract-test",
    validate(value: unknown) {
      return typeof value === "object" && value !== null && "id" in value
        ? { value: value as User }
        : { issues: [{ message: "invalid user" }] };
    },
    types: {
      input: {} as unknown,
      output: {} as User,
    },
  },
} satisfies StandardSchemaV1<unknown, User>;

const schemaWorkflow = workflow(
  { name: "schema-workflow", input: UserSchema, output: UserSchema },
  async (ctx, user) => {
    const checked = await ctx.step(() => Promise.resolve(user), { schema: UserSchema });
    expectType<User>(checked);
    return checked;
  },
);
expectType<Promise<User>>(run(schemaWorkflow, { id: "u1", role: "admin" }));

const mixedSteps = workflow<{ enabled: boolean }>()(async (ctx, input) => {
  const text = await ctx.step(async ({ signal, stash, stashed }) => {
    expectType<AbortSignal>(signal);
    expectType<unknown>(stashed);
    await stash({ offset: 5 });
    return "ok" as const;
  }, {
    name: "effect",
    retry: {
      attempts: 4,
      backoff: "exponential",
      baseMs: 100,
      maxMs: 1_000,
      jitter: true,
    },
    timeoutMs: 500,
    concurrencyKey: "api.example",
    concurrencyLimit: 2,
    compensate: async (output) => {
      expectType<"ok">(output);
    },
  });
  expectType<"ok">(text);

  const finished = ctx.complete(input.enabled ? 1 : 0);
  expectType<WorkflowCompletion<number>>(finished);
  return finished;
});
expectType<Promise<number>>(run(mixedSteps, { enabled: true }));

const loopWorkflow = workflow<{ seed: number }>()(async (ctx, input) => {
  const final = await ctx.loop(
    { count: input.seed },
    async (state, { step, done }) => {
      expectType<number>(state.count);
      const next = await step(() => Promise.resolve(state.count + 1));
      if (next >= 3) return done({ total: next, exact: true as const });
      return { count: next };
    },
    { snapshotEvery: 10, maxIterations: 100 },
  );
  expectType<{ total: number; exact: true }>(final);
  return final;
});
expectType<Promise<{ total: number; exact: true }>>(run(loopWorkflow, { seed: 0 }));

const numberWorkflow = workflow<number>()(async (_ctx, value) => value * 2);
const stringWorkflow = workflow<string>()(async (_ctx, value) => value.toUpperCase());

const topology = workflow<{}>()(async (ctx) => {
  const branches = await ctx.parallel([
    () => ctx.step(() => Promise.resolve(1)),
    () => ctx.step(() => Promise.resolve(2)),
  ], { concurrency: 2 });
  expectType<Result<number>[]>(branches);

  const first = await ctx.spawn(numberWorkflow, 1);
  expectType<RunHandle<number>>(first);

  const homogeneous = await ctx.spawnAll([
    [numberWorkflow, 1],
    [numberWorkflow, 2],
  ] as const);
  const homogeneousResults = await ctx.joinAll(homogeneous);
  expectType<Result<number>[]>(homogeneousResults);

  const heterogeneous = await ctx.spawnAll([
    [numberWorkflow, 1],
    [stringWorkflow, "x"],
  ] as const);
  const heterogeneousResults = await ctx.joinAll(heterogeneous);
  expectType<readonly [Result<number>, Result<string>]>(heterogeneousResults);

  // @ts-expect-error each child input must match its workflow
  await ctx.spawnAll([[numberWorkflow, "not-a-number"]] as const);

  const okOnly = heterogeneousResults.filter(isOk);
  expectType<Array<{ ok: true; value: number | string }>>(okOnly);
  const [oks, errors] = partition(heterogeneousResults);
  expectType<Array<number | string>>(oks);
  expectType<unknown[]>(errors);

  return { branches, first };
});
void topology;

const control = workflow<{}>()(async (ctx) => {
  await ctx.sleep(1_000);
  await ctx.sleepUntil(new Date("2030-01-01T00:00:00Z"));
  const payload = await ctx.waitFor("approval", UserSchema, { timeoutMs: 5_000 });
  expectType<User>(payload);
  expectType<number>(ctx.now());
  expectType<number>(ctx.random());
  ctx.log("approved", payload.id);
  ctx.annotate({ phase: "approved", count: 1 });
  ctx.charge({ usd: 0.2, tokens: 100, units: 1 });
  expectType<number>(ctx.budget.remaining("usd"));
  expectType<number>(ctx.budget.spent("tokens"));
  return payload;
});
void control;

const selectorByPath: Selector = [1, 0, 2];
const selectorByLabel: Selector = "publish";
const selectorByNth: Selector = { label: "publish", nth: 2 };
void [selectorByPath, selectorByLabel, selectorByNth];

const te = testEngine();
const testResult = await te.run(typed, { id: "abc" }, {
  crashAfter: { label: "effect", nth: 1 },
  budget: { usd: 1 },
  advisories: "silent",
});
expectType<string>(testResult.runId);
expectType<"completed" | "failed" | "crashed" | "waiting" | "sleeping">(testResult.status);
expectType<{ id: string; length: number } | undefined>(testResult.result);
expectType<Advisory[]>(testResult.advisories);
await te.resume(testResult.runId);
await te.signal(testResult.runId, "approval", { by: "operator" });
te.clock.advance(1_000);

const openai = resource("openai", {
  rateLimit: { max: 60, perMs: 60_000 },
  breaker: { failureRate: 0.5, windowMs: 30_000, cooldownMs: 10_000 },
  concurrency: 8,
});

const memory = new MemoryStorage();
const files = new FileStorage("/tmp/durably-files");
const sqlite = new SqliteStorage("/tmp/durably.sqlite");
expectType<StorageAdapter>(memory);
expectType<StorageAdapter>(files);
expectType<StorageAdapter>(sqlite);

const engine = createEngine({
  storage: memory,
  resources: [openai],
  concurrency: 8,
  budget: { usd: 100 },
  hooks: {
    onRunFailed: ({ runId, error }) => {
      expectType<string>(runId);
      expectType<unknown>(error);
    },
  },
});
await engine.start();
const queued = await engine.enqueue(typed, { id: "queued" }, { priority: 2, delayMs: 10 });
expectType<RunHandle<{ id: string; length: number }>>(queued);
await engine.signal(queued.runId, "approval", { by: "operator" });
await engine.cancel(queued.runId);
await engine.pause(queued.runId);
await engine.resume(queued.runId);
await engine.retry(queued.runId, { fromStep: { label: "effect", nth: 2 } });
await engine.restart(queued.runId);
await engine.adopt(queued.runId);
const inspected = await engine.inspect(queued.runId);
if (inspected) expectType<string[]>(inspected.children);
await engine.list({ status: "failed", workflow: "schema-workflow", limit: 10 });
await engine.drain();
await engine.stop();

await durably.enqueue(typed, { id: "default" });
await durably.list({ status: "pending" });

expectType<Error>(ValidationError.prototype);
expectType<Error>(DurablyError.prototype);
expectType<Error>(SerializationError.prototype);
expectType<Error>(StepTimeoutError.prototype);
expectType<Error>(PurityError.prototype);
expectType<Error>(StaleRunError.prototype);
expectType<Error>(BudgetExceededError.prototype);
expectType<Error>(CircuitOpenError.prototype);
expectType<Error>(KeyConflictError.prototype);
expectType<Error>(RunCancelledError.prototype);
expectType<Error>(LeaseLostError.prototype);

// @ts-expect-error step retry backoff is a closed vocabulary
workflow(async (ctx) => ctx.step(() => Promise.resolve(1), { retry: { attempts: 2, backoff: "random" } }));
