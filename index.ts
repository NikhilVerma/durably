/**
 * durably's public TypeScript contract.
 *
 * The interfaces stay colocated with the runtime exports so source consumers,
 * emitted declarations, ESM, and CommonJS all expose one canonical surface.
 */

export type MaybePromise<T> = T | PromiseLike<T>;
export type JsonPrimitive = string | number | boolean | null;
export type JsonValue =
  | JsonPrimitive
  | Date
  | { readonly [key: string]: JsonValue }
  | readonly JsonValue[];

export interface StandardSchemaV1<Input = unknown, Output = Input> {
  readonly "~standard": {
    readonly version: 1;
    readonly vendor: string;
    readonly validate: (
      value: unknown,
    ) => MaybePromise<
      | { readonly value: Output; readonly issues?: undefined }
      | {
          readonly issues: readonly {
            readonly message: string;
            readonly path?: readonly unknown[];
          }[];
        }
    >;
    readonly types?: {
      readonly input: Input;
      readonly output: Output;
    };
  };
}

type SchemaInput<Schema extends StandardSchemaV1> =
  NonNullable<Schema["~standard"]["types"]>["input"];
type SchemaOutput<Schema extends StandardSchemaV1> =
  NonNullable<Schema["~standard"]["types"]>["output"];
type SchemaOutputOr<Schema extends StandardSchemaV1, Fallback> =
  unknown extends SchemaOutput<Schema> ? Fallback : SchemaOutput<Schema>;

export type AdvisoryCode =
  | "LOOP_SUGGESTED"
  | "FANOUT_CEILING"
  | "RETRY_STORM"
  | "STASH_SUGGESTED"
  | "BUDGET_NEAR"
  | "WAITFOR_NO_TIMEOUT"
  | "SNAPSHOT_HEAVY"
  | "SLOW_STEP_NO_TIMEOUT";

export interface Advisory {
  readonly code: AdvisoryCode;
  readonly level: "info" | "warn";
  readonly msg: string;
  readonly hint: string;
  readonly docs: string;
  readonly count: number;
  readonly firstAt: string;
}

export type AdvisoryMode = "silent";
export type BudgetKind = "usd" | "tokens" | "units";
export type Budget = Partial<Record<BudgetKind, number>>;

export type Selector =
  | readonly number[]
  | string
  | { readonly label: string; readonly nth: number };

export type Ok<T> = { readonly ok: true; readonly value: T };
export type Err<E = unknown> = { readonly ok: false; readonly error: E };
export type Result<T, E = unknown> = Ok<T> | Err<E>;

export interface RetryPolicy {
  readonly attempts: number;
  readonly backoff: "exponential" | "linear" | "none";
  readonly baseMs?: number;
  readonly maxMs?: number;
  readonly jitter?: boolean;
}

export interface StepAttemptContext {
  readonly signal: AbortSignal;
  readonly stashed: unknown;
  readonly stash: (value: unknown) => Promise<void>;
}

export interface StepOptions<Output> {
  readonly name?: string;
  readonly retry?: RetryPolicy;
  readonly timeoutMs?: number;
  readonly compensate?: (output: Output) => MaybePromise<void>;
  readonly concurrencyKey?: string;
  readonly concurrencyLimit?: number;
  readonly uses?: string;
}

export interface SchemaStepOptions<
  Schema extends StandardSchemaV1,
> extends StepOptions<SchemaOutput<Schema>> {
  readonly schema: Schema;
}

declare const loopCompletion: unique symbol;
export type LoopCompletion<Value> = {
  readonly [loopCompletion]: Value;
};

declare const workflowCompletion: unique symbol;
export type WorkflowCompletion<Value> = {
  readonly [workflowCompletion]: Value;
};

export interface LoopIterationContext {
  step<Schema extends StandardSchemaV1, Output>(
    fn: (context: StepAttemptContext) => MaybePromise<Output>,
    options: SchemaStepOptions<Schema>,
  ): Promise<SchemaOutputOr<Schema, Awaited<Output>>>;
  step<Output>(
    fn: (context: StepAttemptContext) => MaybePromise<Output>,
    options?: StepOptions<Awaited<Output>>,
  ): Promise<Awaited<Output>>;
  done<Value>(value: Value): LoopCompletion<Value>;
}

export interface BudgetView {
  remaining(kind: BudgetKind): number;
  spent(kind: BudgetKind): number;
}

declare const runHandleOutput: unique symbol;
export interface RunHandle<Output = unknown> {
  readonly runId: string;
  readonly [runHandleOutput]: Output;
}

declare const workflowContract: unique symbol;
export interface Workflow<Input = void, Output = unknown> {
  readonly name?: string;
  readonly [workflowContract]: {
    readonly input: (value: Input) => Input;
    readonly output: () => Output;
  };
}

type HandleOutput<Value> =
  Value extends RunHandle<infer Output> ? Output : never;
type LoopOutput<Value> =
  Value extends LoopCompletion<infer Output> ? Output : never;
type WorkflowResult<Value> =
  Value extends WorkflowCompletion<infer Output> ? Output : Value;

type ChildDefinition = readonly [Workflow<any, any>, any];
type ChildHandle<Definition> =
  Definition extends readonly [Workflow<any, infer Output>, any]
    ? RunHandle<Output>
    : never;
type ValidChildDefinitions<Definitions extends readonly ChildDefinition[]> = {
  readonly [Key in keyof Definitions]:
    Definitions[Key] extends readonly [
      Workflow<infer Input, any>,
      infer SuppliedInput,
    ]
      ? SuppliedInput extends Input
        ? Definitions[Key]
        : never
      : never;
};

export interface ParallelOptions {
  readonly concurrency?: number;
}

export interface SpawnOptions {
  readonly detached?: boolean;
}

export interface LoopOptions {
  readonly snapshotEvery?: number;
  readonly maxIterations?: number;
}

export interface WaitOptions {
  readonly timeoutMs?: number;
}

export interface WorkflowContext {
  readonly runId: string;
  readonly attempt: number;
  readonly budget: BudgetView;

  step<Schema extends StandardSchemaV1, Output>(
    fn: (context: StepAttemptContext) => MaybePromise<Output>,
    options: SchemaStepOptions<Schema>,
  ): Promise<SchemaOutputOr<Schema, Awaited<Output>>>;
  step<Output>(
    fn: (context: StepAttemptContext) => MaybePromise<Output>,
    options?: StepOptions<Awaited<Output>>,
  ): Promise<Awaited<Output>>;

  parallel<const Thunks extends readonly (() => MaybePromise<unknown>)[]>(
    thunks: Thunks,
    options?: ParallelOptions,
  ): Promise<Result<Awaited<ReturnType<Thunks[number]>>>[]>;

  spawn<Input, Output>(
    workflow: Workflow<Input, Output>,
    input: Input,
    options?: SpawnOptions,
  ): Promise<RunHandle<Output>>;

  spawnAll<const Definitions extends readonly ChildDefinition[]>(
    definitions: Definitions & ValidChildDefinitions<Definitions>,
    options?: SpawnOptions,
  ): Promise<{ -readonly [Key in keyof Definitions]: ChildHandle<Definitions[Key]> }>;

  joinAll<const Handles extends readonly RunHandle<any>[]>(
    handles: Handles,
  ): Promise<{ [Key in keyof Handles]: Result<HandleOutput<Handles[Key]>> }>;

  loop<State, ReducerOutput extends State | LoopCompletion<unknown>>(
    initialState: State,
    reducer: (
      state: State,
      context: LoopIterationContext,
    ) => MaybePromise<ReducerOutput>,
    options?: LoopOptions,
  ): Promise<LoopOutput<ReducerOutput>>;

  waitFor<Schema extends StandardSchemaV1>(
    name: string,
    schema: Schema,
    options?: WaitOptions,
  ): Promise<SchemaOutputOr<Schema, any>>;
  waitFor(
    name: string,
    schema: undefined,
    options?: WaitOptions,
  ): Promise<unknown>;
  waitFor(name: string, options?: WaitOptions): Promise<unknown>;

  sleep(ms: number): Promise<void>;
  sleepUntil(date: Date): Promise<void>;
  now(): number;
  random(): number;
  log(...args: readonly unknown[]): void;
  annotate(values: Readonly<Record<string, JsonValue>>): void;
  charge(amount: Budget): void;
  complete<Value>(value: Value): WorkflowCompletion<Value>;
}

export type WorkflowFunction<Input, Output> = (
  context: WorkflowContext,
  input: Input,
) => MaybePromise<Output>;

export interface WorkflowSchemaOptions<
  InputSchema extends StandardSchemaV1,
  OutputSchema extends StandardSchemaV1 | undefined = undefined,
> {
  readonly name?: string;
  readonly input: InputSchema;
  readonly output?: OutputSchema;
}

/**
 * Supplying only an input type is curried so TypeScript can still infer Output.
 */
export interface NamedWorkflowOptions {
  readonly name?: string;
}

export interface WorkflowFactory {
  <Input>(): <Output>(
    definition: WorkflowFunction<Input, Output>,
  ) => Workflow<Input, WorkflowResult<Awaited<Output>>>;
  <Input, Output>(
    definition: WorkflowFunction<Input, Output>,
  ): Workflow<Input, WorkflowResult<Awaited<Output>>>;
  <
    InputSchema extends StandardSchemaV1,
    OutputSchema extends StandardSchemaV1,
    Output extends SchemaOutput<OutputSchema>,
  >(
    options: WorkflowSchemaOptions<InputSchema, OutputSchema> & {
      readonly output: OutputSchema;
    },
    definition: WorkflowFunction<SchemaOutput<InputSchema>, Output>,
  ): Workflow<SchemaInput<InputSchema>, SchemaOutput<OutputSchema>>;
  <InputSchema extends StandardSchemaV1, Output>(
    options: WorkflowSchemaOptions<InputSchema>,
    definition: WorkflowFunction<SchemaOutput<InputSchema>, Output>,
  ): Workflow<SchemaInput<InputSchema>, WorkflowResult<Awaited<Output>>>;
  <Input, Output>(
    options: NamedWorkflowOptions,
    definition: WorkflowFunction<Input, Output>,
  ): Workflow<Input, WorkflowResult<Awaited<Output>>>;
}

export type StepEventStatus = "running" | "replayed" | "ok" | "retrying" | "failed";

export interface StepEvent {
  readonly runId: string;
  readonly path: readonly number[];
  readonly label: string;
  readonly status: StepEventStatus;
  readonly attempt: number;
  readonly executions: number;
  readonly ms?: number;
  readonly error?: unknown;
}

export interface StepObserverOptions {
  readonly onStep?: (event: StepEvent) => void;
}

export interface RunOptions extends StepObserverOptions {
  readonly key?: string;
  readonly fresh?: boolean;
  readonly budget?: Budget;
  readonly dir?: string;
  readonly advisories?: AdvisoryMode;
  readonly onAdvisory?: (advisory: Advisory) => void;
}

export interface RunFunction {
  <Input, Output>(
    workflow: Workflow<Input, Output>,
    input: Input,
    options?: RunOptions,
  ): Promise<Output>;
  <Output>(
    workflow: Workflow<void, Output>,
    options?: RunOptions,
  ): Promise<Output>;
}

export type RunStatus =
  | "pending"
  | "running"
  | "waiting"
  | "sleeping"
  | "paused"
  | "completed"
  | "failed"
  | "cancelled"
  | "stale";

export interface RunState {
  readonly runId: string;
  readonly status: RunStatus;
  readonly workflow: string;
  readonly key?: string;
  readonly children: string[];
  readonly advisories: Advisory[];
  readonly annotations?: Readonly<Record<string, JsonValue>>;
  readonly result?: unknown;
  readonly error?: unknown;
}

export interface StoredRun {
  readonly runId: string;
  readonly state: RunState;
  readonly events?: readonly unknown[];
}

export interface RunClaim {
  readonly owner: string;
  readonly expiresAt: string;
}

export interface StorageListOptions {
  readonly status?: RunStatus;
  readonly workflow?: string;
  readonly limit?: number;
}

export interface StorageAdapter {
  init(): MaybePromise<void>;
  createRun(run: StoredRun): MaybePromise<void>;
  append(runId: string, event: unknown): MaybePromise<void>;
  read(runId: string): MaybePromise<unknown[] | null>;
  claim(runId: string, claim: RunClaim): MaybePromise<boolean>;
  heartbeat(runId: string, claim: RunClaim): MaybePromise<boolean>;
  list(options?: StorageListOptions): MaybePromise<StoredRun[]>;
}

export interface RateLimitPolicy {
  readonly max: number;
  readonly perMs: number;
}

export interface BreakerPolicy {
  readonly failureRate: number;
  readonly windowMs: number;
  readonly cooldownMs: number;
}

export interface ResourceOptions {
  readonly rateLimit?: RateLimitPolicy;
  readonly breaker?: BreakerPolicy;
  readonly concurrency?: number;
}

declare const resourceContract: unique symbol;
export interface Resource {
  readonly name: string;
  readonly options: ResourceOptions;
  readonly [resourceContract]: true;
}

export interface EngineHooks {
  readonly onRunFailed?: (event: {
    readonly runId: string;
    readonly error: unknown;
  }) => MaybePromise<void>;
}

export interface EngineOptions {
  readonly storage?: StorageAdapter;
  readonly resources?: readonly Resource[];
  readonly concurrency?: number;
  readonly budget?: Budget;
  readonly hooks?: EngineHooks;
  readonly leaseMs?: number;
  readonly heartbeatMs?: number;
}

export interface EnqueueOptions {
  readonly key?: string;
  readonly priority?: number;
  readonly delayMs?: number;
  readonly budget?: Budget;
}

export interface ListOptions {
  readonly status?: RunStatus;
  readonly workflow?: string;
  readonly key?: string;
  readonly limit?: number;
}

export interface RetryRunOptions {
  readonly fromStep?: Selector;
}

export interface Engine {
  start(): Promise<void>;
  enqueue<Input, Output>(
    workflow: Workflow<Input, Output>,
    input: Input,
    options?: EnqueueOptions,
  ): Promise<RunHandle<Output>>;
  inspect(runId: string): Promise<RunState | null>;
  list(options?: ListOptions): Promise<RunState[]>;
  retry(runId: string, options?: RetryRunOptions): Promise<void>;
  restart(runId: string): Promise<RunHandle<unknown>>;
  adopt(runId: string): Promise<void>;
  signal(runId: string, name: string, payload: unknown): Promise<void>;
  cancel(runId: string): Promise<void>;
  pause(runId: string): Promise<void>;
  resume(runId: string): Promise<void>;
  drain(): Promise<void>;
  stop(): Promise<void>;
}

export interface TestStepState {
  readonly path: readonly number[];
  readonly label: string;
  readonly status: string;
  readonly executions: number;
  readonly attempts: number;
  readonly output?: unknown;
  readonly error?: unknown;
  readonly stashed?: unknown;
  readonly attemptHistory?: readonly {
    readonly attempt: number;
    readonly startedAt: string;
    readonly ms: number;
    readonly stashed?: boolean;
  }[];
  readonly startedAt?: string;
  readonly ms?: number;
}

export type TestRunStatus =
  | "completed"
  | "failed"
  | "crashed"
  | "waiting"
  | "sleeping";

export interface TestRunResult<Output = unknown> {
  readonly runId: string;
  readonly status: TestRunStatus;
  readonly result: Output | undefined;
  readonly steps: TestStepState[];
  readonly advisories: Advisory[];
  readonly error?: unknown;
  readonly compensationError?: unknown;
  readonly children: RunHandle<unknown>[];
}

export interface TestRunOptions extends StepObserverOptions {
  readonly crashAfter?: Selector;
  readonly crashInStep?: Selector;
  readonly budget?: Budget;
  readonly advisories?: AdvisoryMode;
}

export interface TestClock {
  advance(ms: number): void;
  now(): number;
}

export interface TestEngine {
  readonly clock: TestClock;
  run<Input, Output>(
    workflow: Workflow<Input, Output>,
    input: Input,
    options?: TestRunOptions,
  ): Promise<TestRunResult<Output>>;
  resume(runId: string, options?: StepObserverOptions): Promise<TestRunResult<unknown>>;
  signal(runId: string, name: string, payload: unknown): Promise<void>;
}

export {
  BudgetExceededError,
  CircuitOpenError,
  DurablyError,
  FileStorage,
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
  durably,
  isOk,
  partition,
  resource,
  run,
  testEngine,
  workflow,
} from "./src/runtime.ts";
