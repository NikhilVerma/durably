import { AsyncLocalStorage } from "node:async_hooks";
import { createHash, randomUUID } from "node:crypto";
import {
  appendFile,
  mkdir,
  open,
  readFile,
  readdir,
  rename,
  stat,
  truncate,
  unlink,
  writeFile,
} from "node:fs/promises";
import {
  appendFileSync,
  closeSync,
  mkdirSync,
  openSync,
  readFileSync,
  readdirSync,
  renameSync,
  unlinkSync,
  writeFileSync,
} from "node:fs";
import { dirname, join } from "node:path";
import type {
  Advisory,
  Budget,
  BudgetKind,
  Engine,
  EngineOptions,
  EnqueueOptions,
  Err,
  JsonValue,
  ListOptions,
  MaybePromise,
  Ok,
  Resource,
  ResourceOptions,
  Result,
  RetryRunOptions,
  RunClaim,
  RunFunction,
  RunHandle,
  RunOptions,
  RunState,
  RunStatus,
  Selector,
  StandardSchemaV1,
  StorageAdapter,
  StorageListOptions,
  StoredRun,
  TestEngine,
  TestRunOptions,
  TestRunResult,
  TestStepState,
  Workflow,
  WorkflowContext,
  WorkflowFactory,
  WorkflowFunction,
} from "../index.ts";
import { AdvisoryAccumulator } from "./advisories.ts";
import { advisoryRegistry } from "./advisories/registry.ts";
import { errorRegistry } from "./errors/registry.ts";
import { computeBackoffMs } from "./policies/backoff.ts";

type ErrorDetails = Record<string, unknown>;

export class DurablyError extends Error {
  hint: string;
  docs: string;
  runId: string | undefined;
  advisories: Advisory[];

  constructor(message = "durably operation failed", details: ErrorDetails = {}) {
    super(message);
    this.name = new.target.name;
    const definition = errorRegistry[this.name as keyof typeof errorRegistry];
    this.hint = definition?.hint ?? "Inspect the run state and correct the reported failure.";
    this.docs = definition?.docs
      ?? "https://www.npmjs.com/package/@nikhilverma/durably#docs-errors";
    this.runId = typeof details.runId === "string" ? details.runId : undefined;
    this.advisories = Array.isArray(details.advisories)
      ? details.advisories as Advisory[]
      : [];
    Object.assign(this, details);
  }
}

export class ValidationError extends DurablyError {
  issues: readonly unknown[];
  constructor(message = "validation failed", details: ErrorDetails = {}) {
    super(message, details);
    this.issues = Array.isArray(details.issues) ? details.issues : [];
  }
}

export class SerializationError extends DurablyError {
  path: string;
  constructor(message = "value is not serializable", details: ErrorDetails = {}) {
    super(message, details);
    this.path = typeof details.path === "string" ? details.path : "$";
  }
}

export class StepTimeoutError extends DurablyError {
  timeoutMs: number | undefined;
  constructor(message = "step timed out", details: ErrorDetails = {}) {
    super(message, details);
    this.timeoutMs = typeof details.timeoutMs === "number" ? details.timeoutMs : undefined;
  }
}

export class PurityError extends DurablyError {
  path: readonly number[];
  expected: unknown;
  actual: unknown;
  constructor(message = "workflow replay diverged", details: ErrorDetails = {}) {
    super(message, details);
    this.path = Array.isArray(details.path) ? details.path as number[] : [];
    this.expected = details.expected;
    this.actual = details.actual;
  }
}

export class StaleRunError extends DurablyError {}
export class BudgetExceededError extends DurablyError {
  kind: string | undefined;
  constructor(message = "budget exceeded", details: ErrorDetails = {}) {
    super(message, details);
    this.kind = typeof details.kind === "string" ? details.kind : undefined;
  }
}
export class CircuitOpenError extends DurablyError {
  resource: string | undefined;
  constructor(message = "resource circuit is open", details: ErrorDetails = {}) {
    super(message, details);
    this.resource = typeof details.resource === "string" ? details.resource : undefined;
  }
}
export class KeyConflictError extends DurablyError {}
export class RunCancelledError extends DurablyError {}
export class LeaseLostError extends DurablyError {}

const errorClasses: Record<string, new (message?: string, details?: ErrorDetails) => Error> = {
  DurablyError,
  ValidationError,
  SerializationError,
  StepTimeoutError,
  PurityError,
  StaleRunError,
  BudgetExceededError,
  CircuitOpenError,
  KeyConflictError,
  RunCancelledError,
  LeaseLostError,
};

const pathFor = (base: string, key: string | number): string =>
  typeof key === "number" ? `${base}[${key}]` : `${base}.${key}`;

const cloneSerializable = (
  value: unknown,
  path = "$",
  ancestors = new Set<object>(),
): unknown => {
  if (value === undefined && path === "$") return undefined;
  if (value === null || typeof value === "string" || typeof value === "boolean") return value;
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw new SerializationError(`non-finite number at ${path}`, { path });
    }
    return value;
  }
  if (value instanceof Date) {
    if (!Number.isFinite(value.getTime())) {
      throw new SerializationError(`invalid Date at ${path}`, { path });
    }
    return new Date(value.getTime());
  }
  if (
    typeof value === "undefined"
    || typeof value === "function"
    || typeof value === "symbol"
    || typeof value === "bigint"
  ) {
    throw new SerializationError(`unsupported ${typeof value} at ${path}`, { path });
  }
  if (typeof value !== "object") {
    throw new SerializationError(`unsupported value at ${path}`, { path });
  }
  if (ancestors.has(value)) {
    throw new SerializationError(`cyclic value at ${path}`, { path });
  }
  ancestors.add(value);
  try {
    if (Array.isArray(value)) {
      return value.map((item, index) => cloneSerializable(item, pathFor(path, index), ancestors));
    }
    if (Object.getPrototypeOf(value) !== Object.prototype) {
      throw new SerializationError(`class instance at ${path}`, { path });
    }
    const result: Record<string, unknown> = {};
    for (const [key, item] of Object.entries(value)) {
      result[key] = cloneSerializable(item, pathFor(path, key), ancestors);
    }
    return result;
  } finally {
    ancestors.delete(value);
  }
};

const encodeDisk = (value: unknown): unknown => {
  if (value instanceof Date) return { __durably: "Date", value: value.toISOString() };
  if (value instanceof Error) {
    const fields: Record<string, unknown> = {};
    for (const key of Object.keys(value)) fields[key] = encodeDisk((value as unknown as Record<string, unknown>)[key]);
    return {
      __durably: "Error",
      name: value.name,
      message: value.message,
      stack: value.stack,
      fields,
    };
  }
  if (Array.isArray(value)) return value.map(encodeDisk);
  if (value && typeof value === "object") {
    return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, encodeDisk(item)]));
  }
  return value;
};

const decodeDisk = (value: unknown): unknown => {
  if (Array.isArray(value)) return value.map(decodeDisk);
  if (value && typeof value === "object") {
    const record = value as Record<string, unknown>;
    if (record.__durably === "Date" && typeof record.value === "string") return new Date(record.value);
    if (record.__durably === "Error") {
      const ErrorClass = errorClasses[String(record.name)] ?? Error;
      const details = decodeDisk(record.fields) as ErrorDetails;
      const error = new ErrorClass(String(record.message ?? ""), details);
      if (typeof record.stack === "string") error.stack = record.stack;
      return error;
    }
    return Object.fromEntries(Object.entries(record).map(([key, item]) => [key, decodeDisk(item)]));
  }
  return value;
};

const stableValue = (value: unknown): unknown => {
  if (value instanceof Date) return { __date: value.toISOString() };
  if (Array.isArray(value)) return value.map(stableValue);
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>)
        .sort(([left], [right]) => left.localeCompare(right))
        .map(([key, item]) => [key, stableValue(item)]),
    );
  }
  return value;
};

const stableStringify = (value: unknown): string =>
  JSON.stringify(stableValue(value)) ?? "undefined";
const hash = (value: string): string => createHash("sha256").update(value).digest("hex");
const normalizeSource = (source: string): string => source.replace(/\s+/g, " ").trim();

const workflowInternal = Symbol("durably.workflow");
const workflowRegistry = new Map<string, WorkflowRuntime<any, any>>();
interface WorkflowRuntime<Input = unknown, Output = unknown> extends Workflow<Input, Output> {
  readonly [workflowInternal]: {
    readonly fn: WorkflowFunction<Input, unknown>;
    readonly bodyHash: string;
    readonly inputSchema?: StandardSchemaV1;
    readonly outputSchema?: StandardSchemaV1;
  };
}

const makeWorkflow = <Input, Output>(
  fn: WorkflowFunction<Input, Output>,
  options: {
    readonly name?: string;
    readonly inputSchema?: StandardSchemaV1;
    readonly outputSchema?: StandardSchemaV1;
  } = {},
): Workflow<Input, Output> => {
  const bodyHash = hash(normalizeSource(fn.toString()));
  const value = {
    name: options.name ?? (fn.name || `workflow-${bodyHash.slice(0, 10)}`),
    [workflowInternal]: {
      fn: fn as WorkflowFunction<Input, unknown>,
      bodyHash,
      inputSchema: options.inputSchema,
      outputSchema: options.outputSchema,
    },
  };
  workflowRegistry.set(bodyHash, value as unknown as WorkflowRuntime<any, any>);
  return value as unknown as Workflow<Input, Output>;
};

export const workflow: WorkflowFactory = (function (
  first?: unknown,
  second?: unknown,
): unknown {
  if (arguments.length === 0) {
    return (fn: WorkflowFunction<unknown, unknown>) => makeWorkflow(fn);
  }
  if (typeof first === "function") return makeWorkflow(first as WorkflowFunction<unknown, unknown>);
  if (first && typeof first === "object" && typeof second === "function") {
    const options = first as {
      name?: string;
      input?: StandardSchemaV1;
      output?: StandardSchemaV1;
    };
    return makeWorkflow(second as WorkflowFunction<unknown, unknown>, {
      ...(options.name !== undefined ? { name: options.name } : {}),
      ...(options.input !== undefined ? { inputSchema: options.input } : {}),
      ...(options.output !== undefined ? { outputSchema: options.output } : {}),
    });
  }
  throw new TypeError("workflow expects a function or options and a function");
}) as WorkflowFactory;

export const isOk = <Entry extends Result<unknown>>(
  result: Entry,
): result is Extract<Entry, Ok<unknown>> => result.ok;

export const partition = <const Results extends readonly Result<unknown>[]>(
  results: Results,
): [
  Array<Results[number] extends infer Entry ? Entry extends Ok<infer Value> ? Value : never : never>,
  Array<Results[number] extends infer Entry ? Entry extends Err<infer ErrorValue> ? ErrorValue : never : never>,
] => {
  const values: unknown[] = [];
  const errors: unknown[] = [];
  for (const result of results) {
    if (result.ok) values.push(result.value);
    else errors.push(result.error);
  }
  return [values, errors] as [
    Array<Results[number] extends infer Entry ? Entry extends Ok<infer Value> ? Value : never : never>,
    Array<Results[number] extends infer Entry ? Entry extends Err<infer ErrorValue> ? ErrorValue : never : never>,
  ];
};

const crc32 = (bytes: Uint8Array): number => {
  let crc = 0xffffffff;
  for (const byte of bytes) {
    crc ^= byte;
    for (let bit = 0; bit < 8; bit++) crc = (crc >>> 1) ^ (0xedb88320 & -(crc & 1));
  }
  return (crc ^ 0xffffffff) >>> 0;
};

const frame = (event: unknown): Buffer => {
  const payload = Buffer.from(`${JSON.stringify(encodeDisk(event))}\n`);
  const header = Buffer.allocUnsafe(8);
  header.writeUInt32LE(payload.length, 0);
  header.writeUInt32LE(crc32(payload), 4);
  return Buffer.concat([header, payload]);
};

const parseFrames = async (path: string, heal: boolean): Promise<unknown[]> => {
  let bytes: Buffer;
  try {
    bytes = await readFile(path);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return [];
    throw error;
  }
  const events: unknown[] = [];
  let offset = 0;
  while (offset < bytes.length) {
    const start = offset;
    if (offset + 8 > bytes.length) {
      if (heal) await truncate(path, start);
      break;
    }
    const length = bytes.readUInt32LE(offset);
    const expected = bytes.readUInt32LE(offset + 4);
    const end = offset + 8 + length;
    if (end > bytes.length) {
      if (heal) await truncate(path, start);
      break;
    }
    const payload = bytes.subarray(offset + 8, end);
    if (crc32(payload) !== expected) {
      if (heal) await truncate(path, start);
      break;
    }
    try {
      events.push(JSON.parse(payload.toString("utf8").trim()));
    } catch {
      if (heal) await truncate(path, start);
      break;
    }
    offset = end;
  }
  return events;
};

export class MemoryStorage implements StorageAdapter {
  readonly events = new Map<string, unknown[]>();
  readonly states = new Map<string, StoredRun>();
  readonly claims = new Map<string, RunClaim>();
  readonly resourceStates = new Map<string, unknown>();

  async init(): Promise<void> {}
  async createRun(run: StoredRun): Promise<void> {
    this.states.set(run.runId, run);
    this.events.set(run.runId, []);
  }
  async append(runId: string, event: unknown): Promise<void> {
    const events = this.events.get(runId) ?? [];
    events.push(encodeDisk(event));
    this.events.set(runId, events);
    const state = (event as { state?: RunState }).state;
    if (state) this.states.set(runId, { runId, state });
  }
  async read(runId: string): Promise<unknown[] | null> {
    const events = this.events.get(runId);
    return events ? [...events] : null;
  }
  async claim(runId: string, claim: RunClaim): Promise<boolean> {
    const existing = this.claims.get(runId);
    if (existing && existing.owner !== claim.owner && Date.parse(existing.expiresAt) > Date.now()) return false;
    this.claims.set(runId, claim);
    return true;
  }
  async heartbeat(runId: string, claim: RunClaim): Promise<boolean> {
    const existing = this.claims.get(runId);
    if (!existing || existing.owner !== claim.owner) return false;
    this.claims.set(runId, claim);
    return true;
  }
  async release(runId: string, owner: string): Promise<void> {
    if (this.claims.get(runId)?.owner === owner) this.claims.delete(runId);
  }
  async list(options: StorageListOptions = {}): Promise<StoredRun[]> {
    return [...this.states.values()].filter(({ state }) =>
      (!options.status || state.status === options.status)
      && (!options.workflow || state.workflow === options.workflow)
    ).slice(0, options.limit);
  }
  async loadResourceState(name: string): Promise<unknown> {
    return this.resourceStates.get(name);
  }
  async saveResourceState(name: string, state: unknown): Promise<void> {
    this.resourceStates.set(name, state);
  }
}

export class FileStorage implements StorageAdapter {
  readonly directory: string;
  constructor(directory: string) {
    this.directory = directory;
  }
  private runDirectory(runId: string): string {
    return join(this.directory, "runs", runId.slice(0, 2), runId);
  }
  private eventsPath(runId: string): string {
    return join(this.runDirectory(runId), "events.log");
  }
  private statePath(runId: string): string {
    return join(this.runDirectory(runId), "state.json");
  }
  initSync(): void {
    mkdirSync(join(this.directory, "runs"), { recursive: true });
  }
  private runIdsSync(): string[] {
    try {
      return readdirSync(join(this.directory, "runs"))
        .flatMap((shard) => {
          try {
            return readdirSync(join(this.directory, "runs", shard));
          } catch {
            return [];
          }
        });
    } catch {
      return [];
    }
  }
  listSync(): StoredRun[] {
    const runs: StoredRun[] = [];
    for (const runId of this.runIdsSync()) {
      try {
        const state = decodeDisk(JSON.parse(readFileSync(this.statePath(runId), "utf8"))) as RunState;
        runs.push({ runId, state });
      } catch {}
    }
    return runs;
  }
  readStateSync(runId: string): RunState | null {
    try {
      return decodeDisk(JSON.parse(readFileSync(this.statePath(runId), "utf8"))) as RunState;
    } catch {
      return null;
    }
  }
  createRunSync(run: StoredRun): void {
    mkdirSync(this.runDirectory(run.runId), { recursive: true });
    this.appendSync(run.runId, { type: "state", seq: 0, state: run.state });
  }
  appendSync(runId: string, event: unknown): void {
    mkdirSync(this.runDirectory(runId), { recursive: true });
    appendFileSync(this.eventsPath(runId), frame(event));
    const state = (event as { state?: RunState }).state;
    if (state) {
      const target = this.statePath(runId);
      const temporary = `${target}.tmp-${process.pid}-${Math.random().toString(16).slice(2)}`;
      writeFileSync(temporary, `${JSON.stringify(encodeDisk(state), null, 2)}\n`);
      renameSync(temporary, target);
    }
  }
  claimSync(runId: string, claim: RunClaim): boolean {
    const path = join(this.runDirectory(runId), "lease.json");
    mkdirSync(dirname(path), { recursive: true });
    try {
      const descriptor = openSync(path, "wx");
      writeFileSync(descriptor, JSON.stringify(claim));
      closeSync(descriptor);
      return true;
    } catch {}
    try {
      const existing = JSON.parse(readFileSync(path, "utf8")) as RunClaim;
      if (existing.owner === claim.owner) {
        writeFileSync(path, JSON.stringify(claim));
        return true;
      }
      if (Date.parse(existing.expiresAt) > Date.now()) return false;
      unlinkSync(path);
      return this.claimSync(runId, claim);
    } catch {
      return false;
    }
  }
  releaseSync(runId: string, owner: string): void {
    const path = join(this.runDirectory(runId), "lease.json");
    try {
      const existing = JSON.parse(readFileSync(path, "utf8")) as RunClaim;
      if (existing.owner === owner) unlinkSync(path);
    } catch {}
  }
  async init(): Promise<void> {
    await mkdir(join(this.directory, "runs"), { recursive: true });
    for (const runId of await this.runIds()) await parseFrames(this.eventsPath(runId), true);
  }
  private async runIds(): Promise<string[]> {
    const root = join(this.directory, "runs");
    const ids: string[] = [];
    let shards: string[];
    try {
      shards = await readdir(root);
    } catch {
      return ids;
    }
    for (const shard of shards) {
      let leaves: string[] = [];
      try {
        leaves = await readdir(join(root, shard));
      } catch {}
      ids.push(...leaves);
    }
    return ids;
  }
  async createRun(run: StoredRun): Promise<void> {
    await mkdir(this.runDirectory(run.runId), { recursive: true });
    await this.append(run.runId, { type: "state", seq: 0, state: run.state });
  }
  async append(runId: string, event: unknown): Promise<void> {
    await mkdir(this.runDirectory(runId), { recursive: true });
    await appendFile(this.eventsPath(runId), frame(event));
    const state = (event as { state?: RunState }).state;
    if (state) {
      const target = this.statePath(runId);
      const temporary = `${target}.tmp-${process.pid}-${Math.random().toString(16).slice(2)}`;
      await writeFile(temporary, `${JSON.stringify(encodeDisk(state), null, 2)}\n`);
      await rename(temporary, target);
    }
  }
  async read(runId: string): Promise<unknown[] | null> {
    const events = await parseFrames(this.eventsPath(runId), true);
    return events.length > 0 ? events : null;
  }
  async claim(runId: string, claim: RunClaim): Promise<boolean> {
    const path = join(this.runDirectory(runId), "lease.json");
    await mkdir(dirname(path), { recursive: true });
    try {
      const handle = await open(path, "wx");
      await handle.writeFile(JSON.stringify(claim));
      await handle.close();
      return true;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
    }
    try {
      const existing = JSON.parse(await readFile(path, "utf8")) as RunClaim;
      if (existing.owner === claim.owner) {
        await writeFile(path, JSON.stringify(claim));
        return true;
      }
      if (Date.parse(existing.expiresAt) > Date.now()) return false;
      await unlink(path);
      return this.claim(runId, claim);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") return this.claim(runId, claim);
      return false;
    }
  }
  async heartbeat(runId: string, claim: RunClaim): Promise<boolean> {
    const path = join(this.runDirectory(runId), "lease.json");
    try {
      const existing = JSON.parse(await readFile(path, "utf8")) as RunClaim;
      if (existing.owner !== claim.owner) return false;
      await writeFile(path, JSON.stringify(claim));
      return true;
    } catch {
      return false;
    }
  }
  async release(runId: string, owner: string): Promise<void> {
    const path = join(this.runDirectory(runId), "lease.json");
    try {
      const existing = JSON.parse(await readFile(path, "utf8")) as RunClaim;
      if (existing.owner === owner) await unlink(path);
    } catch {}
  }
  async list(options: StorageListOptions = {}): Promise<StoredRun[]> {
    const runs: StoredRun[] = [];
    for (const runId of await this.runIds()) {
      try {
        const state = decodeDisk(JSON.parse(await readFile(this.statePath(runId), "utf8"))) as RunState;
        if (options.status && state.status !== options.status) continue;
        if (options.workflow && state.workflow !== options.workflow) continue;
        runs.push({ runId, state });
      } catch {}
    }
    runs.sort((left, right) => left.runId.localeCompare(right.runId));
    return runs.slice(0, options.limit);
  }
  async loadResourceState(name: string): Promise<unknown> {
    try {
      return JSON.parse(await readFile(join(this.directory, `resource-${hash(name).slice(0, 12)}.json`), "utf8"));
    } catch {
      return undefined;
    }
  }
  async saveResourceState(name: string, state: unknown): Promise<void> {
    const path = join(this.directory, `resource-${hash(name).slice(0, 12)}.json`);
    await writeFile(path, JSON.stringify(encodeDisk(state)));
  }
}

interface Clock {
  now(): number;
  sleep(ms: number): Promise<void>;
}

class RealClock implements Clock {
  now(): number {
    return Date.now();
  }
  sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, Math.max(0, ms)));
  }
}

class FakeClock implements Clock {
  #now = Date.now();
  #timers: Array<{ at: number; resolve: () => void }> = [];
  now(): number {
    return this.#now;
  }
  sleep(ms: number): Promise<void> {
    if (ms <= 0) return Promise.resolve();
    return new Promise((resolve) => {
      this.#timers.push({ at: this.#now + ms, resolve });
      this.#timers.sort((left, right) => left.at - right.at);
    });
  }
  advance(ms: number): void {
    this.#now += ms;
    const ready = this.#timers.filter((timer) => timer.at <= this.#now);
    this.#timers = this.#timers.filter((timer) => timer.at > this.#now);
    for (const timer of ready) timer.resolve();
  }
}

type StepStatus = "running" | "ok" | "failed";
interface AttemptRecord {
  attempt: number;
  startedAt: string;
  ms: number;
  stashed: boolean;
}
interface InternalStep {
  path: number[];
  label: string;
  status: StepStatus;
  output?: unknown;
  error?: unknown;
  executions: number;
  attempts: number;
  startedAt?: string | undefined;
  ms?: number | undefined;
  stashed?: unknown;
  attemptHistory: AttemptRecord[];
  compensated?: boolean;
  compensationError?: unknown;
  compensate?: ((output: unknown) => MaybePromise<void>) | undefined;
}

interface InternalOp {
  kind: string;
  value?: unknown;
  wakeAt?: number;
  signal?: string;
  timeoutAt?: number;
  payload?: unknown;
}

interface InternalLoop {
  snapshotState: unknown;
  snapshotIteration: number;
  snapshots: number;
  done?: boolean;
  output?: unknown;
}

interface InternalRun {
  runId: string;
  status: RunStatus | "crashed";
  workflow: string;
  bodyHash: string;
  inputHash: string;
  input: unknown;
  key?: string | undefined;
  result?: unknown;
  error?: unknown;
  compensationError?: unknown;
  steps: InternalStep[];
  children: string[];
  detachedChildren: string[];
  annotations: Record<string, JsonValue>;
  advisories: Advisory[];
  waitingFor?: { signal: string; since: string; timeoutAt?: number } | undefined;
  wakeAt?: number | undefined;
  adoption?: { assertedCompatible: true; bodyHash: string } | undefined;
  priority: number;
  createdAt: number;
  dueAt: number;
  budgetLimit: Budget;
  budgetSpent: Budget;
  ops: Record<string, InternalOp>;
  loops: Record<string, InternalLoop>;
  seq: number;
  workflowAttempt: number;
  rollbackPending: boolean;
  rollbackIndex: number;
  workflowDef?: WorkflowRuntime<any, any> | undefined;
  accumulator?: AdvisoryAccumulator | undefined;
  injector?: FailureInjector | undefined;
  signalSchemas: Map<string, StandardSchemaV1 | undefined>;
  activeControllers: Set<AbortController>;
  cancelRequested: boolean;
  pauseRequested: boolean;
  execution?: Promise<void> | undefined;
  listeners: Set<() => void>;
  advisoryListeners: Set<(advisory: Advisory) => void>;
  stderrCodes: Set<string>;
  advisoriesSilent: boolean;
  fastStart: boolean;
}

interface PathFrame {
  prefix: number[];
  counter: number;
}

interface FailureInjector {
  crashAfter?: Selector | undefined;
  crashInStep?: Selector | undefined;
  used: boolean;
}

class ParkSignal extends Error {}
class CrashSignal extends Error {}
class CompleteSignal {
  constructor(readonly value: unknown) {}
}

const loopDone = Symbol("durably.loop.done");
interface RuntimeLoopDone {
  readonly [loopDone]: true;
  readonly value: unknown;
}

const pathKey = (path: readonly number[]): string => path.join(".");
const comparePaths = (left: readonly number[], right: readonly number[]): number => {
  for (let index = 0; index < Math.max(left.length, right.length); index++) {
    if (left[index] === undefined) return -1;
    if (right[index] === undefined) return 1;
    if (left[index] !== right[index]) return left[index]! - right[index]!;
  }
  return 0;
};

const autoLabel = (fn: Function): string => {
  const source = normalizeSource(fn.toString())
    .replace(/^(?:async\s*)?\(?[^=]*\)?\s*=>\s*/, "")
    .replace(/^function\s*[^(]*\([^)]*\)\s*\{?/, "");
  return source.slice(0, 80) || "step";
};

const asError = (error: unknown): Error =>
  error instanceof Error ? error : new Error(typeof error === "string" ? error : String(error));
const errorValue = (error: unknown): { name: string; message: string } => {
  const value = asError(error);
  return { name: value.name, message: value.message };
};

const schemaValidate = async (
  schema: StandardSchemaV1 | undefined,
  value: unknown,
): Promise<unknown> => {
  if (!schema) return value;
  const result = await schema["~standard"].validate(value);
  if ("issues" in result && result.issues) {
    throw new ValidationError(
      result.issues.map((issue) => issue.message).join("; ") || "validation failed",
      { issues: result.issues },
    );
  }
  return cloneSerializable(result.value);
};

const publicStep = (step: InternalStep): Record<string, unknown> => ({
  path: [...step.path],
  label: step.label,
  status: step.status,
  ...(step.output !== undefined ? { output: step.output } : {}),
  ...(step.error !== undefined ? { error: step.error } : {}),
  executions: step.executions,
  attempts: step.attempts,
  ...(step.startedAt ? { startedAt: step.startedAt } : {}),
  ...(step.ms !== undefined ? { ms: step.ms } : {}),
  ...(step.stashed !== undefined ? { stashed: step.stashed } : {}),
  attemptHistory: step.attemptHistory.map((attempt) => ({ ...attempt })),
});

const stateOf = (run: InternalRun): RunState & Record<string, unknown> => ({
  runId: run.runId,
  status: run.status === "crashed" ? "pending" : run.status,
  workflow: run.workflow,
  bodyHash: run.bodyHash,
  inputHash: run.inputHash,
  input: run.input,
  ...(run.key !== undefined ? { key: run.key } : {}),
  ...(run.result !== undefined ? { result: run.result } : {}),
  ...(run.error !== undefined ? { error: run.error } : { error: null }),
  ...(run.compensationError !== undefined ? { compensationError: run.compensationError } : {}),
  steps: [...run.steps].sort((left, right) => comparePaths(left.path, right.path)).map(publicStep),
  children: [...run.children],
  detachedChildren: [...run.detachedChildren],
  annotations: { ...run.annotations },
  advisories: run.advisories.map((item) => ({ ...item })),
  ...(run.waitingFor ? { waitingFor: { ...run.waitingFor } } : {}),
  ...(run.wakeAt !== undefined ? { wakeAt: run.wakeAt } : {}),
  ...(run.adoption ? { adoption: { ...run.adoption } } : {}),
  priority: run.priority,
  createdAt: run.createdAt,
  dueAt: run.dueAt,
  budget: Object.fromEntries(
    (["usd", "tokens", "units"] as BudgetKind[])
      .filter((kind) => run.budgetLimit[kind] !== undefined || run.budgetSpent[kind] !== undefined)
      .map((kind) => [kind, {
        spent: run.budgetSpent[kind] ?? 0,
        ...(run.budgetLimit[kind] !== undefined ? { limit: run.budgetLimit[kind] } : {}),
      }]),
  ),
  __internal: {
    ops: run.ops,
    loops: run.loops,
    seq: run.seq,
    workflowAttempt: run.workflowAttempt,
    rollbackPending: run.rollbackPending,
    rollbackIndex: run.rollbackIndex,
  },
});

const hydrateRun = (stateValue: RunState & Record<string, unknown>): InternalRun => {
  const state = decodeDisk(stateValue) as RunState & Record<string, unknown>;
  const internal = (state.__internal ?? {}) as Record<string, unknown>;
  const budgetRecord = (state.budget ?? {}) as Record<string, { spent?: number; limit?: number }>;
  const steps = Array.isArray(state.steps)
    ? (state.steps as Array<Record<string, unknown>>).map((step) => ({
        path: [...(step.path as number[])],
        label: String(step.label),
        status: step.status as StepStatus,
        output: step.output,
        error: step.error,
        executions: Number(step.executions ?? 0),
        attempts: Number(step.attempts ?? 0),
        startedAt: typeof step.startedAt === "string" ? step.startedAt : undefined,
        ms: typeof step.ms === "number" ? step.ms : undefined,
        stashed: step.stashed,
        attemptHistory: Array.isArray(step.attemptHistory)
          ? step.attemptHistory as unknown as AttemptRecord[]
          : [],
      }))
    : [];
  return {
    runId: state.runId,
    status: state.status,
    workflow: state.workflow,
    bodyHash: String(state.bodyHash ?? ""),
    inputHash: String(state.inputHash ?? ""),
    input: state.input,
    key: typeof state.key === "string" ? state.key : undefined,
    result: state.result,
    error: state.error === null ? undefined : state.error,
    compensationError: state.compensationError,
    steps,
    children: [...state.children],
    detachedChildren: Array.isArray(state.detachedChildren) ? [...state.detachedChildren as string[]] : [],
    annotations: (state.annotations ?? {}) as Record<string, JsonValue>,
    advisories: [...state.advisories],
    waitingFor: state.waitingFor as InternalRun["waitingFor"],
    wakeAt: typeof state.wakeAt === "number" ? state.wakeAt : undefined,
    adoption: state.adoption as InternalRun["adoption"],
    priority: Number(state.priority ?? 0),
    createdAt: Number(state.createdAt ?? Date.now()),
    dueAt: Number(state.dueAt ?? 0),
    budgetLimit: Object.fromEntries(Object.entries(budgetRecord).map(([kind, value]) => [kind, value.limit])),
    budgetSpent: Object.fromEntries(Object.entries(budgetRecord).map(([kind, value]) => [kind, value.spent ?? 0])),
    ops: (internal.ops ?? {}) as Record<string, InternalOp>,
    loops: (internal.loops ?? {}) as Record<string, InternalLoop>,
    seq: Number(internal.seq ?? 0),
    workflowAttempt: Number(internal.workflowAttempt ?? 0),
    rollbackPending: Boolean(internal.rollbackPending),
    rollbackIndex: Number(internal.rollbackIndex ?? -1),
    signalSchemas: new Map(),
    activeControllers: new Set(),
    cancelRequested: false,
    pauseRequested: false,
    listeners: new Set(),
    advisoryListeners: new Set(),
    stderrCodes: new Set(),
    advisoriesSilent: false,
    fastStart: false,
  };
};

class Semaphore {
  #active = 0;
  readonly #queue: Array<() => void> = [];
  constructor(readonly limit: number) {}
  tryAcquire(): (() => void) | undefined {
    if (this.#active >= this.limit) return undefined;
    this.#active++;
    return () => {
      this.#active--;
      this.#queue.shift()?.();
    };
  }
  async acquire(): Promise<() => void> {
    const immediate = this.tryAcquire();
    if (immediate) return immediate;
    await new Promise<void>((resolve) => this.#queue.push(resolve));
    this.#active++;
    return () => {
      this.#active--;
      this.#queue.shift()?.();
    };
  }
  run<Value>(task: () => MaybePromise<Value>): Promise<Value> {
    return new Promise<Value>((resolve, reject) => {
      const launch = () => {
        this.#active++;
        let result: PromiseLike<Value>;
        try {
          result = Promise.resolve(task());
        } catch (error) {
          this.#active--;
          this.#queue.shift()?.();
          reject(error);
          return;
        }
        result.then(
          (value) => {
            this.#active--;
            this.#queue.shift()?.();
            resolve(value);
          },
          (error) => {
            this.#active--;
            this.#queue.shift()?.();
            reject(error);
          },
        );
      };
      if (this.#active < this.limit) launch();
      else this.#queue.push(launch);
    });
  }
}

interface ResourceRuntime {
  readonly definition: Resource;
  readonly semaphore?: Semaphore | undefined;
  tokens: number[];
  failures: Array<{ at: number; failed: boolean }>;
  openUntil: number;
  probing: boolean;
}

const resourceInternal = Symbol("durably.resource");
interface RuntimeResource extends Resource {
  readonly [resourceInternal]: true;
}

export const resource = (name: string, options: ResourceOptions = {}): Resource =>
  ({ name, options, [resourceInternal]: true }) as RuntimeResource;

interface RuntimeStorage extends StorageAdapter {
  release?(runId: string, owner: string): MaybePromise<void>;
  loadResourceState?(name: string): MaybePromise<unknown>;
  saveResourceState?(name: string, state: unknown): MaybePromise<void>;
}

interface EngineRuntimeOptions extends EngineOptions {
  readonly clock?: Clock;
  readonly testMode?: boolean;
  readonly shadowReplay?: boolean;
}

class EngineImpl implements Engine {
  readonly storage: RuntimeStorage;
  readonly clock: Clock;
  readonly owner = `${process.pid}-${randomUUID()}`;
  readonly runs = new Map<string, InternalRun>();
  readonly pathStorage = new AsyncLocalStorage<PathFrame>();
  readonly keyedSemaphores = new Map<string, Semaphore>();
  readonly resourceRuntimes = new Map<string, ResourceRuntime>();
  readonly concurrency: number;
  readonly leaseMs: number;
  readonly heartbeatMs: number;
  readonly engineBudget: Budget;
  readonly engineSpent: Budget = {};
  readonly hooks: EngineOptions["hooks"];
  readonly testMode: boolean;
  readonly shadowReplay: boolean;
  #started = false;
  #initialized = false;
  #active = 0;
  #starting = false;
  #runCounter = 0;

  constructor(options: EngineRuntimeOptions = {}) {
    this.storage = (options.storage ?? new MemoryStorage()) as RuntimeStorage;
    this.clock = options.clock ?? new RealClock();
    this.concurrency = options.concurrency ?? 4;
    this.leaseMs = options.leaseMs ?? 2_000;
    this.heartbeatMs = options.heartbeatMs ?? Math.max(100, Math.floor(this.leaseMs / 3));
    this.engineBudget = { ...(options.budget ?? {}) };
    this.hooks = options.hooks;
    this.testMode = options.testMode ?? false;
    this.shadowReplay = options.shadowReplay ?? this.testMode;
    if (this.testMode) this.#started = true;
    for (const definition of options.resources ?? []) {
      this.resourceRuntimes.set(definition.name, {
        definition,
        semaphore: definition.options.concurrency
          ? new Semaphore(definition.options.concurrency)
          : undefined,
        tokens: [],
        failures: [],
        openUntil: 0,
        probing: false,
      });
    }
  }

  async start(): Promise<void> {
    if (this.#started || this.#starting) return;
    this.#starting = true;
    await this.initialize();
    this.#started = true;
    this.#starting = false;
    this.kick();
  }

  private async initialize(): Promise<void> {
    if (this.#initialized) return;
    await this.storage.init();
    await this.refreshRuns();
    for (const runtime of this.resourceRuntimes.values()) {
      const persisted = await this.storage.loadResourceState?.(runtime.definition.name);
      if (persisted && typeof persisted === "object") {
        const value = decodeDisk(persisted) as Partial<ResourceRuntime>;
        runtime.failures = Array.isArray(value.failures) ? value.failures : [];
        runtime.openUntil = typeof value.openUntil === "number" ? value.openUntil : 0;
      }
    }
    this.#initialized = true;
  }

  private async refreshRuns(): Promise<void> {
    const stored = await this.storage.list();
    for (const item of stored) {
      const incoming = hydrateRun(item.state as RunState & Record<string, unknown>);
      const current = this.runs.get(item.runId);
      if (!current) {
        incoming.workflowDef = workflowRegistry.get(incoming.bodyHash);
        this.runs.set(item.runId, incoming);
        continue;
      }
      if (incoming.seq > current.seq && !current.execution) {
        incoming.workflowDef = current.workflowDef ?? workflowRegistry.get(incoming.bodyHash);
        incoming.listeners = current.listeners;
        incoming.advisoryListeners = current.advisoryListeners;
        incoming.stderrCodes = current.stderrCodes;
        incoming.injector = current.injector;
        incoming.advisoriesSilent = current.advisoriesSilent;
        this.runs.set(item.runId, incoming);
      }
    }
  }

  private createAccumulator(run: InternalRun): AdvisoryAccumulator {
    const accumulator = new AdvisoryAccumulator({
      maxActive: 10,
      emit: (advisory) => {
        if (run.advisoriesSilent) return;
        for (const listener of run.advisoryListeners) listener({ ...advisory });
        if (!run.stderrCodes.has(advisory.code)) {
          run.stderrCodes.add(advisory.code);
          process.stderr.write(
            `[durably ${run.runId}] ${advisory.code}: ${advisory.msg} ${advisory.hint}\n`,
          );
        }
      },
    });
    accumulator.restore(run.advisories);
    run.accumulator = accumulator;
    return accumulator;
  }

  private observe(
    run: InternalRun,
    code: keyof typeof advisoryRegistry,
    level: "info" | "warn" = "info",
  ): void {
    if (run.advisoriesSilent) return;
    const definition = advisoryRegistry[code];
    const accumulator = run.accumulator ?? this.createAccumulator(run);
    accumulator.observe({
      code,
      level,
      msg: definition.msg,
      hint: definition.hint,
      docs: definition.docs,
      at: new Date(this.clock.now()).toISOString(),
    });
    run.advisories = accumulator.active();
  }

  private attachError(run: InternalRun, error: unknown): Error {
    const value = asError(error);
    if (value instanceof DurablyError) {
      value.runId = run.runId;
      value.advisories = run.advisories.map((item) => ({ ...item }));
    }
    return value;
  }

  private async persist(run: InternalRun): Promise<void> {
    if (this.testMode) {
      run.seq++;
      for (const listener of run.listeners) listener();
      return;
    }
    const completedSteps = run.steps.filter((step) => step.status === "ok").length;
    if (
      run.status === "running"
      && run.steps.length > 200
      && completedSteps % 100 !== 0
    ) return;
    run.seq++;
    const event = {
      type: "state",
      seq: run.seq,
      state: stateOf(run),
    };
    if (run.fastStart && this.storage instanceof FileStorage) {
      this.storage.appendSync(run.runId, event);
      for (const listener of run.listeners) listener();
      return;
    }
    await this.storage.append(run.runId, event);
    for (const listener of run.listeners) listener();
  }

  private newRun(
    definition: WorkflowRuntime<any, any>,
    input: unknown,
    options: EnqueueOptions & { runId?: string; advisories?: "silent" } = {},
  ): InternalRun {
    const now = this.clock.now();
    const runId = options.runId
      ?? (this.testMode ? `run-${++this.#runCounter}` : `${now.toString(36)}-${randomUUID().slice(0, 12)}`);
    return {
      runId,
      status: "pending",
      workflow: definition.name ?? "workflow",
      bodyHash: definition[workflowInternal].bodyHash,
      inputHash: hash(stableStringify(input)),
      input,
      key: options.key,
      steps: [],
      children: [],
      detachedChildren: [],
      annotations: {},
      advisories: [],
      priority: options.priority ?? 0,
      createdAt: now,
      dueAt: now + (options.delayMs ?? 0),
      budgetLimit: { ...(options.budget ?? {}) },
      budgetSpent: {},
      ops: {},
      loops: {},
      seq: 0,
      workflowAttempt: 0,
      rollbackPending: false,
      rollbackIndex: -1,
      workflowDef: definition,
      signalSchemas: new Map(),
      activeControllers: new Set(),
      cancelRequested: false,
      pauseRequested: false,
      listeners: new Set(),
      advisoryListeners: new Set(),
      stderrCodes: new Set(),
      advisoriesSilent: options.advisories === "silent",
      fastStart: false,
    };
  }

  private isTerminal(status: InternalRun["status"]): boolean {
    return status === "completed"
      || status === "failed"
      || status === "cancelled"
      || status === "stale";
  }

  private isParked(status: InternalRun["status"]): boolean {
    return status === "waiting" || status === "sleeping" || status === "paused";
  }

  async enqueue<Input, Output>(
    workflowValue: Workflow<Input, Output>,
    inputValue: Input,
    options: EnqueueOptions = {},
  ): Promise<RunHandle<Output>> {
    return this.enqueueInternal(workflowValue, inputValue, options, false);
  }

  private async enqueueInternal<Input, Output>(
    workflowValue: Workflow<Input, Output>,
    inputValue: Input,
    options: EnqueueOptions,
    matchImplicitIdentity: boolean,
  ): Promise<RunHandle<Output>> {
    await this.initialize();
    await this.refreshRuns();
    const definition = workflowValue as WorkflowRuntime<Input, Output>;
    if (!definition[workflowInternal]) throw new TypeError("invalid durably workflow");
    const input = cloneSerializable(inputValue);
    if (definition[workflowInternal].inputSchema) {
      await schemaValidate(definition[workflowInternal].inputSchema, input);
    }
    const inputHash = hash(stableStringify(input));
    const name = definition.name ?? "workflow";
    const candidates = [...this.runs.values()]
      .filter((run) => run.workflow === name)
      .sort((left, right) => right.createdAt - left.createdAt);
    let existing: InternalRun | undefined;
    if (options.key !== undefined) {
      existing = candidates.find((run) => run.key === options.key);
      if (existing && existing.inputHash !== inputHash) {
        throw new KeyConflictError(`key ${options.key} is already bound to another input`);
      }
    } else if (matchImplicitIdentity) {
      existing = candidates.find((run) =>
        run.bodyHash === definition[workflowInternal].bodyHash
        && run.inputHash === inputHash
        && !this.isTerminal(run.status)
      ) ?? candidates.find((run) =>
        run.bodyHash === definition[workflowInternal].bodyHash
        && run.inputHash === inputHash
        && run.status === "failed"
      );
    }

    if (existing && existing.bodyHash !== definition[workflowInternal].bodyHash) {
      if (!this.isTerminal(existing.status) || existing.status === "stale") {
        existing.status = "stale";
        existing.workflowDef = definition;
        await this.persist(existing);
        throw new StaleRunError(`run ${existing.runId} was created by changed workflow code`, {
          runId: existing.runId,
        });
      }
      existing = undefined;
    }

    if (existing && existing.status !== "completed") {
      existing.workflowDef = definition;
      existing.input = input;
      existing.advisoriesSilent = existing.advisoriesSilent || false;
      if (existing.status === "failed") {
        existing.status = "pending";
        existing.error = undefined;
        existing.compensationError = undefined;
      }
      if (this.#started) this.schedule(existing);
      return { runId: existing.runId } as RunHandle<Output>;
    }

    const run = this.newRun(definition, input, options);
    this.runs.set(run.runId, run);
    await this.storage.createRun({ runId: run.runId, state: stateOf(run) });
    if (this.#started) this.kick();
    return { runId: run.runId } as RunHandle<Output>;
  }

  private schedule(run: InternalRun): void {
    if (!this.#started || run.execution || !run.workflowDef || this.#active >= this.concurrency) return;
    if (run.dueAt > this.clock.now()) return;
    if (this.isTerminal(run.status) || this.isParked(run.status)) return;
    this.#active++;
    const execution = this.claimAndExecute(run)
      .finally(() => {
        if (run.execution === execution) run.execution = undefined;
        this.#active--;
        this.kick();
      });
    run.execution = execution;
  }

  private kick(): void {
    if (!this.#started) return;
    const pending = [...this.runs.values()]
      .filter((run) => !run.execution && run.workflowDef && !this.isTerminal(run.status) && !this.isParked(run.status))
      .sort((left, right) => right.priority - left.priority || left.createdAt - right.createdAt);
    for (const run of pending) {
      if (this.#active >= this.concurrency) break;
      this.schedule(run);
    }
  }

  private async claimAndExecute(run: InternalRun): Promise<void> {
    const claim = {
      owner: this.owner,
      expiresAt: new Date(Date.now() + this.leaseMs).toISOString(),
    };
    const fileStorage = this.storage instanceof FileStorage ? this.storage : undefined;
    const claimed = fileStorage
      ? fileStorage.claimSync(run.runId, claim)
      : await this.storage.claim(run.runId, claim);
    if (!claimed) {
      await new Promise<void>((resolve) => setTimeout(resolve, Math.min(10, this.heartbeatMs)));
      return;
    }
    if (fileStorage) {
      const latestState = fileStorage.readStateSync(run.runId);
      if (latestState) {
        const latest = hydrateRun(latestState as RunState & Record<string, unknown>);
        if (latest.seq > run.seq) {
          latest.workflowDef = run.workflowDef ?? workflowRegistry.get(latest.bodyHash);
          latest.listeners = run.listeners;
          latest.advisoryListeners = run.advisoryListeners;
          latest.stderrCodes = run.stderrCodes;
          latest.injector = run.injector;
          latest.advisoriesSilent = run.advisoriesSilent;
          Object.assign(run, latest);
        }
      }
      run.fastStart = true;
    }
    let heartbeat: ReturnType<typeof setInterval> | undefined;
    if (!this.testMode) {
      heartbeat = setInterval(() => {
        void this.storage.heartbeat(run.runId, {
          owner: this.owner,
          expiresAt: new Date(Date.now() + this.leaseMs).toISOString(),
        });
      }, this.heartbeatMs);
      heartbeat.unref?.();
    }
    try {
      if (!this.isTerminal(run.status)) await this.execute(run);
    } finally {
      if (heartbeat) clearInterval(heartbeat);
      if (fileStorage) {
        fileStorage.releaseSync(run.runId, this.owner);
        run.fastStart = false;
      } else {
        await this.storage.release?.(run.runId, this.owner);
      }
    }
  }

  private async execute(run: InternalRun): Promise<void> {
    if (!run.workflowDef) return;
    run.status = "running";
    run.workflowAttempt++;
    if (run.fastStart || this.testMode || this.storage instanceof MemoryStorage) {
      void this.persist(run);
    }
    else await this.persist(run);
    if (run.rollbackPending) {
      await this.rollback(run);
      return;
    }
    try {
      const result = await this.invokeWorkflow(run, false);
      run.result = cloneSerializable(result);
      const outputSchema = run.workflowDef[workflowInternal].outputSchema;
      if (outputSchema) run.result = await schemaValidate(outputSchema, run.result);
      run.error = undefined;
      run.status = "completed";
      await this.persist(run);
      if (this.shadowReplay) await this.shadow(run);
    } catch (error) {
      if (error instanceof PurityError) throw error;
      if (error instanceof ParkSignal) {
        await this.persist(run);
        return;
      }
      if (error instanceof CrashSignal) {
        run.status = "crashed";
        await this.persist(run);
        return;
      }
      if (error instanceof CompleteSignal) {
        run.result = cloneSerializable(error.value);
        run.status = "completed";
        await this.persist(run);
        if (this.shadowReplay) await this.shadow(run);
        return;
      }
      if (run.cancelRequested || error instanceof RunCancelledError) {
        run.error = this.attachError(run, new RunCancelledError("run cancelled"));
        run.status = "cancelled";
        await this.persist(run);
        return;
      }
      run.error = this.attachError(run, error);
      run.rollbackPending = run.steps.some((step) => step.status === "ok" && step.compensate && !step.compensated);
      run.rollbackIndex = run.steps.length - 1;
      if (run.rollbackPending) {
        await this.rollback(run);
      } else {
        run.status = "failed";
        await this.persist(run);
        await this.hooks?.onRunFailed?.({ runId: run.runId, error: run.error });
      }
    }
  }

  private async invokeWorkflow(run: InternalRun, replay: boolean): Promise<unknown> {
    const definition = run.workflowDef!;
    const context = this.contextFor(run, replay);
    return this.pathStorage.run({ prefix: [], counter: 0 }, () =>
      definition[workflowInternal].fn(context, run.input)
    );
  }

  private async shadow(run: InternalRun): Promise<void> {
    try {
      const result = await this.invokeWorkflow(run, true);
      cloneSerializable(result);
    } catch (error) {
      if (error instanceof CompleteSignal) return;
      if (error instanceof PurityError) {
        run.status = "failed";
        run.error = this.attachError(run, error);
        await this.persist(run);
        throw error;
      }
      throw error;
    }
  }

  private async rollback(run: InternalRun): Promise<void> {
    for (let index = Math.min(run.rollbackIndex, run.steps.length - 1); index >= 0; index--) {
      const step = run.steps[index]!;
      if (step.status !== "ok" || !step.compensate || step.compensated) continue;
      try {
        await step.compensate(step.output);
        step.compensated = true;
        step.compensationError = undefined;
        run.rollbackIndex = index - 1;
        await this.persist(run);
        if (this.shouldCrash(run, `compensate:${step.label}`, step.path, "after")) {
          run.status = "crashed";
          await this.persist(run);
          return;
        }
      } catch (error) {
        step.compensationError = asError(error);
        run.compensationError = step.compensationError;
        run.rollbackIndex = index;
        run.status = "failed";
        await this.persist(run);
        return;
      }
    }
    run.rollbackPending = false;
    run.compensationError = undefined;
    run.status = "failed";
    await this.persist(run);
    await this.hooks?.onRunFailed?.({ runId: run.runId, error: run.error });
  }

  private allocatePath(): number[] {
    const frameValue = this.pathStorage.getStore();
    if (!frameValue) throw new Error("workflow context used outside execution");
    return [...frameValue.prefix, frameValue.counter++];
  }

  private selectorMatches(
    run: InternalRun,
    selector: Selector | undefined,
    label: string,
    path: readonly number[],
  ): boolean {
    if (!selector) return false;
    if (Array.isArray(selector)) {
      return selector.length === path.length && selector.every((value, index) => value === path[index]);
    }
    if (typeof selector === "string") {
      if (label.startsWith("compensate:")) return selector === label;
      const prior = run.steps.filter((step) => step.label === label && comparePaths(step.path, path) <= 0);
      return selector === label && prior.length === 1;
    }
    const candidate = selector as { label: string; nth: number };
    if (candidate.label !== label) return false;
    const occurrence = run.steps.filter((step) => step.label === label && comparePaths(step.path, path) <= 0).length;
    return occurrence === candidate.nth;
  }

  private shouldCrash(
    run: InternalRun,
    label: string,
    path: readonly number[],
    kind: "after" | "inside",
  ): boolean {
    const injector = run.injector;
    if (!injector || injector.used) return false;
    const selector = kind === "after" ? injector.crashAfter : injector.crashInStep;
    if (!this.selectorMatches(run, selector, label, path)) return false;
    injector.used = true;
    return true;
  }

  private async acquireResource(name: string | undefined): Promise<{
    release: () => void;
    success: () => Promise<void>;
    failure: () => Promise<void>;
  }> {
    if (!name) {
      return {
        release: () => undefined,
        success: async () => undefined,
        failure: async () => undefined,
      };
    }
    const runtime = this.resourceRuntimes.get(name);
    if (!runtime) throw new Error(`unknown resource: ${name}`);
    const now = this.clock.now();
    if (runtime.openUntil > now) {
      throw new CircuitOpenError(`resource ${name} circuit is open`, { resource: name });
    }
    if (runtime.openUntil > 0 && runtime.openUntil <= now) {
      if (runtime.probing) {
        throw new CircuitOpenError(`resource ${name} circuit is half-open`, { resource: name });
      }
      runtime.probing = true;
    }
    const rate = runtime.definition.options.rateLimit;
    if (rate) {
      runtime.tokens = runtime.tokens.filter((at) => at > this.clock.now() - rate.perMs);
      if (runtime.tokens.length >= rate.max) {
        await this.clock.sleep(Math.max(0, runtime.tokens[0]! + rate.perMs - this.clock.now()));
        runtime.tokens = runtime.tokens.filter((at) => at > this.clock.now() - rate.perMs);
      }
      runtime.tokens.push(this.clock.now());
    }
    const persist = async () => {
      await this.storage.saveResourceState?.(name, {
        failures: runtime.failures,
        openUntil: runtime.openUntil,
      });
    };
    return {
      release: () => undefined,
      success: async () => {
        runtime.probing = false;
        runtime.openUntil = 0;
        runtime.failures.push({ at: this.clock.now(), failed: false });
        await persist();
      },
      failure: async () => {
        runtime.probing = false;
        const breaker = runtime.definition.options.breaker;
        if (!breaker) return;
        const at = this.clock.now();
        runtime.failures = runtime.failures.filter((item) => item.at >= at - breaker.windowMs);
        runtime.failures.push({ at, failed: true });
        const failed = runtime.failures.filter((item) => item.failed).length;
        if (failed / runtime.failures.length >= breaker.failureRate) {
          runtime.openUntil = at + breaker.cooldownMs;
        }
        await persist();
      },
    };
  }

  private acquireResourceImmediate(name: string | undefined): {
    release: () => void;
    success: () => Promise<void>;
    failure: () => Promise<void>;
  } | undefined {
    if (!name) {
      return {
        release: () => undefined,
        success: async () => undefined,
        failure: async () => undefined,
      };
    }
    const runtime = this.resourceRuntimes.get(name);
    if (!runtime || runtime.definition.options.rateLimit) return undefined;
    const now = this.clock.now();
    if (runtime.openUntil > now) {
      throw new CircuitOpenError(`resource ${name} circuit is open`, { resource: name });
    }
    if (runtime.openUntil > 0 && runtime.openUntil <= now) {
      if (runtime.probing) {
        throw new CircuitOpenError(`resource ${name} circuit is half-open`, { resource: name });
      }
      runtime.probing = true;
    }
    const persist = async () => {
      await this.storage.saveResourceState?.(name, {
        failures: runtime.failures,
        openUntil: runtime.openUntil,
      });
    };
    return {
      release: () => undefined,
      success: async () => {
        runtime.probing = false;
        runtime.openUntil = 0;
        runtime.failures.push({ at: this.clock.now(), failed: false });
        await persist();
      },
      failure: async () => {
        runtime.probing = false;
        const breaker = runtime.definition.options.breaker;
        if (!breaker) return;
        const at = this.clock.now();
        runtime.failures = runtime.failures.filter((item) => item.at >= at - breaker.windowMs);
        runtime.failures.push({ at, failed: true });
        if (runtime.failures.filter((item) => item.failed).length / runtime.failures.length >= breaker.failureRate) {
          runtime.openUntil = at + breaker.cooldownMs;
        }
        await persist();
      },
    };
  }

  private contextFor(run: InternalRun, replay: boolean): WorkflowContext {
    const engine = this;

    const step = async (
      fn: (context: {
        signal: AbortSignal;
        stash: (value: unknown) => Promise<void>;
        stashed: unknown;
      }) => MaybePromise<unknown>,
      options: {
        name?: string;
        retry?: {
          attempts: number;
          backoff: "exponential" | "linear" | "none";
          baseMs?: number;
          maxMs?: number;
          jitter?: boolean;
        };
        timeoutMs?: number;
        schema?: StandardSchemaV1;
        compensate?: (output: unknown) => MaybePromise<void>;
        concurrencyKey?: string;
        concurrencyLimit?: number;
        uses?: string;
      } = {},
    ): Promise<unknown> => {
      if (run.cancelRequested) throw new RunCancelledError("run cancelled");
      if (run.pauseRequested && !replay) {
        run.status = "paused";
        throw new ParkSignal();
      }
      const path = engine.allocatePath();
      const key = pathKey(path);
      const label = options.name ?? autoLabel(fn);
      let record = run.steps.find((candidate) => pathKey(candidate.path) === key);

      if (replay) {
        if (!record || record.label !== label) {
          throw new PurityError(`step at ${key} diverged during shadow replay`, {
            path,
            expected: record ? { label: record.label, status: record.status } : "no step",
            actual: { label },
          });
        }
        if (record.status === "failed") throw record.error;
        if (record.status !== "ok") {
          throw new PurityError(`step at ${key} was incomplete during shadow replay`, {
            path,
            expected: { label: record.label, status: "ok or failed" },
            actual: { label, status: record.status },
          });
        }
        return options.schema ? schemaValidate(options.schema, record.output) : record.output;
      }

      if (record?.status === "ok") {
        record.compensate = options.compensate;
        try {
          return options.schema ? await schemaValidate(options.schema, record.output) : record.output;
        } catch (error) {
          record.status = "failed";
          record.error = engine.attachError(run, error);
          await engine.persist(run);
          throw error;
        }
      }
      if (!record) {
        record = {
          path,
          label,
          status: "running",
          executions: 0,
          attempts: 0,
          attemptHistory: [],
        };
        run.steps.push(record);
      }
      record.label = label;
      record.compensate = options.compensate;

      const previous = record.attemptHistory.at(-1);
      if (
        record.executions > 0
        && previous
        && previous.ms > 30_000
        && !previous.stashed
      ) {
        engine.observe(run, "STASH_SUGGESTED");
      }

      const retry = options.retry ?? { attempts: 1, backoff: "none" as const };
      const maxAttempts = Math.max(1, retry.attempts);
      let lastError: unknown;
      for (let localAttempt = 1; localAttempt <= maxAttempts; localAttempt++) {
        if (run.cancelRequested) throw new RunCancelledError("run cancelled");
        record.status = "running";
        record.error = undefined;
        record.attempts++;
        record.executions++;
        const attempt = record.attempts;
        const started = engine.clock.now();
        record.startedAt = new Date(started).toISOString();
        if (run.fastStart || engine.testMode || engine.storage instanceof MemoryStorage) {
          void engine.persist(run);
        }
        else await engine.persist(run);

        const controller = new AbortController();
        run.activeControllers.add(controller);
        let stashedThisAttempt = record.stashed !== undefined;
        const stash = async (value: unknown) => {
          record!.stashed = cloneSerializable(value);
          stashedThisAttempt = true;
          await engine.persist(run);
        };
        let keyedSemaphore: Semaphore | undefined;
        let acquiredResource:
          | {
              release: () => void;
              success: () => Promise<void>;
              failure: () => Promise<void>;
            }
          | undefined;
        try {
          if (options.concurrencyKey) {
            const semaphoreKey = `${options.concurrencyKey}:${options.concurrencyLimit ?? 1}`;
            keyedSemaphore = engine.keyedSemaphores.get(semaphoreKey)
              ?? new Semaphore(options.concurrencyLimit ?? 1);
            engine.keyedSemaphores.set(semaphoreKey, keyedSemaphore);
          }
          acquiredResource = engine.acquireResourceImmediate(options.uses)
            ?? await engine.acquireResource(options.uses);
          let settled = false;
          const invokeEffect = () => fn({
            signal: controller.signal,
            stash,
            stashed: record!.stashed,
          });
          const resourceSemaphore = options.uses
            ? engine.resourceRuntimes.get(options.uses)?.semaphore
            : undefined;
          const withResource = () => resourceSemaphore
            ? resourceSemaphore.run(invokeEffect)
            : invokeEffect();
          const effect = keyedSemaphore
            ? keyedSemaphore.run(withResource)
            : Promise.resolve(withResource());
          const attempted = options.timeoutMs === undefined
            ? effect
            : Promise.race([
                effect,
                engine.clock.sleep(options.timeoutMs).then(() => {
                  if (settled) return new Promise<never>(() => undefined);
                  const timeout = new StepTimeoutError(
                    `step ${label} timed out after ${options.timeoutMs}ms`,
                    { timeoutMs: options.timeoutMs },
                  );
                  controller.abort(timeout);
                  throw timeout;
                }),
              ]);
          const raw = await attempted;
          settled = true;
          const ended = engine.clock.now();
          const attemptRecord: AttemptRecord = {
            attempt,
            startedAt: new Date(started).toISOString(),
            ms: ended - started,
            stashed: stashedThisAttempt,
          };
          record.attemptHistory.push(attemptRecord);
          record.ms = attemptRecord.ms;

          if (engine.shouldCrash(run, label, path, "inside")) {
            await engine.persist(run);
            throw new CrashSignal();
          }

          const serializable = cloneSerializable(raw);
          const output = options.schema
            ? await schemaValidate(options.schema, serializable)
            : serializable;
          record.output = output;
          record.status = "ok";
          record.error = undefined;
          record.stashed = undefined;
          if (options.uses) await acquiredResource.success();
          run.accumulator?.retrySucceeded({ runId: run.runId, path });
          const completedSteps = run.steps.filter((item) => item.status === "ok").length;
          if (completedSteps === 1_000) engine.observe(run, "LOOP_SUGGESTED", "info");
          if (completedSteps === 5_000) engine.observe(run, "LOOP_SUGGESTED", "warn");
          if (attemptRecord.ms > 30_000 && options.timeoutMs === undefined) {
            engine.observe(run, "SLOW_STEP_NO_TIMEOUT");
          }
          if (run.fastStart || engine.testMode || engine.storage instanceof MemoryStorage) {
            void engine.persist(run);
          }
          else await engine.persist(run);
          if (engine.shouldCrash(run, label, path, "after")) throw new CrashSignal();
          return output;
        } catch (error) {
          if (error instanceof CrashSignal) throw error;
          lastError = error;
          const ended = engine.clock.now();
          if (!record.attemptHistory.some((item) => item.attempt === attempt)) {
            record.attemptHistory.push({
              attempt,
              startedAt: new Date(started).toISOString(),
              ms: ended - started,
              stashed: stashedThisAttempt,
            });
          }
          record.ms = ended - started;
          record.error = engine.attachError(run, error);
          if (options.uses) await acquiredResource?.failure();
          const errorClass = asError(error).constructor.name;
          const accumulator = run.accumulator ?? engine.createAccumulator(run);
          accumulator.retryFailed({ runId: run.runId, path, errorClass, attempt });
          run.advisories = accumulator.active();
          if (localAttempt >= maxAttempts) {
            record.status = "failed";
            await engine.persist(run);
            throw record.error;
          }
          if (run.fastStart || engine.testMode || engine.storage instanceof MemoryStorage) {
            void engine.persist(run);
          }
          else await engine.persist(run);
          const delay = computeBackoffMs({
            attempt: localAttempt,
            backoff: retry.backoff,
            baseMs: retry.baseMs,
            maxMs: retry.maxMs,
            jitter: retry.jitter,
            random: Math.random,
          });
          await engine.clock.sleep(delay);
        } finally {
          run.activeControllers.delete(controller);
          acquiredResource?.release();
        }
      }
      throw lastError;
    };

    const spawnAt = async (
      path: number[],
      definition: WorkflowRuntime,
      input: unknown,
      index: number,
      detached: boolean,
    ): Promise<RunHandle<unknown>> => {
      const childId = `${run.runId}:${path.join(":")}:${index}`;
      let child = engine.runs.get(childId);
      if (!child) {
        const clonedInput = cloneSerializable(input);
        child = engine.newRun(definition, clonedInput, { runId: childId });
        engine.runs.set(childId, child);
        run.children.push(childId);
        if (detached) run.detachedChildren.push(childId);
        await engine.storage.createRun({ runId: childId, state: stateOf(child) });
        await engine.persist(run);
      } else {
        child.workflowDef = definition;
      }
      if (!replay && !child.execution && !engine.isTerminal(child.status) && !engine.isParked(child.status)) {
        if (engine.testMode) await engine.claimAndExecute(child);
        else engine.schedule(child);
      }
      return { runId: childId } as RunHandle<unknown>;
    };

    const context = {
      runId: run.runId,
      attempt: run.workflowAttempt,
      budget: {
        remaining(kind: BudgetKind): number {
          const limit = run.budgetLimit[kind];
          return limit === undefined ? Number.POSITIVE_INFINITY : limit - (run.budgetSpent[kind] ?? 0);
        },
        spent(kind: BudgetKind): number {
          return run.budgetSpent[kind] ?? 0;
        },
      },
      step,
      async parallel(
        thunks: readonly (() => MaybePromise<unknown>)[],
        options: { concurrency?: number } = {},
      ): Promise<Result<unknown>[]> {
        const operationPath = engine.allocatePath();
        const concurrency = Math.max(1, options.concurrency ?? (thunks.length || 1));
        const results: Result<unknown>[] = new Array(thunks.length);
        let next = 0;
        let fatal: unknown;
        const worker = async () => {
          while (next < thunks.length && !fatal && !run.cancelRequested) {
            const index = next++;
            try {
              const value = await engine.pathStorage.run(
                { prefix: [...operationPath, index], counter: 0 },
                () => thunks[index]!(),
              );
              results[index] = { ok: true, value };
            } catch (error) {
              if (
                error instanceof CrashSignal
                || error instanceof ParkSignal
                || error instanceof PurityError
                || error instanceof CompleteSignal
              ) {
                fatal = error;
                break;
              }
              results[index] = { ok: false, error: errorValue(error) };
            }
          }
        };
        await Promise.all(Array.from({ length: Math.min(concurrency, thunks.length) }, worker));
        if (fatal) throw fatal;
        if (run.cancelRequested) throw new RunCancelledError("run cancelled");
        return results;
      },
      async spawn(
        definition: WorkflowRuntime,
        input: unknown,
        options: { detached?: boolean } = {},
      ): Promise<RunHandle<unknown>> {
        const operationPath = engine.allocatePath();
        return spawnAt(operationPath, definition, input, 0, options.detached ?? false);
      },
      async spawnAll(
        definitions: readonly (readonly [WorkflowRuntime, unknown])[],
        options: { detached?: boolean } = {},
      ): Promise<RunHandle<unknown>[]> {
        const operationPath = engine.allocatePath();
        const handles = await Promise.all(definitions.map(([definition, input], index) =>
          spawnAt(operationPath, definition, input, index, options.detached ?? false)
        ));
        if (!replay) {
          const count = run.children.length;
          if (count >= 1_000 && !run.advisories.some((item) => item.code === "FANOUT_CEILING")) {
            engine.observe(run, "FANOUT_CEILING", "info");
          }
          if (count >= 5_000) engine.observe(run, "FANOUT_CEILING", "warn");
          await engine.persist(run);
        }
        return handles;
      },
      async joinAll(handles: readonly RunHandle<unknown>[]): Promise<Result<unknown>[]> {
        const results: Result<unknown>[] = [];
        for (const handle of handles) {
          const child = engine.runs.get(handle.runId);
          if (!child) {
            results.push({ ok: false, error: errorValue(new Error(`child ${handle.runId} not found`)) });
            continue;
          }
          if (!engine.isTerminal(child.status)) await engine.waitForSettled(child);
          if (child.status === "completed") results.push({ ok: true, value: child.result });
          else results.push({
            ok: false,
            error: errorValue(child.error ?? new Error(`child ${handle.runId} ${child.status}`)),
          });
        }
        return results;
      },
      async loop(
        initialState: unknown,
        reducer: (
          state: unknown,
          tools: {
            step: typeof step;
            done: (value: unknown) => RuntimeLoopDone;
          },
        ) => MaybePromise<unknown>,
        options: { snapshotEvery?: number; maxIterations?: number } = {},
      ): Promise<unknown> {
        const operationPath = engine.allocatePath();
        const key = pathKey(operationPath);
        let record = run.loops[key];
        if (!record) {
          record = {
            snapshotState: cloneSerializable(initialState),
            snapshotIteration: 0,
            snapshots: 0,
          };
          run.loops[key] = record;
        }
        if (record.done) return record.output;
        let state = cloneSerializable(record.snapshotState);
        let iteration = record.snapshotIteration;
        const snapshotEvery = Math.max(1, options.snapshotEvery ?? 1);
        const maxIterations = options.maxIterations ?? Number.POSITIVE_INFINITY;
        while (iteration < maxIterations) {
          if (run.pauseRequested && !replay) {
            run.status = "paused";
            throw new ParkSignal();
          }
          const value = await engine.pathStorage.run(
            { prefix: [...operationPath, iteration], counter: 0 },
            () => reducer(state, {
              step,
              done: (output) => ({ [loopDone]: true, value: output }),
            }),
          );
          if (value && typeof value === "object" && (value as RuntimeLoopDone)[loopDone]) {
            const output = cloneSerializable((value as RuntimeLoopDone).value);
            record.done = true;
            record.output = output;
            if (!replay) await engine.persist(run);
            return output;
          }
          state = value;
          iteration++;
          if (iteration % snapshotEvery === 0) {
            record.snapshotState = cloneSerializable(state);
            record.snapshotIteration = iteration;
            record.snapshots++;
            const size = stableStringify(record.snapshotState).length;
            if (size * record.snapshots >= 5_000_000) engine.observe(run, "SNAPSHOT_HEAVY");
            if (!replay) await engine.persist(run);
          }
        }
        throw new DurablyError(`loop reached maxIterations (${maxIterations})`);
      },
      async waitFor(
        name: string,
        schemaOrOptions?: StandardSchemaV1 | { timeoutMs?: number },
        maybeOptions?: { timeoutMs?: number },
      ): Promise<unknown> {
        const operationPath = engine.allocatePath();
        const key = pathKey(operationPath);
        const schema = schemaOrOptions && "~standard" in schemaOrOptions
          ? schemaOrOptions as StandardSchemaV1
          : undefined;
        const options = schema
          ? maybeOptions ?? {}
          : (schemaOrOptions as { timeoutMs?: number } | undefined) ?? maybeOptions ?? {};
        let operation = run.ops[key];
        if (operation?.payload !== undefined) {
          const payload = operation.payload;
          operation.payload = undefined;
          run.waitingFor = undefined;
          return schemaValidate(schema, payload);
        }
        if (operation?.timeoutAt !== undefined && engine.clock.now() >= operation.timeoutAt) {
          throw new StepTimeoutError(`waitFor ${name} timed out after ${options.timeoutMs}ms`, {
            timeoutMs: options.timeoutMs,
          });
        }
        if (!operation) {
          operation = {
            kind: "wait",
            signal: name,
            ...(options.timeoutMs !== undefined ? { timeoutAt: engine.clock.now() + options.timeoutMs } : {}),
          };
          run.ops[key] = operation;
        }
        run.signalSchemas.set(name, schema);
        run.waitingFor = {
          signal: name,
          since: new Date(engine.clock.now()).toISOString(),
          ...(operation.timeoutAt !== undefined ? { timeoutAt: operation.timeoutAt } : {}),
        };
        run.status = "waiting";
        if (options.timeoutMs === undefined) engine.observe(run, "WAITFOR_NO_TIMEOUT");
        throw new ParkSignal();
      },
      async sleep(ms: number): Promise<void> {
        const operationPath = engine.allocatePath();
        const key = pathKey(operationPath);
        const operation = run.ops[key] ?? { kind: "sleep", wakeAt: engine.clock.now() + ms };
        run.ops[key] = operation;
        if (engine.clock.now() < operation.wakeAt!) {
          run.wakeAt = operation.wakeAt;
          run.status = "sleeping";
          throw new ParkSignal();
        }
        run.wakeAt = undefined;
      },
      async sleepUntil(date: Date): Promise<void> {
        const operationPath = engine.allocatePath();
        const key = pathKey(operationPath);
        const operation = run.ops[key] ?? { kind: "sleep", wakeAt: date.getTime() };
        run.ops[key] = operation;
        if (engine.clock.now() < operation.wakeAt!) {
          run.wakeAt = operation.wakeAt;
          run.status = "sleeping";
          throw new ParkSignal();
        }
        run.wakeAt = undefined;
      },
      now(): number {
        const path = engine.allocatePath();
        const key = pathKey(path);
        const operation = run.ops[key] ?? { kind: "now", value: engine.clock.now() };
        run.ops[key] = operation;
        return operation.value as number;
      },
      random(): number {
        const path = engine.allocatePath();
        const key = pathKey(path);
        const operation = run.ops[key] ?? { kind: "random", value: Math.random() };
        run.ops[key] = operation;
        return operation.value as number;
      },
      log(...args: readonly unknown[]): void {
        const path = engine.allocatePath();
        const key = pathKey(path);
        if (!run.ops[key] && !replay) {
          run.ops[key] = { kind: "log", value: cloneSerializable(args) };
          console.log(...args);
        }
      },
      annotate(values: Record<string, JsonValue>): void {
        const path = engine.allocatePath();
        const key = pathKey(path);
        if (run.ops[key] || replay) return;
        const cloned = cloneSerializable(values) as Record<string, JsonValue>;
        run.ops[key] = { kind: "annotate", value: cloned };
        Object.assign(run.annotations, cloned);
      },
      charge(amount: Budget): void {
        const path = engine.allocatePath();
        const key = pathKey(path);
        if (run.ops[key] || replay) return;
        const cloned = cloneSerializable(amount) as Budget;
        run.ops[key] = { kind: "charge", value: cloned };
        for (const kind of ["usd", "tokens", "units"] as BudgetKind[]) {
          const charged = cloned[kind] ?? 0;
          if (!charged) continue;
          run.budgetSpent[kind] = (run.budgetSpent[kind] ?? 0) + charged;
          engine.engineSpent[kind] = (engine.engineSpent[kind] ?? 0) + charged;
          const runLimit = run.budgetLimit[kind];
          const engineLimit = engine.engineBudget[kind];
          if (runLimit !== undefined && run.budgetSpent[kind]! >= runLimit * 0.8 && run.budgetSpent[kind]! <= runLimit) {
            if (!run.advisories.some((item) => item.code === "BUDGET_NEAR")) {
              engine.observe(run, "BUDGET_NEAR");
            }
          }
          if (
            (runLimit !== undefined && run.budgetSpent[kind]! > runLimit)
            || (engineLimit !== undefined && engine.engineSpent[kind]! > engineLimit)
          ) {
            throw new BudgetExceededError(`${kind} budget exceeded`, { kind });
          }
        }
      },
      complete(value: unknown): never {
        throw new CompleteSignal(cloneSerializable(value));
      },
    };
    return context as unknown as WorkflowContext;
  }

  async inspect(runId: string): Promise<RunState | null> {
    await this.initialize();
    await this.refreshRuns();
    const run = this.runs.get(runId);
    return run ? stateOf(run) : null;
  }

  async list(options: ListOptions = {}): Promise<RunState[]> {
    await this.initialize();
    await this.refreshRuns();
    return [...this.runs.values()]
      .filter((run) =>
        (!options.status || run.status === options.status)
        && (!options.workflow || run.workflow === options.workflow)
      )
      .sort((left, right) => left.createdAt - right.createdAt)
      .slice(0, options.limit)
      .map(stateOf);
  }

  async signal(runId: string, name: string, payload: unknown): Promise<void> {
    await this.start();
    await this.refreshRuns();
    const run = this.runs.get(runId);
    if (!run) throw new Error(`run ${runId} not found`);
    const operation = Object.values(run.ops).find((item) => item.kind === "wait" && item.signal === name);
    if (!operation || run.waitingFor?.signal !== name) return;
    const schema = run.signalSchemas.get(name);
    operation.payload = await schemaValidate(schema, cloneSerializable(payload));
    run.status = "pending";
    run.waitingFor = undefined;
    await this.persist(run);
    this.schedule(run);
  }

  async cancel(runId: string): Promise<void> {
    await this.start();
    const run = this.runs.get(runId);
    if (!run) return;
    run.cancelRequested = true;
    for (const controller of run.activeControllers) {
      controller.abort(new RunCancelledError("run cancelled", { runId }));
    }
    run.status = "cancelled";
    run.error = this.attachError(run, new RunCancelledError("run cancelled", { runId }));
    await this.persist(run);
    for (const childId of run.children) {
      if (!run.detachedChildren.includes(childId)) await this.cancel(childId);
    }
  }

  async pause(runId: string): Promise<void> {
    await this.start();
    const run = this.runs.get(runId);
    if (!run || this.isTerminal(run.status)) return;
    run.pauseRequested = true;
    if (!run.execution) run.status = "paused";
    await this.persist(run);
  }

  async resume(runId: string): Promise<void> {
    await this.start();
    const run = this.runs.get(runId);
    if (!run) throw new Error(`run ${runId} not found`);
    run.pauseRequested = false;
    if (
      run.status === "paused"
      || run.status === "crashed"
      || run.status === "failed"
      || (run.status === "sleeping" && (run.wakeAt ?? Number.POSITIVE_INFINITY) <= this.clock.now())
    ) {
      run.status = "pending";
      await this.persist(run);
      this.schedule(run);
    }
  }

  async retry(runId: string, options: RetryRunOptions = {}): Promise<void> {
    await this.start();
    const run = this.runs.get(runId);
    if (!run) throw new Error(`run ${runId} not found`);
    if (options.fromStep !== undefined) {
      const selected = run.steps.find((step) =>
        this.selectorMatches(run, options.fromStep, step.label, step.path)
      );
      if (!selected) throw new Error(`step selector did not match in ${runId}`);
      for (const step of run.steps) {
        if (comparePaths(step.path, selected.path) >= 0) {
          step.status = "running";
          step.output = undefined;
          step.error = undefined;
          step.compensated = false;
        }
      }
    }
    run.status = "pending";
    run.error = undefined;
    run.rollbackPending = false;
    run.compensationError = undefined;
    await this.persist(run);
    this.schedule(run);
  }

  async adopt(runId: string): Promise<void> {
    await this.start();
    const run = this.runs.get(runId);
    if (!run) throw new Error(`run ${runId} not found`);
    if (!run.workflowDef) throw new Error(`no replacement workflow supplied for ${runId}`);
    run.bodyHash = run.workflowDef[workflowInternal].bodyHash;
    run.adoption = { assertedCompatible: true, bodyHash: run.bodyHash };
    run.status = run.waitingFor ? "waiting" : run.wakeAt ? "sleeping" : "pending";
    await this.persist(run);
  }

  async restart(runId: string): Promise<RunHandle<unknown>> {
    await this.start();
    const prior = this.runs.get(runId);
    if (!prior) throw new Error(`run ${runId} not found`);
    if (!prior.workflowDef) throw new Error(`no replacement workflow supplied for ${runId}`);
    const fresh = this.newRun(prior.workflowDef, prior.input, {
      ...(prior.key !== undefined ? { key: prior.key } : {}),
      priority: prior.priority,
      budget: prior.budgetLimit,
    });
    this.runs.set(fresh.runId, fresh);
    await this.storage.createRun({ runId: fresh.runId, state: stateOf(fresh) });
    this.schedule(fresh);
    return { runId: fresh.runId } as RunHandle<unknown>;
  }

  async drain(): Promise<void> {
    await this.start();
    for (;;) {
      await this.refreshRuns();
      for (const run of this.runs.values()) {
        if (run.status === "sleeping" && (run.wakeAt ?? Number.POSITIVE_INFINITY) <= this.clock.now()) {
          run.status = "pending";
        }
      }
      this.kick();
      const executions = [...this.runs.values()]
        .map((run) => run.execution)
        .filter((value): value is Promise<void> => Boolean(value));
      const foreignActive = [...this.runs.values()].some((run) =>
        run.workflowDef && run.status === "running" && !run.execution
      );
      const runnable = [...this.runs.values()].some((run) =>
        run.workflowDef
        && !run.execution
        && !this.isTerminal(run.status)
        && !this.isParked(run.status)
        && run.dueAt <= this.clock.now()
      );
      if (executions.length === 0 && !foreignActive && !runnable) return;
      await Promise.race([
        ...executions.map((execution) => execution.catch(() => undefined)),
        new Promise<void>((resolve) => setTimeout(resolve, 10)),
      ]);
    }
  }

  async stop(): Promise<void> {
    this.#started = false;
  }

  private initializeFileSync(storage: FileStorage): void {
    if (!this.#initialized) {
      storage.initSync();
      for (const item of storage.listSync()) {
        if (this.runs.has(item.runId)) continue;
        const run = hydrateRun(item.state as RunState & Record<string, unknown>);
        run.workflowDef = workflowRegistry.get(run.bodyHash);
        this.runs.set(item.runId, run);
      }
      this.#initialized = true;
    }
    this.#started = true;
  }

  private attachDirectOptions(run: InternalRun, options: RunOptions): void {
    run.advisoriesSilent = options.advisories === "silent";
    if (options.onAdvisory && !run.advisoriesSilent) {
      run.advisoryListeners.add(options.onAdvisory);
      for (const advisory of run.advisories) options.onAdvisory({ ...advisory });
    }
  }

  private directResult<Output>(run: InternalRun): Promise<Output> {
    return this.waitForSettled(run).then(() => {
      if (run.status === "completed") return run.result as Output;
      throw this.attachError(run, run.error ?? new Error(`run ${run.runId} ended as ${run.status}`));
    });
  }

  private startFileExecution(run: InternalRun, storage: FileStorage): void {
    if (run.execution) return;
    const claim = {
      owner: this.owner,
      expiresAt: new Date(Date.now() + this.leaseMs).toISOString(),
    };
    if (!storage.claimSync(run.runId, claim)) {
      this.schedule(run);
      return;
    }
    run.fastStart = true;
    this.#active++;
    const heartbeat = setInterval(() => {
      void storage.heartbeat(run.runId, {
        owner: this.owner,
        expiresAt: new Date(Date.now() + this.leaseMs).toISOString(),
      });
    }, this.heartbeatMs);
    heartbeat.unref?.();
    const execution = this.execute(run).finally(() => {
      clearInterval(heartbeat);
      storage.releaseSync(run.runId, this.owner);
      run.fastStart = false;
      if (run.execution === execution) run.execution = undefined;
      this.#active--;
      this.kick();
    });
    run.execution = execution;
  }

  runDirectFast<Input, Output>(
    workflowValue: Workflow<Input, Output>,
    inputValue: Input,
    options: RunOptions,
  ): Promise<Output> {
    if (!(this.storage instanceof FileStorage)) {
      return this.runDirect(workflowValue, inputValue, options);
    }
    const storage = this.storage;
    const definition = workflowValue as WorkflowRuntime<Input, Output>;
    if (!definition[workflowInternal]) return Promise.reject(new TypeError("invalid durably workflow"));
    if (definition[workflowInternal].inputSchema) {
      return this.runDirect(workflowValue, inputValue, options);
    }

    let input: Input;
    try {
      input = cloneSerializable(inputValue) as Input;
      this.initializeFileSync(storage);
    } catch (error) {
      return Promise.reject(error);
    }

    const inputHash = hash(stableStringify(input));
    const name = definition.name ?? "workflow";
    const candidates = [...this.runs.values()]
      .filter((run) => run.workflow === name)
      .sort((left, right) => right.createdAt - left.createdAt);
    let existing: InternalRun | undefined;
    if (!options.fresh && options.key !== undefined) {
      existing = candidates.find((run) => run.key === options.key);
      if (existing && existing.inputHash !== inputHash) {
        return Promise.reject(
          new KeyConflictError(`key ${options.key} is already bound to another input`),
        );
      }
    } else if (!options.fresh) {
      existing = candidates.find((run) =>
        run.bodyHash === definition[workflowInternal].bodyHash
        && run.inputHash === inputHash
        && !this.isTerminal(run.status)
      ) ?? candidates.find((run) =>
        run.bodyHash === definition[workflowInternal].bodyHash
        && run.inputHash === inputHash
        && run.status === "failed"
      );
    }

    if (existing && existing.bodyHash !== definition[workflowInternal].bodyHash) {
      if (!this.isTerminal(existing.status) || existing.status === "stale") {
        existing.status = "stale";
        existing.workflowDef = definition;
        existing.fastStart = true;
        void this.persist(existing);
        existing.fastStart = false;
        return Promise.reject(new StaleRunError(
          `run ${existing.runId} was created by changed workflow code`,
          { runId: existing.runId },
        ));
      }
      existing = undefined;
    }

    let run = existing && existing.status !== "completed" ? existing : undefined;
    if (run) {
      run.workflowDef = definition;
      run.input = input;
      if (run.status === "failed") {
        run.status = "pending";
        run.error = undefined;
        run.compensationError = undefined;
      }
    } else {
      run = this.newRun(definition, input, {
        ...(options.key !== undefined ? { key: options.key } : {}),
        ...(options.budget !== undefined ? { budget: options.budget } : {}),
        ...(options.advisories !== undefined ? { advisories: options.advisories } : {}),
      });
      this.runs.set(run.runId, run);
      storage.createRunSync({ runId: run.runId, state: stateOf(run) });
    }

    this.attachDirectOptions(run, options);
    this.startFileExecution(run, storage);
    return this.directResult<Output>(run);
  }

  async runDirect<Input, Output>(
    workflowValue: Workflow<Input, Output>,
    input: Input,
    options: RunOptions,
  ): Promise<Output> {
    await this.start();
    let handle: RunHandle<Output>;
    if (options.fresh) {
      const definition = workflowValue as WorkflowRuntime<Input, Output>;
      const cloned = cloneSerializable(input);
      const fresh = this.newRun(definition, cloned, {
        ...(options.key !== undefined ? { key: options.key } : {}),
        ...(options.budget !== undefined ? { budget: options.budget } : {}),
        ...(options.advisories !== undefined ? { advisories: options.advisories } : {}),
      });
      this.runs.set(fresh.runId, fresh);
      await this.storage.createRun({ runId: fresh.runId, state: stateOf(fresh) });
      handle = { runId: fresh.runId } as RunHandle<Output>;
      this.schedule(fresh);
    } else {
      handle = await this.enqueueInternal(workflowValue, input, {
        ...(options.key !== undefined ? { key: options.key } : {}),
        ...(options.budget !== undefined ? { budget: options.budget } : {}),
      }, true);
    }
    const run = this.runs.get(handle.runId)!;
    run.advisoriesSilent = options.advisories === "silent";
    if (options.onAdvisory && !run.advisoriesSilent) {
      run.advisoryListeners.add(options.onAdvisory);
      for (const advisory of run.advisories) options.onAdvisory({ ...advisory });
    }
    this.schedule(run);
    await this.waitForSettled(run);
    if (run.status === "completed") return run.result as Output;
    throw this.attachError(run, run.error ?? new Error(`run ${run.runId} ended as ${run.status}`));
  }

  private async waitForSettled(run: InternalRun): Promise<void> {
    for (;;) {
      if (run.status === "completed" || run.status === "failed" || run.status === "cancelled" || run.status === "stale") {
        return;
      }
      if (run.execution) {
        await run.execution;
        continue;
      }
      await this.refreshRuns();
      const current = this.runs.get(run.runId);
      if (current && current !== run) Object.assign(run, current);
      this.schedule(run);
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
  }

  async testRun<Input, Output>(
    workflowValue: Workflow<Input, Output>,
    inputValue: Input,
    options: TestRunOptions,
  ): Promise<TestRunResult<Output>> {
    if (!this.testMode) await this.start();
    const definition = workflowValue as WorkflowRuntime<Input, Output>;
    const input = cloneSerializable(inputValue);
    if (definition[workflowInternal].inputSchema) {
      await schemaValidate(definition[workflowInternal].inputSchema, input);
    }
    const run = this.newRun(definition, input, {
      ...(options.budget !== undefined ? { budget: options.budget } : {}),
      ...(options.advisories !== undefined ? { advisories: options.advisories } : {}),
    });
    run.injector = {
      crashAfter: options.crashAfter,
      crashInStep: options.crashInStep,
      used: false,
    };
    this.runs.set(run.runId, run);
    void this.storage.createRun({ runId: run.runId, state: stateOf(run) });
    await this.execute(run);
    return this.testResult(run) as TestRunResult<Output>;
  }

  async testResume(runId: string): Promise<TestRunResult<unknown>> {
    const run = this.runs.get(runId);
    if (!run) throw new Error(`run ${runId} not found`);
    if (run.status === "completed" || run.status === "cancelled") return this.testResult(run);
    if (run.status === "waiting") {
      const operation = Object.values(run.ops).find((item) =>
        item.kind === "wait" && item.signal === run.waitingFor?.signal
      );
      if (
        operation?.payload === undefined
        && (operation?.timeoutAt === undefined || this.clock.now() < operation.timeoutAt)
      ) return this.testResult(run);
    }
    if (run.status === "sleeping" && (run.wakeAt ?? Number.POSITIVE_INFINITY) > this.clock.now()) {
      return this.testResult(run);
    }
    run.status = "pending";
    await this.execute(run);
    return this.testResult(run);
  }

  testResult(run: InternalRun): TestRunResult<unknown> {
    return {
      runId: run.runId,
      status: run.status as TestRunResult["status"],
      result: run.result,
      steps: [...run.steps]
        .sort((left, right) => comparePaths(left.path, right.path))
        .map((stepValue) => publicStep(stepValue) as unknown as TestStepState),
      advisories: run.advisories.map((item) => ({ ...item })),
      ...(run.error !== undefined ? { error: run.error } : {}),
      ...(run.compensationError !== undefined ? { compensationError: run.compensationError } : {}),
      children: run.children.map((runIdValue) => ({ runId: runIdValue } as RunHandle<unknown>)),
    };
  }
}

export const createEngine = (options: EngineOptions = {}): Engine =>
  new EngineImpl(options);

const defaultEngines = new Map<string, EngineImpl>();
const defaultEngine = (directory = join(process.cwd(), ".durably")): EngineImpl => {
  let engine = defaultEngines.get(directory);
  if (!engine) {
    engine = new EngineImpl({ storage: new FileStorage(directory) });
    defaultEngines.set(directory, engine);
  }
  return engine;
};

export const run: RunFunction = ((
  workflowValue: Workflow<unknown, unknown>,
  inputOrOptions?: unknown,
  maybeOptions?: RunOptions,
): Promise<unknown> => {
  const input = inputOrOptions;
  const options = maybeOptions ?? {};
  return defaultEngine(options.dir).runDirectFast(workflowValue, input, options);
}) as RunFunction;

export const durably: Engine = {
  start: () => defaultEngine().start(),
  enqueue: (workflowValue, input, options) => defaultEngine().enqueue(workflowValue, input, options),
  inspect: (runId) => defaultEngine().inspect(runId),
  list: (options) => defaultEngine().list(options),
  retry: (runId, options) => defaultEngine().retry(runId, options),
  restart: (runId) => defaultEngine().restart(runId),
  adopt: (runId) => defaultEngine().adopt(runId),
  signal: (runId, name, payload) => defaultEngine().signal(runId, name, payload),
  cancel: (runId) => defaultEngine().cancel(runId),
  pause: (runId) => defaultEngine().pause(runId),
  resume: (runId) => defaultEngine().resume(runId),
  drain: () => defaultEngine().drain(),
  stop: () => defaultEngine().stop(),
};

export const testEngine = (): TestEngine => {
  const clock = new FakeClock();
  const engine = new EngineImpl({
    storage: new MemoryStorage(),
    clock,
    testMode: true,
    shadowReplay: true,
    concurrency: Number.MAX_SAFE_INTEGER,
  });
  return {
    clock,
    run: (workflowValue, input, options = {}) => engine.testRun(workflowValue, input, options),
    resume: (runId) => engine.testResume(runId),
    signal: (runId, name, payload) => engine.signal(runId, name, payload),
  };
};
